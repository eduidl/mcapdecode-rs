//! Recursive ROS 2 IDL parser.
//!
//! The grammar follows `rosidl_parser/grammar.lark`.  Lexing first is important:
//! it makes newlines ordinary whitespace and prevents `<` / `>` in constant
//! expressions from being mistaken for template delimiters.
//!
//! `grammar.lark` rules intentionally not represented by the decoder AST / CDR are:
//! - (34), (35), (42), (43): `char`, `wchar`, and fixed-point types/literals; CDR's
//!   decoder AST has no corresponding value representation.
//! - (44), (63)--(66), (198), (204): union, typedef, and bitmask declarations;
//!   discriminator, alias, and bit-set layouts have no AST representation.
//! - (218)--(224): annotation declarations; annotation *applications* are parsed,
//!   but user-defined annotation schemas have no decoder use.

use mcapdecode_core::EnumVariant;
use mcapdecode_ros2_common::{
    ConstDef, EnumDef, FieldDef, ParsedSection, PrimitiveType, Ros2Error, StructDef, TypeExpr,
};

use crate::idl_lexer::{Token, lex};

type Tokens<'a> = &'a [Token];

/// Parse one complete IDL section into the public decoder AST.
///
/// Grammar (1) `specification`: one or more top-level definitions.
pub fn parse_idl_section(idl_body: &str) -> Result<ParsedSection, Ros2Error> {
    let tokens = lex(idl_body)?;
    let mut output = ParsedSection::default();
    let rest = parse_definitions(&tokens, &mut Vec::new(), &mut output).map_err(Ros2Error)?;
    if let Some(token) = rest.first() {
        return Err(Ros2Error(error_at(token, "unexpected token")));
    }
    Ok(output)
}

/// Grammar (1) `specification` body: consume definitions recursively until a module
/// closing brace. Newlines are already whitespace at this point.
fn parse_definitions<'a>(
    mut input: Tokens<'a>,
    modules: &mut Vec<String>,
    output: &mut ParsedSection,
) -> Result<Tokens<'a>, String> {
    while !input.is_empty() && !input[0].is("}") {
        input = parse_definition(input, modules, output)?;
    }
    Ok(input)
}

fn parse_definition<'a>(
    input: Tokens<'a>,
    modules: &mut Vec<String>,
    output: &mut ParsedSection,
) -> Result<Tokens<'a>, String> {
    // Grammar (2) `definition`: module, const, type declaration, or include.
    let (input, annotations) = parse_annotations(input)?;
    if input.first().is_some_and(|token| token.is("}")) {
        return Ok(input);
    }
    let Some(token) = input.first() else {
        return Err("unexpected end of IDL input".to_string());
    };
    match token.text.as_str() {
        "#" => parse_include(input),
        "module" => parse_module(input, modules, output),
        "struct" => parse_struct(input, modules, output),
        "enum" => parse_enum(input, modules, output, annotations),
        "const" => {
            let (rest, _) = parse_const(input)?;
            Ok(rest)
        }
        "union" | "typedef" | "bitmask" => Err(error_at(
            token,
            "unsupported IDL declaration (union, typedef, and bitmask are not supported)",
        )),
        _ => Err(error_at(token, "unexpected top-level declaration")),
    }
}

fn parse_module<'a>(
    input: Tokens<'a>,
    modules: &mut Vec<String>,
    output: &mut ParsedSection,
) -> Result<Tokens<'a>, String> {
    // Grammar (3) `module_dcl`: `module IDENTIFIER { definition+ }`.
    let (input, _) = expect(input, "module")?;
    let (input, name) = identifier(input)?;
    let (input, _) = expect(input, "{")?;
    modules.push(name.text.clone());
    let input = parse_definitions(input, modules, output)?;
    let (input, _) = expect(input, "}")?;
    modules.pop();
    let (input, _) = expect(input, ";")?;
    Ok(input)
}

fn parse_struct<'a>(
    input: Tokens<'a>,
    modules: &[String],
    output: &mut ParsedSection,
) -> Result<Tokens<'a>, String> {
    // Grammar (44)--(47): constrained type declarations, struct definitions, and
    // members. Grammar (48) `struct_forward_dcl` is rejected below (no CDR layout).
    let (input, _) = expect(input, "struct")?;
    let (mut input, name) = identifier(input)?;
    if !input.first().is_some_and(|token| token.is("{")) {
        // grammar.lark's `struct_forward_dcl` has no CDR layout to decode.
        return Err(error_at(name, "unsupported struct forward declaration"));
    }
    let struct_line = name.line;
    (input, _) = expect(input, "{")?;
    let mut fields = Vec::new();
    let mut consts = Vec::new();
    while !input.is_empty() && !input[0].is("}") {
        let (rest, _) = parse_annotations(input)?;
        input = rest;
        if input.first().is_some_and(|token| token.is("}")) {
            break;
        }
        if input.first().is_some_and(|token| token.is("const")) {
            let (rest, constant) = parse_const(input)?;
            consts.push(constant);
            input = rest;
        } else {
            let (rest, field) = parse_member(input)?;
            fields.push(field);
            input = rest;
        }
    }
    if input.is_empty() {
        return Err(format!("unclosed struct declaration at line {struct_line}"));
    }
    let (input, _) = expect(input, "}")?;
    let (input, _) = expect(input, ";")?;
    let mut full_name = modules.to_vec();
    full_name.push(name.text.clone());
    output.structs.insert(
        full_name.clone(),
        StructDef {
            full_name,
            fields,
            consts,
        },
    );
    Ok(input)
}

fn parse_enum<'a>(
    input: Tokens<'a>,
    modules: &[String],
    output: &mut ParsedSection,
    _declaration_annotations: Vec<Annotation>,
) -> Result<Tokens<'a>, String> {
    // Grammar (44), (57), (58): enum declaration and comma-separated enumerators.
    // `@bit_bound` is accepted as an annotation but intentionally ignored: the
    // decoder AST and CDR decoder currently model every enum as 32-bit.
    let (input, _) = expect(input, "enum")?;
    let (mut input, name) = identifier(input)?;
    let (rest, _) = expect(input, "{")?;
    input = rest;
    let mut variants = Vec::new();
    let mut next_value = 0_i64;
    let mut needs_variant = true;
    while !input.is_empty() && !input[0].is("}") {
        if input[0].is(",") {
            if needs_variant {
                return Err(error_at(&input[0], "expected enumerator before `,`"));
            }
            needs_variant = true;
            input = &input[1..];
            continue;
        }
        if !needs_variant {
            return Err(error_at(
                &input[0],
                "unexpected trailing characters in enum variant (expected `,`)",
            ));
        }
        let (rest, annotations) = parse_annotations(input)?;
        let (rest, enumerator) = identifier(rest)?;
        let explicit = annotations
            .iter()
            .rev()
            .find(|annotation| annotation.name == "value")
            .map(|annotation| parse_enum_value(&annotation.argument));
        let value = match explicit.transpose()? {
            Some(value) => value,
            None => next_value,
        };
        if !(i64::from(i32::MIN)..=i64::from(u32::MAX)).contains(&value) {
            return Err(error_at(
                enumerator,
                "enum value is outside the 32-bit range of an enum",
            ));
        }
        next_value = value
            .checked_add(1)
            .ok_or_else(|| error_at(enumerator, "enum value overflow"))?;
        variants.push(EnumVariant::new(enumerator.text.clone(), value));
        input = rest;
        needs_variant = false;
    }
    if variants.is_empty() {
        return Err(error_at(
            name,
            "enum declaration must contain an enumerator",
        ));
    }
    if needs_variant {
        return Err(input.first().map_or_else(
            || "unclosed enum declaration".to_string(),
            |token| error_at(token, "trailing `,` in enum declaration"),
        ));
    }
    let (input, _) = expect(input, "}")?;
    let (input, _) = expect(input, ";")?;
    let mut full_name = modules.to_vec();
    full_name.push(name.text.clone());
    output.enums.insert(
        full_name.clone(),
        EnumDef {
            full_name,
            variants,
        },
    );
    Ok(input)
}

fn parse_member<'a>(input: Tokens<'a>) -> Result<(Tokens<'a>, FieldDef), String> {
    // Grammar (47), (67), (68): a member has a type and one declarator. The public
    // AST stores one field per declaration, so multiple declarators are unsupported.
    let start = input
        .first()
        .ok_or_else(|| "unexpected end of input in struct member".to_string())?;
    let (input, ty) = parse_type(input)?;
    let (input, name) = identifier(input)?;
    let (input, fixed_len) = parse_fixed_array(input)?;
    if input.first().is_some_and(|token| token.is("[")) {
        return Err(error_at(
            &input[0],
            "unsupported multi-dimensional fixed array declaration",
        ));
    }
    if input.first().is_some_and(|token| token.is(",")) {
        return Err(error_at(
            &input[0],
            "unsupported multiple field declarators",
        ));
    }
    let (input, _) =
        expect(input, ";").map_err(|_| error_at(start, "field declaration must end with `;`"))?;
    Ok((
        input,
        FieldDef {
            name: name.text.clone(),
            ty,
            fixed_len,
        },
    ))
}

fn parse_const<'a>(input: Tokens<'a>) -> Result<(Tokens<'a>, ConstDef), String> {
    // Grammar (5), (6): constant declaration and constant type. Grammar (7)--(16)
    // define expression precedence; values are retained verbatim because the AST
    // exposes `ConstDef::value` and CDR decoding never evaluates constants.
    let (input, _) = expect(input, "const")?;
    let (input, ty) = parse_type(input)?;
    let (input, name) = identifier(input)?;
    let (input, _) = expect(input, "=")?;
    let start = input
        .first()
        .ok_or_else(|| "unexpected end of input in const expression".to_string())?;
    let mut index = 0;
    let mut parentheses = 0_usize;
    let mut first_open_parenthesis = None;
    while let Some(token) = input.get(index) {
        if token.is(";") && parentheses == 0 {
            break;
        }
        match token.text.as_str() {
            "(" => {
                parentheses += 1;
                first_open_parenthesis.get_or_insert(token);
            }
            ")" => {
                parentheses = parentheses
                    .checked_sub(1)
                    .ok_or_else(|| error_at(token, "unmatched `)`"))?
            }
            _ => {}
        }
        index += 1;
    }
    if input.get(index).is_none() {
        if let Some(open) = first_open_parenthesis {
            return Err(error_at(open, "unclosed `(` in const expression"));
        }
        return Err(error_at(start, "const declaration must end with `;`"));
    }
    if index == 0 {
        return Err(error_at(start, "expected const expression"));
    }
    // Comments were discarded by the lexer, so reconstruct from token text instead
    // of slicing the source span, whose gaps may still contain comments.
    let value = format_const_value(&input[..index]);
    Ok((
        &input[index + 1..],
        ConstDef {
            ty,
            name: name.text.clone(),
            value,
        },
    ))
}

/// Reconstruct a comment-free expression, retaining whitespace only where its
/// absence in the original source would merge two token spellings.
fn format_const_value(tokens: &[Token]) -> String {
    let mut value = String::new();
    let mut previous: Option<&Token> = None;
    for token in tokens {
        if !value.is_empty() && previous.is_some_and(|previous| previous.end != token.start) {
            value.push(' ');
        }
        value.push_str(&token.text);
        previous = Some(token);
    }
    value
}

fn parse_type<'a>(input: Tokens<'a>) -> Result<(Tokens<'a>, TypeExpr), String> {
    // Grammar (21)--(23): type specifiers, simple types, and base types.
    let Some(first) = input.first() else {
        return Err("unexpected end of input while parsing type".to_string());
    };
    if first.is("long") && input.get(1).is_some_and(|token| token.is("double")) {
        return Err(error_at(first, "unsupported IDL type `long double`"));
    }
    if first.is("sequence") {
        // Grammar (38), (39): `sequence<type_spec[, positive_int_const]>`.
        let (input, _) = expect(input, "sequence")?;
        let (input, _) = expect(input, "<")?;
        let (input, elem) = parse_type(input)?;
        let (input, max_len) = if input.first().is_some_and(|token| token.is(",")) {
            let (input, _) = expect(input, ",")?;
            parse_bound(input)?
        } else {
            (input, None)
        };
        let (input, _) = expect(input, ">")?;
        return Ok((
            input,
            TypeExpr::Sequence {
                elem: Box::new(elem),
                max_len,
            },
        ));
    }
    if first.is("string") || first.is("wstring") {
        // Grammar (38), (40), (41): bounded and unbounded string templates.
        let wide = first.is("wstring");
        let mut input = &input[1..];
        if input.first().is_some_and(|token| token.is("<")) {
            input = &input[1..];
            let (rest, bound) = parse_positive_integer(input)?;
            let (rest, _) = expect(rest, ">")?;
            return Ok((
                rest,
                if wide {
                    TypeExpr::BoundedWString(bound)
                } else {
                    TypeExpr::BoundedString(bound)
                },
            ));
        }
        return Ok((
            input,
            TypeExpr::Primitive(if wide {
                PrimitiveType::WString
            } else {
                PrimitiveType::String
            }),
        ));
    }
    if let Some((consumed, primitive)) = primitive(input) {
        return Ok((&input[consumed..], TypeExpr::Primitive(primitive)));
    }
    if first.is("unsigned") {
        return Err(error_at(
            first,
            "unsupported IDL type starting with `unsigned`",
        ));
    }
    let (input, scoped) = parse_scoped_name(input)?;
    Ok((input, TypeExpr::Scoped(scoped)))
}

fn primitive(input: Tokens<'_>) -> Option<(usize, PrimitiveType)> {
    // Grammar (24)--(37), plus ROS 2's int8/16/32/64 aliases (206)--(215).
    let word = input.first()?.text.as_str();
    let second = input.get(1).map(|token| token.text.as_str());
    let third = input.get(2).map(|token| token.text.as_str());
    match (word, second, third) {
        ("unsigned", Some("long"), Some("long")) => Some((3, PrimitiveType::U64)),
        ("long", Some("long"), _) => Some((2, PrimitiveType::I64)),
        ("unsigned", Some("short"), _) => Some((2, PrimitiveType::U16)),
        ("unsigned", Some("long"), _) => Some((2, PrimitiveType::U32)),
        ("boolean" | "bool", _, _) => Some((1, PrimitiveType::Bool)),
        ("int8", _, _) => Some((1, PrimitiveType::I8)),
        ("int16" | "short", _, _) => Some((1, PrimitiveType::I16)),
        ("int32" | "long", _, _) => Some((1, PrimitiveType::I32)),
        ("int64", _, _) => Some((1, PrimitiveType::I64)),
        ("uint8", _, _) => Some((1, PrimitiveType::U8)),
        ("uint16", _, _) => Some((1, PrimitiveType::U16)),
        ("uint32", _, _) => Some((1, PrimitiveType::U32)),
        ("uint64", _, _) => Some((1, PrimitiveType::U64)),
        ("float32" | "float", _, _) => Some((1, PrimitiveType::F32)),
        ("float64" | "double", _, _) => Some((1, PrimitiveType::F64)),
        ("octet", _, _) => Some((1, PrimitiveType::Octet)),
        _ => None,
    }
}

fn parse_fixed_array<'a>(input: Tokens<'a>) -> Result<(Tokens<'a>, Option<usize>), String> {
    // Grammar (59), (60): array declarator and fixed-array size.
    if !input.first().is_some_and(|token| token.is("[")) {
        return Ok((input, None));
    }
    let (input, _) = expect(input, "[")?;
    let (input, length) = parse_positive_integer(input)?;
    let (input, _) = expect(input, "]")?;
    Ok((input, Some(length)))
}

fn parse_bound<'a>(input: Tokens<'a>) -> Result<(Tokens<'a>, Option<usize>), String> {
    // Grammar (19) `positive_int_const`; named constants cannot be evaluated by the
    // decoder AST, so their bound remains unknown.
    if input.first().is_some_and(|token| {
        token
            .text
            .as_bytes()
            .first()
            .is_some_and(|byte| byte.is_ascii_digit())
    }) {
        return parse_positive_integer(input).map(|(rest, value)| (rest, Some(value)));
    }
    // Positive named constants are accepted, but the AST cannot enforce their bound.
    parse_scoped_name(input).map(|(rest, _)| (rest, None))
}

fn parse_positive_integer<'a>(input: Tokens<'a>) -> Result<(Tokens<'a>, usize), String> {
    let token = input
        .first()
        .ok_or_else(|| "expected positive integer".to_string())?;
    let value = match token
        .text
        .strip_prefix("0x")
        .or(token.text.strip_prefix("0X"))
    {
        Some(hex) => usize::from_str_radix(hex, 16),
        None => token.text.parse(),
    }
    .map_err(|_| error_at(token, "expected positive integer"))?;
    if value == 0 {
        return Err(error_at(token, "expected positive integer"));
    }
    Ok((&input[1..], value))
}

#[derive(Debug)]
struct Annotation {
    name: String,
    argument: String,
}

fn parse_annotations<'a>(mut input: Tokens<'a>) -> Result<(Tokens<'a>, Vec<Annotation>), String> {
    // Grammar (225): zero or more annotation applications before a declaration.
    let mut annotations = Vec::new();
    while input.first().is_some_and(|token| token.is("@")) {
        let (rest, annotation) = parse_annotation(input)?;
        annotations.push(annotation);
        input = rest;
    }
    Ok((input, annotations))
}

fn parse_annotation<'a>(input: Tokens<'a>) -> Result<(Tokens<'a>, Annotation), String> {
    // Grammar (225)--(227): `@scoped_name` and optional positional/named parameters.
    let (input, _) = expect(input, "@")?;
    let (mut input, name) = parse_scoped_name(input)?;
    let mut argument = String::new();
    if input.first().is_some_and(|token| token.is("(")) {
        let start = &input[0];
        input = &input[1..];
        let mut depth = 1_usize;
        let mut index = 0;
        while let Some(token) = input.get(index) {
            match token.text.as_str() {
                "(" => depth += 1,
                ")" => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            index += 1;
        }
        let close = input
            .get(index)
            .ok_or_else(|| error_at(start, "unclosed annotation"))?;
        argument = format_const_value(&input[..index]);
        input = &input[index + 1..];
        let _ = close;
    }
    Ok((
        input,
        Annotation {
            name: name.join("::"),
            argument,
        },
    ))
}

fn parse_scoped_name<'a>(input: Tokens<'a>) -> Result<(Tokens<'a>, Vec<String>), String> {
    // Grammar (4): optional leading `::`, then identifiers joined by `::`.
    let input = if input.first().is_some_and(|token| token.is("::")) {
        &input[1..]
    } else {
        input
    };
    let (mut input, first) = identifier(input)?;
    let mut names = vec![first.text.clone()];
    while input.first().is_some_and(|token| token.is("::")) {
        input = &input[1..];
        let (rest, name) = identifier(input)?;
        names.push(name.text.clone());
        input = rest;
    }
    Ok((input, names))
}

fn parse_include(input: Tokens<'_>) -> Result<Tokens<'_>, String> {
    // 7.3 preprocessing: grammar `include_directive` accepts quoted and angle paths.
    let (input, _) = expect(input, "#")?;
    let (input, include) = identifier(input)?;
    if include.text != "include" {
        return Err(error_at(include, "expected `include` after `#`"));
    }
    let Some(first) = input.first() else {
        return Err("incomplete #include directive".to_string());
    };
    if first.is("<") {
        let mut index = 1;
        while let Some(token) = input.get(index) {
            if token.is(">") {
                return Ok(&input[index + 1..]);
            }
            index += 1;
        }
        Err(error_at(first, "unclosed #include path"))
    } else if first.text.starts_with('"') {
        Ok(&input[1..])
    } else {
        Err(error_at(first, "expected #include path"))
    }
}

fn expect<'a>(input: Tokens<'a>, wanted: &'static str) -> Result<(Tokens<'a>, &'a Token), String> {
    match input.split_first() {
        Some((token, rest)) if token.is(wanted) => Ok((rest, token)),
        Some((token, _)) => Err(error_at(token, &format!("expected `{wanted}`"))),
        None => Err(format!("expected `{wanted}` at end of input")),
    }
}

fn identifier(input: Tokens<'_>) -> Result<(Tokens<'_>, &Token), String> {
    let Some((token, rest)) = input.split_first() else {
        return Err("expected identifier at end of input".to_string());
    };
    if token
        .text
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_alphabetic() || ch == '_')
        && token
            .text
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        Ok((rest, token))
    } else {
        Err(error_at(token, "expected identifier"))
    }
}

fn error_at(token: &Token, message: &str) -> String {
    format!("parse error at line {}: {message}", token.line)
}

fn parse_enum_value(value: &str) -> Result<i64, String> {
    let value = value.trim();
    let value = value
        .strip_prefix("value")
        .and_then(|rest| rest.trim_start().strip_prefix('=').map(str::trim))
        .unwrap_or(value);
    let (negative, digits) = match value.strip_prefix('-') {
        Some(rest) => (true, rest.trim()),
        None => (false, value),
    };
    let magnitude = match digits
        .strip_prefix("0x")
        .or_else(|| digits.strip_prefix("0X"))
    {
        Some(hex) => i64::from_str_radix(hex, 16),
        None => digits.parse(),
    }
    .map_err(|error| format!("invalid enum value `{value}`: {error}"))?;
    let number = if negative { -magnitude } else { magnitude };
    if !(i64::from(i32::MIN)..=i64::from(u32::MAX)).contains(&number) {
        return Err(format!(
            "invalid enum value `{value}`: outside the 32-bit range of an enum"
        ));
    }
    Ok(number)
}

//! ROS2 IDL parser implementation using nom parser combinators.
//!
//! This module provides a robust parser for ROS2 Interface Definition Language (IDL)
//! schemas. It uses the `nom` library for parser combinators, which provides better
//! error handling, composability, and maintainability compared to hand-written parsers.
//!
//! # Supported Features
//!
//! - Struct declarations with fields
//! - Enum declarations with variants (`@value` sets an enumerator's value)
//! - Primitive types (bool, int8-64, uint8-64, float32/64, string, etc.)
//! - Sequence types (bounded and unbounded)
//! - Bounded strings and wide strings
//! - Fixed-size arrays
//! - Const declarations
//! - Module scoping
//! - Scoped type names (using :: or / separators)
//! - Annotations (ignored, except `@value` on enumerators)
//! - Include directives (ignored)
//!
//! # Unsupported Features
//!
//! The following IDL features are explicitly unsupported and will return errors:
//! - Union types
//! - Typedef declarations
//! - Bitmask types
//!
//! # Limitations
//!
//! Parsing is line based, whereas IDL itself is whitespace insensitive. Declarations
//! written on a single line (`enum E { A, B };`) are rejected, and several enumerators
//! on one line (`A, B,`) keep only the first one. Lifting this requires tokenizing the
//! input instead of splitting it into lines.

use std::collections::HashMap;

use mcapdecode_core::EnumVariant;
use mcapdecode_ros2_common::{
    ConstDef, EnumDef, FieldDef, ParsedSection, PrimitiveType, Ros2Error, StructDef, TypeExpr,
};
use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, take_while, take_while1},
    character::complete::{alpha1, alphanumeric1, char, space0},
    combinator::{all_consuming, map, opt, recognize, value},
    error::{Error, ErrorKind},
    multi::{many0, many1, separated_list0},
    sequence::{pair, preceded, terminated, tuple},
};

use crate::lex::strip_comments;

enum PendingDecl {
    Module(String),
    Struct(String),
    Enum(String),
}

/// Enum declaration being collected, with the state needed to number its enumerators.
struct EnumBuilder {
    name: String,
    variants: Vec<EnumVariant>,
    /// Value used for the next enumerator without an explicit `@value`.
    next_value: i64,
    /// `@value` seen since the last enumerator, applied to the enumerator that follows.
    pending_value: Option<i64>,
    /// Annotation text whose parentheses are still open, continued on the next line.
    pending_annotation: String,
}

impl EnumBuilder {
    fn new(name: String) -> Self {
        Self {
            name,
            variants: Vec::new(),
            next_value: 0,
            pending_value: None,
            pending_annotation: String::new(),
        }
    }

    /// Append an enumerator, using its `@value` if one was given.
    ///
    /// Enumerators without an explicit value continue from the previous one, as
    /// specified by DDS X-Types 7.3.1.2.1.5 (Enumerated Literal Values).
    fn push_variant(&mut self, name: String) -> Result<(), Ros2Error> {
        let value = self.pending_value.take().unwrap_or(self.next_value);
        self.next_value = value
            .checked_add(1)
            .ok_or_else(|| Ros2Error("enum value overflow".to_string()))?;
        self.variants.push(EnumVariant::new(name, value));
        Ok(())
    }
}

#[derive(Clone)]
enum LineStatement {
    Include,
    Unsupported,
    ModuleOpens(Vec<String>),
    ModuleHead(String),
    StructOpen(String),
    StructHead(String),
    EnumOpen(String),
    EnumHead(String),
    Close(usize),
}

pub fn parse_idl_section(idl_body: &str) -> Result<ParsedSection, Ros2Error> {
    let mut structs: HashMap<Vec<String>, StructDef> = HashMap::new();
    let mut enums: HashMap<Vec<String>, EnumDef> = HashMap::new();
    let mut modules: Vec<String> = Vec::new();
    let mut current_struct: Option<(String, Vec<FieldDef>, Vec<ConstDef>)> = None;
    let mut current_enum: Option<EnumBuilder> = None;
    let mut pending_decl: Option<PendingDecl> = None;

    let mut annotation_depth = 0i32;
    let mut ann_in_str = false;
    let mut ann_escaped = false;
    let mut in_block_comment = false;

    for (idx, raw) in idl_body.lines().enumerate() {
        let line_no = idx + 1;
        let line = strip_comments(raw, &mut in_block_comment);
        let mut line = line.trim();
        if line.is_empty() {
            continue;
        }

        // Inside an enum body annotations bind to the enumerator that follows them, so
        // `@value` has to be captured (and the rest of the line kept) rather than skipped.
        let mut annotation_buf = String::new();
        if annotation_depth == 0
            && let Some(builder) = current_enum.as_mut()
            && (line.starts_with('@') || !builder.pending_annotation.is_empty())
        {
            annotation_buf = std::mem::take(&mut builder.pending_annotation);
            if !annotation_buf.is_empty() {
                annotation_buf.push(' ');
            }
            annotation_buf.push_str(line);

            match split_leading_annotations(&annotation_buf) {
                Some((value, rest)) => {
                    if let Some(value) = value {
                        builder.pending_value = Some(parse_enum_value(value).map_err(|e| {
                            Ros2Error(format!("parse error at line {line_no}: {e}"))
                        })?);
                    }
                    line = rest.trim();
                    if line.is_empty() {
                        continue;
                    }
                }
                None => {
                    // Argument list still open: continue on the next line.
                    builder.pending_annotation = annotation_buf.clone();
                    continue;
                }
            }
        }

        if annotation_depth > 0 || line.starts_with('@') {
            let (open, close) =
                paren_counts_outside_strings(line, &mut ann_in_str, &mut ann_escaped);
            annotation_depth += open as i32;
            annotation_depth -= close as i32;
            continue;
        }

        if let Some(pending) = pending_decl.take() {
            if line != "{" {
                return Err(
                    format!("expected '{{' after declaration at line {line_no}: {line}").into(),
                );
            }
            match pending {
                PendingDecl::Module(name) => modules.push(name),
                PendingDecl::Struct(name) => {
                    if current_struct.is_some() || current_enum.is_some() {
                        return Err(format!(
                            "nested declaration unsupported at line {line_no}: {line}"
                        )
                        .into());
                    }
                    current_struct = Some((name, Vec::new(), Vec::new()));
                }
                PendingDecl::Enum(name) => {
                    if current_struct.is_some() || current_enum.is_some() {
                        return Err(format!(
                            "nested declaration unsupported at line {line_no}: {line}"
                        )
                        .into());
                    }
                    current_enum = Some(EnumBuilder::new(name));
                }
            }
            continue;
        }

        if let Some(statement) = parse_line_statement(line) {
            match statement {
                LineStatement::Include => continue,
                LineStatement::Unsupported => {
                    return Err(
                        format!("unsupported IDL declaration at line {line_no}: {line}").into(),
                    );
                }
                LineStatement::ModuleOpens(names) => {
                    modules.extend(names);
                    continue;
                }
                LineStatement::ModuleHead(name) => {
                    pending_decl = Some(PendingDecl::Module(name));
                    continue;
                }
                LineStatement::StructOpen(name) => {
                    ensure_no_nested_declaration(
                        current_struct.is_some(),
                        current_enum.is_some(),
                        line_no,
                        line,
                    )?;
                    current_struct = Some((name, Vec::new(), Vec::new()));
                    continue;
                }
                LineStatement::StructHead(name) => {
                    ensure_no_nested_declaration(
                        current_struct.is_some(),
                        current_enum.is_some(),
                        line_no,
                        line,
                    )?;
                    pending_decl = Some(PendingDecl::Struct(name));
                    continue;
                }
                LineStatement::EnumOpen(name) => {
                    ensure_no_nested_declaration(
                        current_struct.is_some(),
                        current_enum.is_some(),
                        line_no,
                        line,
                    )?;
                    current_enum = Some(EnumBuilder::new(name));
                    continue;
                }
                LineStatement::EnumHead(name) => {
                    ensure_no_nested_declaration(
                        current_struct.is_some(),
                        current_enum.is_some(),
                        line_no,
                        line,
                    )?;
                    pending_decl = Some(PendingDecl::Enum(name));
                    continue;
                }
                LineStatement::Close(close_count) => {
                    for _ in 0..close_count {
                        if let Some((name, fields, consts)) = current_struct.take() {
                            let mut full = modules.clone();
                            full.push(name);
                            structs.insert(
                                full.clone(),
                                StructDef {
                                    full_name: full,
                                    fields,
                                    consts,
                                },
                            );
                        } else if let Some(builder) = current_enum.take() {
                            let mut full = modules.clone();
                            full.push(builder.name);
                            enums.insert(
                                full.clone(),
                                EnumDef {
                                    full_name: full,
                                    variants: builder.variants,
                                },
                            );
                        } else if modules.pop().is_none() {
                            return Err(format!("unmatched closing brace at line {line_no}").into());
                        }
                    }
                    continue;
                }
            }
        }

        if let Some((_, fields, consts)) = current_struct.as_mut() {
            if line.starts_with("const ") {
                consts.push(
                    parse_const(line)
                        .map_err(|e| Ros2Error(format!("parse error at line {line_no}: {e}")))?,
                );
            } else {
                fields.push(
                    parse_field(line)
                        .map_err(|e| Ros2Error(format!("parse error at line {line_no}: {e}")))?,
                );
            }
            continue;
        }

        if let Some(builder) = current_enum.as_mut() {
            let variant = parse_enum_variant(line)
                .map_err(|e| Ros2Error(format!("parse error at line {line_no}: {e}")))?;
            if let Some(name) = variant {
                builder
                    .push_variant(name)
                    .map_err(|e| Ros2Error(format!("parse error at line {line_no}: {e}")))?;
            }
            continue;
        }

        if line.starts_with("const ") {
            parse_const(line)
                .map_err(|e| Ros2Error(format!("parse error at line {line_no}: {e}")))?;
            continue;
        }

        return Err(format!("unexpected top-level statement at line {line_no}: {line}").into());
    }

    if current_struct.is_some() {
        return Err("unclosed struct declaration".into());
    }
    if current_enum.is_some() {
        return Err("unclosed enum declaration".into());
    }
    if pending_decl.is_some() {
        return Err("declaration missing opening brace".into());
    }
    Ok(ParsedSection { structs, enums })
}

fn ensure_no_nested_declaration(
    has_current_struct: bool,
    has_current_enum: bool,
    line_no: usize,
    line: &str,
) -> Result<(), Ros2Error> {
    if has_current_struct || has_current_enum {
        return Err(format!("nested declaration unsupported at line {line_no}: {line}").into());
    }
    Ok(())
}

fn parse_line_statement(line: &str) -> Option<LineStatement> {
    parse_complete(line_statement, line)
}

fn parse_complete<'a, O, P>(parser: P, input: &'a str) -> Option<O>
where
    P: Parser<&'a str, O, Error<&'a str>>,
{
    all_consuming(parser)
        .parse(input)
        .ok()
        .map(|(_, output)| output)
}

fn line_statement(input: &str) -> IResult<&str, LineStatement> {
    alt((
        value(LineStatement::Include, include_directive),
        value(LineStatement::Unsupported, unsupported_decl),
        map(chained_module_decls, LineStatement::ModuleOpens),
        map(module_decl_head, |name| {
            LineStatement::ModuleHead(name.to_string())
        }),
        map(struct_decl, |name| {
            LineStatement::StructOpen(name.to_string())
        }),
        map(struct_decl_head, |name| {
            LineStatement::StructHead(name.to_string())
        }),
        map(enum_decl, |name| LineStatement::EnumOpen(name.to_string())),
        map(enum_decl_head, |name| {
            LineStatement::EnumHead(name.to_string())
        }),
        map(close_tokens, LineStatement::Close),
    ))(input)
}

fn include_directive(input: &str) -> IResult<&str, ()> {
    value((), pair(tag("#include"), take_while(|_: char| true)))(input)
}

fn unsupported_decl(input: &str) -> IResult<&str, ()> {
    value(
        (),
        pair(
            terminated(alt((tag("union"), tag("bitmask"), tag("typedef"))), ws1),
            take_while(|_: char| true),
        ),
    )(input)
}

fn chained_module_decls(input: &str) -> IResult<&str, Vec<String>> {
    map(many1(terminated(module_decl, ws)), |names| {
        names.into_iter().map(ToString::to_string).collect()
    })(input)
}

fn close_tokens(input: &str) -> IResult<&str, usize> {
    map(
        many1(terminated(alt((tag("};"), tag("}"))), ws)),
        |tokens| tokens.len(),
    )(input)
}

/// Parse module declaration: module Name {
fn module_decl(input: &str) -> IResult<&str, &str> {
    map(
        tuple((tag("module"), ws1, identifier, ws, char('{'))),
        |(_, _, name, _, _)| name,
    )(input)
}

/// Parse module declaration head: module Name
fn module_decl_head(input: &str) -> IResult<&str, &str> {
    map(
        tuple((tag("module"), ws1, identifier, ws)),
        |(_, _, name, _)| name,
    )(input)
}

/// Parse struct declaration: struct Name {
fn struct_decl(input: &str) -> IResult<&str, &str> {
    map(
        tuple((tag("struct"), ws1, identifier, ws, char('{'))),
        |(_, _, name, _, _)| name,
    )(input)
}

/// Parse struct declaration head: struct Name
fn struct_decl_head(input: &str) -> IResult<&str, &str> {
    map(
        tuple((tag("struct"), ws1, identifier, ws)),
        |(_, _, name, _)| name,
    )(input)
}

/// Parse enum declaration: enum Name {
fn enum_decl(input: &str) -> IResult<&str, &str> {
    map(
        tuple((tag("enum"), ws1, identifier, ws, char('{'))),
        |(_, _, name, _, _)| name,
    )(input)
}

/// Parse enum declaration head: enum Name
fn enum_decl_head(input: &str) -> IResult<&str, &str> {
    map(
        tuple((tag("enum"), ws1, identifier, ws)),
        |(_, _, name, _)| name,
    )(input)
}

fn parse_const(line: &str) -> Result<ConstDef, Ros2Error> {
    let body = line
        .strip_prefix("const ")
        .ok_or_else(|| Ros2Error("const declaration must start with `const`".to_string()))?;
    let body = body
        .strip_suffix(';')
        .ok_or_else(|| Ros2Error("const declaration must end with ';'".to_string()))?;
    if has_long_double_tokens(body) {
        return Err("unsupported IDL type `long double`".into());
    }

    match const_decl(body.trim()) {
        Ok((remaining, def)) if remaining.trim().is_empty() => Ok(def),
        Ok((remaining, _)) => {
            Err(format!("Unexpected trailing characters in const: {remaining}").into())
        }
        Err(e) => Err(format!("Failed to parse const declaration: {e}").into()),
    }
}

fn parse_field(line: &str) -> Result<FieldDef, Ros2Error> {
    let body = line
        .strip_suffix(';')
        .ok_or_else(|| "field declaration must end with ';'".to_string())?
        .trim();
    if has_long_double_tokens(body) {
        return Err("unsupported IDL type `long double`".into());
    }

    match field_decl(body) {
        Ok((remaining, def)) if remaining.trim().is_empty() => Ok(def),
        Ok((remaining, _)) => {
            Err(format!("Unexpected trailing characters in field: {remaining}").into())
        }
        Err(e) => Err(format!("Failed to parse field declaration: {e}").into()),
    }
}

/// Parse an identifier (alphanumeric + underscore, must start with alpha or _)
fn identifier(input: &str) -> IResult<&str, &str> {
    recognize(pair(
        alt((alpha1, tag("_"))),
        many0(alt((alphanumeric1, tag("_")))),
    ))(input)
}

/// Parse whitespace and comments
fn ws(input: &str) -> IResult<&str, ()> {
    value((), space0)(input)
}

/// Parse one-or-more whitespace characters.
fn ws1(input: &str) -> IResult<&str, ()> {
    value((), take_while1(|c: char| c.is_whitespace()))(input)
}

/// Parse a scoped identifier (e.g., "foo::bar::Baz" or "foo/bar/Baz")
fn scoped_name(input: &str) -> IResult<&str, Vec<String>> {
    let sep = if input.contains("::") { "::" } else { "/" };
    map(
        separated_list0(tag(sep), map(identifier, String::from)),
        |parts| parts.into_iter().filter(|s| !s.is_empty()).collect(),
    )(input)
}

/// Parse primitive type names (order matters: longer matches first)
fn primitive_type(input: &str) -> IResult<&str, PrimitiveType> {
    terminated(
        alt((
            value(
                PrimitiveType::U64,
                tuple((tag("unsigned"), ws1, tag("long"), ws1, tag("long"))),
            ),
            value(PrimitiveType::I64, tuple((tag("long"), ws1, tag("long")))),
            value(
                PrimitiveType::U16,
                tuple((tag("unsigned"), ws1, tag("short"))),
            ),
            value(
                PrimitiveType::U32,
                tuple((tag("unsigned"), ws1, tag("long"))),
            ),
            value(PrimitiveType::Bool, alt((tag("boolean"), tag("bool")))),
            value(PrimitiveType::I8, tag("int8")),
            value(PrimitiveType::I16, alt((tag("int16"), tag("short")))),
            value(PrimitiveType::I32, alt((tag("int32"), tag("long")))),
            value(PrimitiveType::I64, tag("int64")),
            value(PrimitiveType::U8, tag("uint8")),
            value(PrimitiveType::U16, tag("uint16")),
            value(PrimitiveType::U32, tag("uint32")),
            value(PrimitiveType::U64, tag("uint64")),
            value(PrimitiveType::F32, alt((tag("float32"), tag("float")))),
            value(PrimitiveType::F64, alt((tag("float64"), tag("double")))),
            value(PrimitiveType::String, tag("string")),
            value(PrimitiveType::WString, tag("wstring")),
            value(PrimitiveType::Octet, tag("octet")),
        )),
        keyword_boundary,
    )(input)
}

fn keyword_boundary(input: &str) -> IResult<&str, ()> {
    if input.chars().next().is_some_and(is_ident_continue) {
        return Err(nom::Err::Error(Error::new(input, ErrorKind::Verify)));
    }
    Ok((input, ()))
}

fn is_ident_continue(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn has_long_double_tokens(s: &str) -> bool {
    let mut normalized = String::with_capacity(s.len());
    for ch in s.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            normalized.push(ch);
        } else {
            normalized.push(' ');
        }
    }
    let tokens: Vec<&str> = normalized.split_whitespace().collect();
    tokens
        .windows(2)
        .any(|pair| pair[0] == "long" && pair[1] == "double")
}

/// Parse a number
fn number(input: &str) -> IResult<&str, usize> {
    map(take_while1(|c: char| c.is_ascii_digit()), |s: &str| {
        s.parse().unwrap()
    })(input)
}

fn sequence_bound(input: &str) -> IResult<&str, Option<usize>> {
    alt((
        map(number, Some),
        // Some schemas use named constants like `kObjectsCapacity`.
        // Reader-side schema derivation accepts those declarations but
        // leaves enforcement to the writer / producer side for now.
        value(None, scoped_name),
    ))(input)
}

/// Parse sequence<T> or sequence<T, N>
fn sequence_type(input: &str) -> IResult<&str, TypeExpr> {
    map(
        tuple((
            tag("sequence"),
            ws,
            char('<'),
            ws,
            type_expr_inner,
            opt(preceded(tuple((ws, char(','), ws)), sequence_bound)),
            ws,
            char('>'),
        )),
        |(_, _, _, _, elem, max_len, _, _)| TypeExpr::Sequence {
            elem: Box::new(elem),
            max_len: max_len.flatten(),
        },
    )(input)
}

/// Parse string<N>
fn bounded_string_type(input: &str) -> IResult<&str, TypeExpr> {
    map(
        tuple((tag("string"), ws, char('<'), ws, number, ws, char('>'))),
        |(_, _, _, _, n, _, _)| TypeExpr::BoundedString(n),
    )(input)
}

/// Parse wstring<N>
fn bounded_wstring_type(input: &str) -> IResult<&str, TypeExpr> {
    map(
        tuple((tag("wstring"), ws, char('<'), ws, number, ws, char('>'))),
        |(_, _, _, _, n, _, _)| TypeExpr::BoundedWString(n),
    )(input)
}

/// Parse any type expression (internal, does not consume leading whitespace)
fn type_expr_inner(input: &str) -> IResult<&str, TypeExpr> {
    alt((
        sequence_type,
        bounded_string_type,
        bounded_wstring_type,
        map(primitive_type, TypeExpr::Primitive),
        map(scoped_name, TypeExpr::Scoped),
    ))(input)
}

/// Parse field array notation: name[N]
fn field_array_notation(input: &str) -> IResult<&str, (&str, Option<usize>)> {
    alt((
        map(
            pair(identifier, tuple((char('['), ws, number, ws, char(']')))),
            |(name, (_, _, size, _, _))| (name, Some(size)),
        ),
        map(identifier, |name| (name, None)),
    ))(input)
}

/// Parse a field declaration (without semicolon): type_expr name or type_expr name[N]
fn field_decl(input: &str) -> IResult<&str, FieldDef> {
    map(
        tuple((type_expr_inner, ws1, field_array_notation)),
        |(ty, _, (name, fixed_len))| FieldDef {
            name: name.to_string(),
            ty,
            fixed_len,
        },
    )(input)
}

/// Parse a const value (everything after '=')
fn const_value(input: &str) -> IResult<&str, &str> {
    map(take_while(|c: char| c != ';'), str::trim)(input)
}

/// Parse a const declaration (without "const " prefix and semicolon): type name = value
fn const_decl(input: &str) -> IResult<&str, ConstDef> {
    map(
        tuple((
            type_expr_inner,
            ws1,
            identifier,
            ws,
            char('='),
            ws,
            const_value,
        )),
        |(ty, _, name, _, _, _, value)| ConstDef {
            ty,
            name: name.to_string(),
            value: value.to_string(),
        },
    )(input)
}

/// Parse enum variant: `VARIANT`.
///
/// ROS 2 IDL enumerators are bare identifiers; explicit values are written with the
/// `@value` annotation. Anything trailing the identifier (such as the non-IDL
/// `VARIANT = 1` form) is left unconsumed and ignored.
fn enum_variant(input: &str) -> IResult<&str, Option<&str>> {
    let trimmed = input.trim().trim_end_matches(',');
    if trimmed.is_empty() {
        return Ok((input, None));
    }

    map(identifier, Some)(trimmed)
}

fn parse_enum_variant(line: &str) -> std::result::Result<Option<String>, Ros2Error> {
    match enum_variant(line) {
        Ok((_, variant)) => Ok(variant.map(ToString::to_string)),
        Err(e) => Err(format!("Failed to parse enum variant '{line}': {e}").into()),
    }
}

/// Split the annotations at the start of `line`, returning the `@value` argument (if
/// any) together with the rest of the line.
///
/// Returns `None` when an annotation's parentheses are not closed on this line, which
/// leaves the multi-line annotation skipping in charge.
fn split_leading_annotations(mut line: &str) -> Option<(Option<&str>, &str)> {
    let mut value = None;

    while let Some(rest) = line.strip_prefix('@') {
        let name_len = rest
            .find(|c: char| !(c.is_alphanumeric() || c == '_' || c == ':'))
            .unwrap_or(rest.len());
        let (name, rest) = rest.split_at(name_len);
        let rest = rest.trim_start();

        let Some(args_body) = rest.strip_prefix('(') else {
            line = rest;
            continue;
        };
        let end = find_annotation_args_end(args_body)?;
        if name == "value" {
            value = Some(annotation_value_arg(&args_body[..end]));
        }
        line = args_body[end + 1..].trim_start();
    }

    Some((value, line))
}

/// Byte offset of the `)` closing an annotation argument list, ignoring parentheses
/// inside string literals. `None` when the list is not closed within `body`.
fn find_annotation_args_end(body: &str) -> Option<usize> {
    let mut in_str = false;
    let mut escaped = false;
    let mut depth = 0usize;
    for (idx, ch) in body.char_indices() {
        if in_str {
            if escaped {
                escaped = false;
                continue;
            }
            match ch {
                '\\' => escaped = true,
                '"' => in_str = false,
                _ => {}
            }
            continue;
        }
        match ch {
            '"' => in_str = true,
            '(' => depth += 1,
            ')' if depth == 0 => return Some(idx),
            ')' => depth -= 1,
            _ => {}
        }
    }
    None
}

/// Extract the argument of `@value`, accepting both `@value(1)` and `@value(value = 1)`.
fn annotation_value_arg(args: &str) -> &str {
    let args = args.trim();
    args.strip_prefix("value")
        .map(str::trim_start)
        .and_then(|rest| rest.strip_prefix('='))
        .map(str::trim)
        .unwrap_or(args)
}

/// Parse an enumerator value, which has to fit in the 32 bits an enum is serialized
/// with, either as a signed or as an unsigned value.
fn parse_enum_value(value: &str) -> std::result::Result<i64, Ros2Error> {
    let value = value.trim();
    let (negative, digits) = match value.strip_prefix('-') {
        Some(rest) => (true, rest.trim_start()),
        None => (false, value),
    };

    let magnitude = match digits
        .strip_prefix("0x")
        .or_else(|| digits.strip_prefix("0X"))
    {
        Some(hex) => i64::from_str_radix(hex, 16),
        None => digits.parse::<i64>(),
    }
    .map_err(|e| Ros2Error(format!("invalid enum value '{value}': {e}")))?;

    let number = if negative { -magnitude } else { magnitude };
    if !(i64::from(i32::MIN)..=i64::from(u32::MAX)).contains(&number) {
        return Err(Ros2Error(format!(
            "invalid enum value '{value}': outside the 32-bit range of an enum"
        )));
    }
    Ok(number)
}

fn paren_counts_outside_strings(s: &str, in_str: &mut bool, escaped: &mut bool) -> (usize, usize) {
    let mut open = 0usize;
    let mut close = 0usize;
    for ch in s.chars() {
        if *in_str {
            if *escaped {
                *escaped = false;
                continue;
            }
            match ch {
                '\\' => *escaped = true,
                '"' => *in_str = false,
                _ => {}
            }
            continue;
        }
        match ch {
            '"' => *in_str = true,
            '(' => open += 1,
            ')' => close += 1,
            _ => {}
        }
    }
    (open, close)
}

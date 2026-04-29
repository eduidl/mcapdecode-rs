//! ROS2 IDL parser implementation using nom parser combinators.
//!
//! This module provides a robust parser for ROS2 Interface Definition Language (IDL)
//! schemas. It uses the `nom` library for parser combinators, which provides better
//! error handling, composability, and maintainability compared to hand-written parsers.
//!
//! # Supported Features
//!
//! - Struct declarations with fields
//! - Enum declarations with variants
//! - Primitive types (bool, int8-64, uint8-64, float32/64, string, etc.)
//! - Sequence types (bounded and unbounded)
//! - Bounded strings and wide strings
//! - Fixed-size arrays
//! - Const declarations
//! - Module scoping
//! - Scoped type names (using :: or / separators)
//! - Annotations (ignored)
//! - Include directives (ignored)
//!
//! # Unsupported Features
//!
//! The following IDL features are explicitly unsupported and will return errors:
//! - Union types
//! - Typedef declarations
//! - Bitmask types

use std::collections::HashMap;

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
    let mut current_enum: Option<(String, Vec<String>)> = None;
    let mut pending_decl: Option<PendingDecl> = None;

    let mut annotation_depth = 0i32;
    let mut ann_in_str = false;
    let mut ann_escaped = false;
    let mut in_block_comment = false;

    for (idx, raw) in idl_body.lines().enumerate() {
        let line_no = idx + 1;
        let line = strip_comments(raw, &mut in_block_comment);
        let line = line.trim();
        if line.is_empty() {
            continue;
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
                    current_enum = Some((name, Vec::new()));
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
                    current_enum = Some((name, Vec::new()));
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
                        } else if let Some((name, variants)) = current_enum.take() {
                            let mut full = modules.clone();
                            full.push(name);
                            enums.insert(
                                full.clone(),
                                EnumDef {
                                    full_name: full,
                                    variants,
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

        if let Some((_, variants)) = current_enum.as_mut() {
            let name = parse_enum_variant(line)
                .map_err(|e| Ros2Error(format!("parse error at line {line_no}: {e}")))?;
            if !name.is_empty() {
                variants.push(name);
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

/// Parse enum variant: VARIANT or VARIANT = value
fn enum_variant(input: &str) -> IResult<&str, Option<&str>> {
    let trimmed = input.trim().trim_end_matches(',');
    if trimmed.is_empty() {
        return Ok((input, None));
    }

    alt((
        map(
            tuple((identifier, ws, char('='), take_while(|c: char| c != ','))),
            |(name, _, _, _)| Some(name),
        ),
        map(identifier, Some),
    ))(trimmed)
}

fn parse_enum_variant(line: &str) -> std::result::Result<String, Ros2Error> {
    match enum_variant(line) {
        Ok((_, Some(name))) => Ok(name.to_string()),
        Ok((_, None)) => Ok(String::new()),
        Err(e) => Err(format!("Failed to parse enum variant '{line}': {e}").into()),
    }
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

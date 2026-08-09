//! Type resolution: converts raw AST types into a fully-resolved schema.
//!
//! The key transformation is expanding every [`TypeExpr::Scoped`] reference
//! (a name like `"geometry_msgs::msg::Point"` or just `"Point"`) into either
//! a [`ResolvedType::Struct`] or [`ResolvedType::Enum`] by searching the
//! collected type maps.
//!
//! # Lookup strategy for scoped names
//!
//! 1. **Exact match** — look up the candidate key directly.
//! 2. **Enum exact match** — same, in the enum map.
//! 3. **Suffix match** — if the exact key is absent, find a unique entry whose
//!    key *ends with* the candidate segments (e.g. `["Point"]` resolves to
//!    `["geometry_msgs", "msg", "Point"]`).  Returns `None` if the suffix is
//!    ambiguous (more than one match).
//! 4. **Error** — if none of the above succeeds.

use std::collections::HashMap;

use mcapdecode_core::EnumVariant;

use crate::{
    ast::{EnumDef, FieldDef, ParsedSection, PrimitiveType, StructDef, TypeExpr},
    error::Ros2Error,
};

/// A fully-resolved type — all named references have been replaced with
/// their qualified keys into [`ResolvedSchema::structs`] / [`ResolvedSchema::enums`].
#[derive(Debug, Clone)]
pub enum ResolvedType {
    Primitive(PrimitiveType),
    /// Key into [`ResolvedSchema::structs`].
    Struct(Vec<String>),
    /// Key into [`ResolvedSchema::enums`].
    Enum(Vec<String>),
    Sequence {
        elem: Box<ResolvedType>,
        /// `None` for unbounded; `Some(n)` for bounded.
        max_len: Option<usize>,
    },
    /// UTF-8 string bounded to at most `N` bytes.
    BoundedString(usize),
    /// Wide string bounded to at most `N` characters.
    BoundedWString(usize),
}

/// A field with its type fully resolved.
#[derive(Debug, Clone)]
pub struct ResolvedField {
    pub name: String,
    pub ty: ResolvedType,
    /// `Some(n)` means this field is a fixed-length array of `n` elements.
    pub fixed_len: Option<usize>,
}

/// A struct with all its fields fully resolved.
#[derive(Debug, Clone)]
pub struct ResolvedStruct {
    pub fields: Vec<ResolvedField>,
}

/// The complete, self-contained schema needed for CDR decoding.
#[derive(Debug, Clone)]
pub struct ResolvedSchema {
    /// Qualified key of the top-level message type to decode.
    pub root: Vec<String>,
    /// All reachable struct definitions, keyed by qualified name.
    pub structs: HashMap<Vec<String>, ResolvedStruct>,
    /// All reachable enum definitions, keyed by qualified name.
    /// Values are named numeric variants used for value → name mapping.
    pub enums: HashMap<Vec<String>, Vec<EnumVariant>>,
}

/// Ensure that `builtin_interfaces::msg::Time` and `builtin_interfaces::msg::Duration`
/// are present in `all_structs`.
///
/// These types appear in virtually every stamped ROS 2 message but are not
/// bundled with the schemas of other packages, so they are injected here
/// when the caller has not provided them explicitly (e.g. when only a single
/// .msg file is available rather than a full [`SchemaBundle`]).
pub fn ensure_builtin_structs(all_structs: &mut HashMap<Vec<String>, StructDef>) {
    let time_name = vec![
        "builtin_interfaces".to_string(),
        "msg".to_string(),
        "Time".to_string(),
    ];
    all_structs
        .entry(time_name.clone())
        .or_insert_with(|| StructDef {
            full_name: time_name,
            fields: vec![
                FieldDef {
                    name: "sec".to_string(),
                    ty: TypeExpr::Primitive(PrimitiveType::I32),
                    fixed_len: None,
                },
                FieldDef {
                    name: "nanosec".to_string(),
                    ty: TypeExpr::Primitive(PrimitiveType::U32),
                    fixed_len: None,
                },
            ],
            consts: vec![],
        });

    let duration_name = vec![
        "builtin_interfaces".to_string(),
        "msg".to_string(),
        "Duration".to_string(),
    ];
    all_structs
        .entry(duration_name.clone())
        .or_insert_with(|| StructDef {
            full_name: duration_name,
            fields: vec![
                FieldDef {
                    name: "sec".to_string(),
                    ty: TypeExpr::Primitive(PrimitiveType::I32),
                    fixed_len: None,
                },
                FieldDef {
                    name: "nanosec".to_string(),
                    ty: TypeExpr::Primitive(PrimitiveType::U32),
                    fixed_len: None,
                },
            ],
            consts: vec![],
        });
}

/// Resolve all field types in a single struct definition.
pub fn resolve_struct(
    def: &StructDef,
    all_structs: &HashMap<Vec<String>, StructDef>,
    all_enums: &HashMap<Vec<String>, EnumDef>,
) -> Result<ResolvedStruct, Ros2Error> {
    let mut fields = Vec::with_capacity(def.fields.len());
    for f in &def.fields {
        let ty = resolve_type_expr(&f.ty, &def.full_name, all_structs, all_enums)?;
        fields.push(ResolvedField {
            name: f.name.clone(),
            ty,
            fixed_len: f.fixed_len,
        });
    }

    Ok(ResolvedStruct { fields })
}

/// Build a [`ResolvedSchema`] from parsed structs/enums and a selected root type.
///
/// This is the shared resolution backend used by both `ros2idl` and `ros2msg`.
/// Builtin `builtin_interfaces` structs are injected when missing.
pub fn resolve_parsed_section(
    mut parsed: ParsedSection,
    root: Vec<String>,
) -> Result<ResolvedSchema, Ros2Error> {
    ensure_builtin_structs(&mut parsed.structs);

    let mut out = HashMap::new();
    for (name, def) in &parsed.structs {
        let resolved = resolve_struct(def, &parsed.structs, &parsed.enums)?;
        out.insert(name.clone(), resolved);
    }

    let mut enum_out = HashMap::new();
    for (name, def) in &parsed.enums {
        enum_out.insert(name.clone(), def.variants.clone());
    }

    if !out.contains_key(&root) {
        return Err(format!(
            "root type '{}' not found in parsed structs",
            root.join("::")
        )
        .into());
    }

    Ok(ResolvedSchema {
        root,
        structs: out,
        enums: enum_out,
    })
}

/// Recursively resolve a [`TypeExpr`] within the context of `current_struct`.
///
/// Single-segment scoped names are first qualified with the enclosing module
/// before attempting exact and suffix lookups.
fn resolve_type_expr(
    expr: &TypeExpr,
    current_struct: &[String],
    all_structs: &HashMap<Vec<String>, StructDef>,
    all_enums: &HashMap<Vec<String>, EnumDef>,
) -> Result<ResolvedType, Ros2Error> {
    match expr {
        TypeExpr::Primitive(p) => Ok(ResolvedType::Primitive(p.clone())),
        TypeExpr::BoundedString(n) => Ok(ResolvedType::BoundedString(*n)),
        TypeExpr::BoundedWString(n) => Ok(ResolvedType::BoundedWString(*n)),
        TypeExpr::Sequence { elem, max_len } => Ok(ResolvedType::Sequence {
            elem: Box::new(resolve_type_expr(
                elem,
                current_struct,
                all_structs,
                all_enums,
            )?),
            max_len: *max_len,
        }),
        TypeExpr::Scoped(name) => {
            // For a single-segment name, prepend the enclosing module so that
            // intra-module references (e.g. `State` within `ex::msg`) resolve
            // to `ex::msg::State` before falling back to a global suffix search.
            let candidate = if name.len() == 1 {
                let mut scope = current_struct[..current_struct.len().saturating_sub(1)].to_vec();
                scope.push(name[0].clone());
                scope
            } else {
                name.clone()
            };

            if all_structs.contains_key(&candidate) {
                Ok(ResolvedType::Struct(candidate))
            } else if all_enums.contains_key(&candidate) {
                Ok(ResolvedType::Enum(candidate))
            } else if let Some(found) = find_by_suffix(all_structs, &candidate) {
                Ok(ResolvedType::Struct(found))
            } else if let Some(found) = find_by_suffix(all_enums, &candidate) {
                Ok(ResolvedType::Enum(found))
            } else {
                Err(format!(
                    "unresolved type '{}' in '{}'",
                    name.join("::"),
                    current_struct.join("::")
                )
                .into())
            }
        }
    }
}

/// Find the unique key in `map` whose suffix matches `wanted`.
///
/// Returns `None` if no key matches or if more than one key matches
/// (ambiguous reference).
fn find_by_suffix(
    all_structs: &HashMap<Vec<String>, impl Sized>,
    wanted: &[String],
) -> Option<Vec<String>> {
    let mut found: Option<Vec<String>> = None;
    for key in all_structs.keys() {
        if key.len() < wanted.len() {
            continue;
        }
        if key[key.len() - wanted.len()..] == *wanted {
            if found.is_some() {
                // Ambiguous — more than one key ends with `wanted`.
                return None;
            }
            found = Some(key.clone());
        }
    }
    found
}

/// Build a [`ResolvedSchema`] from a single struct definition.
///
/// This is the simplified path used by `mcapdecode-ros2msg`, where only one
/// .msg file is available (no multi-section [`SchemaBundle`]).
/// [`ensure_builtin_structs`] is called automatically so that fields like
/// `builtin_interfaces/Time` resolve without requiring explicit dependencies.
pub fn resolve_single_struct(
    _schema_name: &str,
    struct_def: StructDef,
) -> Result<ResolvedSchema, Ros2Error> {
    let mut parsed = ParsedSection::default();
    let root = struct_def.full_name.clone();
    parsed.structs.insert(root.clone(), struct_def);
    resolve_parsed_section(parsed, root)
}

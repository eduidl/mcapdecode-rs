//! ROS 2 schema rendering and CDR encoding for the generated models.

use crate::model::{FieldTy, Model, PACKAGE, Sample};

fn idl_type(ty: &FieldTy) -> String {
    match ty {
        FieldTy::U64 => "uint64".into(),
        FieldTy::F64Array(_) => "double".into(),
        FieldTy::ByteSeq(n) => format!("sequence<uint8, {n}>"),
        FieldTy::Str => "string".into(),
        FieldTy::Struct(name) => format!("{PACKAGE}::msg::{name}"),
    }
}

/// Render the model as a `====`-separated IDL bundle, one section per struct.
pub(crate) fn render_idl(model: &Model) -> String {
    let mut out = String::new();
    for (index, def) in model.structs.iter().enumerate() {
        if index > 0 {
            out.push_str(&"=".repeat(80));
            out.push('\n');
        }
        out.push_str(&format!("IDL: {PACKAGE}/msg/{}\n", def.name));
        out.push_str(&format!("module {PACKAGE} {{\n  module msg {{\n"));
        out.push_str(&format!("    struct {} {{\n", def.name));
        for (name, ty) in &def.fields {
            // Fixed-size arrays put the bound after the field name in IDL.
            let suffix = match ty {
                FieldTy::F64Array(n) => format!("[{n}]"),
                _ => String::new(),
            };
            out.push_str(&format!("      {} {name}{suffix};\n", idl_type(ty)));
        }
        out.push_str("    };\n  };\n};\n");
    }
    out
}

fn msg_type(ty: &FieldTy) -> String {
    match ty {
        FieldTy::U64 => "uint64".into(),
        FieldTy::F64Array(n) => format!("float64[{n}]"),
        FieldTy::ByteSeq(_) => "uint8[]".into(),
        FieldTy::Str => "string".into(),
        FieldTy::Struct(name) => format!("{PACKAGE}/{name}"),
    }
}

/// Render the model as a `.msg` bundle: the root section is bare, dependencies follow
/// behind `MSG:` headers.
pub(crate) fn render_msg(model: &Model) -> String {
    let mut out = String::new();
    for (index, def) in model.structs.iter().enumerate() {
        if index > 0 {
            out.push_str(&"=".repeat(80));
            out.push('\n');
            out.push_str(&format!("MSG: {PACKAGE}/{}\n", def.name));
        }
        for (name, ty) in &def.fields {
            out.push_str(&format!("{} {name}\n", msg_type(ty)));
        }
    }
    out
}

fn align(out: &mut Vec<u8>, n: usize) {
    // Alignment is relative to the end of the 4-byte encapsulation header.
    let pad = (n - ((out.len() - 4) % n)) % n;
    out.extend(std::iter::repeat_n(0u8, pad));
}

fn encode_sample(out: &mut Vec<u8>, sample: &Sample) {
    match sample {
        Sample::U64(v) => {
            align(out, 8);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Sample::F64List(values) => {
            for v in values {
                align(out, 8);
                out.extend_from_slice(&v.to_le_bytes());
            }
        }
        Sample::Bytes(bytes) => {
            align(out, 4);
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(bytes);
        }
        Sample::Str(s) => {
            // CDR strings are length-prefixed including the null terminator.
            align(out, 4);
            out.extend_from_slice(&((s.len() + 1) as u32).to_le_bytes());
            out.extend_from_slice(s.as_bytes());
            out.push(0);
        }
        Sample::Struct(fields) => {
            for field in fields {
                encode_sample(out, field);
            }
        }
    }
}

/// Encode a sample as little-endian CDR, including the encapsulation header.
pub(crate) fn encode_cdr(sample: &Sample) -> Vec<u8> {
    let mut out = vec![0, 1, 0, 0];
    encode_sample(&mut out, sample);
    out
}

//! The single description each payload case is generated from.
//!
//! [`Model`] defines the struct layout, [`Sample`] holds the concrete values, and the
//! encoders in [`crate::ros2`] and [`crate::protobuf`] render both. Because every
//! representation starts here, a schema can never describe something the payload does
//! not contain.

use crate::PayloadCase;

/// Package name shared by the generated ROS 2 and protobuf schemas.
pub(crate) const PACKAGE: &str = "bench_msgs";

const FLAT_FIELDS: [&str; 10] = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];
const BYTE_SEQ_LEN: usize = 1024 * 1024;
const NUMERIC_ARRAY_LEN: usize = 1024;
const SEED: u64 = 7;

/// Small deterministic LCG so benchmark data does not need `rand`.
pub struct Lcg(u64);

impl Lcg {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }
    pub fn next_u32(&mut self) -> u32 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
        (self.0 >> 32) as u32
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum FieldTy {
    U64,
    F64Array(usize),
    ByteSeq(usize),
    Str,
    Struct(&'static str),
}

pub(crate) struct StructDef {
    pub name: &'static str,
    pub fields: Vec<(&'static str, FieldTy)>,
}

/// The root struct is always the first entry of `structs`.
pub(crate) struct Model {
    pub structs: Vec<StructDef>,
}

impl Model {
    pub fn root(&self) -> &StructDef {
        &self.structs[0]
    }
    pub fn get(&self, name: &str) -> &StructDef {
        self.structs
            .iter()
            .find(|s| s.name == name)
            .expect("model references an undefined struct")
    }
}

pub(crate) fn model(case: PayloadCase) -> Model {
    match case {
        PayloadCase::Flat => Model {
            structs: vec![StructDef {
                name: "Sample",
                fields: FLAT_FIELDS.iter().map(|n| (*n, FieldTy::U64)).collect(),
            }],
        },
        // Four levels of nesting so that per-field work (path building, struct
        // lookups) dominates instead of raw byte volume.
        PayloadCase::Nested => Model {
            structs: vec![
                nesting_level("Sample", "Level1"),
                nesting_level("Level1", "Level2"),
                nesting_level("Level2", "Level3"),
                StructDef {
                    name: "Level3",
                    fields: vec![("a", FieldTy::U64), ("b", FieldTy::U64)],
                },
            ],
        },
        PayloadCase::Bytes => Model {
            structs: vec![StructDef {
                name: "Sample",
                fields: vec![("data", FieldTy::ByteSeq(BYTE_SEQ_LEN))],
            }],
        },
        PayloadCase::NumericArray => Model {
            structs: vec![StructDef {
                name: "Sample",
                fields: vec![("values", FieldTy::F64Array(NUMERIC_ARRAY_LEN))],
            }],
        },
        PayloadCase::Strings => Model {
            structs: vec![StructDef {
                name: "Sample",
                fields: FLAT_FIELDS.iter().map(|n| (*n, FieldTy::Str)).collect(),
            }],
        },
    }
}

fn nesting_level(name: &'static str, child: &'static str) -> StructDef {
    StructDef {
        name,
        fields: vec![("a", FieldTy::Struct(child)), ("b", FieldTy::Struct(child))],
    }
}

/// The generated data itself, kept separate from the wire encoding so that every
/// encoder and the expected-value builder all start from identical numbers.
#[derive(Clone, Debug, PartialEq)]
pub enum Sample {
    U64(u64),
    F64List(Vec<f64>),
    Bytes(Vec<u8>),
    Str(String),
    Struct(Vec<Sample>),
}

fn sample_of(model: &Model, def: &StructDef, rng: &mut Lcg) -> Sample {
    let fields = def
        .fields
        .iter()
        .map(|(_, ty)| match ty {
            FieldTy::U64 => Sample::U64(rng.next_u32() as u64),
            FieldTy::F64Array(n) => {
                Sample::F64List((0..*n).map(|_| f64::from(rng.next_u32())).collect())
            }
            FieldTy::ByteSeq(n) => Sample::Bytes((0..*n).map(|_| rng.next_u32() as u8).collect()),
            FieldTy::Str => Sample::Str(format!("bench-string-{}", rng.next_u32())),
            FieldTy::Struct(name) => sample_of(model, model.get(name), rng),
        })
        .collect();
    Sample::Struct(fields)
}

/// Build the deterministic sample tree for `case`. Benchmarks and round-trip tests
/// share this so expectations never have to be restated by hand.
pub fn sample(case: PayloadCase) -> Sample {
    let model = model(case);
    sample_of(&model, model.root(), &mut Lcg::new(SEED))
}

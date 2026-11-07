//! Field paths resolved once against a schema and reused for many values.

use crate::{DataTypeDef, ElementDef, FieldDef, Value, ValueAccessError, ValueTypeError};

/// One segment of a dotted path. A segment of only ASCII digits is an
/// element index; anything else is a field name.
pub(crate) enum Segment<'a> {
    Name(&'a str),
    Index(usize),
}

impl<'a> Segment<'a> {
    pub(crate) fn parse(segment: &'a str) -> Self {
        // `usize::from_str` also accepts a leading `+`, which is not an index.
        match segment.parse() {
            Ok(index) if segment.bytes().all(|b| b.is_ascii_digit()) => Self::Index(index),
            _ => Self::Name(segment),
        }
    }
}

#[derive(Debug, Clone)]
enum Step {
    /// A struct field by position, with its name kept to detect values decoded
    /// with a different schema. The last field step also keeps its definition,
    /// since it determines the type of everything the path reads below it.
    Field {
        index: usize,
        name: Box<str>,
        element: Option<Box<ElementDef>>,
    },
    /// An element of a list or fixed-size array.
    Element(usize),
}

/// A path through struct fields and sequence elements, resolved against a
/// schema before reading messages.
///
/// Paths are dot-separated, and a segment of only digits is an element index,
/// as in `poses.0.position.x`. Map entries cannot be addressed.
#[derive(Debug, Clone)]
pub struct FieldPath {
    steps: Vec<Step>,
    element: ElementDef,
}

impl FieldPath {
    /// Resolve a sequence of segments, such as `["poses", "0", "x"]`, against
    /// the top-level fields of a schema.
    ///
    /// Fails with [`ValueAccessError::NotFound`] for an unknown field, an empty
    /// path, or an index beyond a fixed-size array, and with
    /// [`ValueAccessError::TypeMismatch`] when a segment does not apply to the
    /// type it is used on.
    pub fn resolve<S: AsRef<str>>(
        fields: &[FieldDef],
        segments: impl IntoIterator<Item = S>,
    ) -> Result<Self, ValueAccessError> {
        let mut steps = Vec::new();
        let mut current: Option<&ElementDef> = None;
        let mut last_field = None;
        for segment in segments {
            let segment = segment.as_ref();
            let next = match Segment::parse(segment) {
                Segment::Name(name) => {
                    let fields = match current.map(|element| &element.data_type) {
                        None => fields,
                        Some(DataTypeDef::Struct(definition)) => &definition.fields,
                        Some(other) => return Err(mismatch("struct", other)),
                    };
                    let index = fields
                        .iter()
                        .position(|field| field.name == name)
                        .ok_or(ValueAccessError::NotFound)?;
                    last_field = Some((steps.len(), &fields[index].element));
                    steps.push(Step::Field {
                        index,
                        name: name.into(),
                        element: None,
                    });
                    &fields[index].element
                }
                Segment::Index(index) => {
                    let element = match current.map(|element| &element.data_type) {
                        Some(DataTypeDef::List(element) | DataTypeDef::BoundedList(element, _)) => {
                            element
                        }
                        Some(DataTypeDef::Array(element, len)) => {
                            if index >= *len {
                                return Err(ValueAccessError::NotFound);
                            }
                            element
                        }
                        Some(other) => return Err(mismatch("list or array", other)),
                        None => return Err(ValueAccessError::NotFound),
                    };
                    steps.push(Step::Element(index));
                    element.as_ref()
                }
            };
            current = Some(next);
        }
        let element = current.ok_or(ValueAccessError::NotFound)?.clone();
        // A path always starts with a field, since the top level is a struct.
        if let Some((position, definition)) = last_field
            && let Step::Field { element, .. } = &mut steps[position]
        {
            *element = Some(Box::new(definition.clone()));
        }
        Ok(Self { steps, element })
    }

    /// Resolve a dotted path such as `"poses.0.position.x"`.
    pub fn parse(fields: &[FieldDef], path: &str) -> Result<Self, ValueAccessError> {
        Self::resolve(fields, path.split('.'))
    }

    /// The type and nullability of the value the path points to.
    pub fn element(&self) -> &ElementDef {
        &self.element
    }

    /// The type of the value the path points to.
    pub fn data_type(&self) -> &DataTypeDef {
        &self.element.data_type
    }

    /// Follow the path through a top-level message value.
    ///
    /// Passing through a null value fails with [`ValueAccessError::NullValue`],
    /// as with [`Value::get`]; a null at the end of the path is returned as is.
    /// A value decoded with a different schema fails with
    /// [`ValueAccessError::SchemaMismatch`]: each struct on the path must have
    /// the resolved field name at the resolved position, and the last field on
    /// the path must have the resolved definition.
    pub fn get<'a>(&self, value: &'a Value) -> Result<&'a Value, ValueAccessError> {
        self.steps.iter().try_fold(value, |value, step| match step {
            Step::Field {
                index,
                name,
                element,
            } => {
                let Value::Struct(fields) = value else {
                    return Err(value.access_mismatch("Struct"));
                };
                fields.check_schema()?;
                match fields.schema().fields.get(*index) {
                    Some(field)
                        if field.name == **name
                            && element.as_deref().is_none_or(|e| field.element == *e) =>
                    {
                        Ok(&fields[*index])
                    }
                    _ => Err(ValueAccessError::SchemaMismatch),
                }
            }
            Step::Element(index) => value
                .as_sequence()?
                .get(*index)
                .ok_or(ValueAccessError::NotFound),
        })
    }
}

fn mismatch(expected: &str, actual: &DataTypeDef) -> ValueAccessError {
    ValueTypeError::new(expected, actual.type_name()).into()
}

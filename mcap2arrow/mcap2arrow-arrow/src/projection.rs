use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, FixedSizeListArray, ListArray, StructArray},
    datatypes::{DataType, Field, Fields, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};

/// Describes which fields (and sub-fields) to keep.
///
/// - [`Selection::All`] – keep the entire subtree.
/// - [`Selection::Partial`] – keep only the named children (recursively).
enum Selection {
    All,
    Partial(HashMap<String, Selection>),
}

impl Selection {
    /// Build a selection tree from a slice of dot-separated field paths.
    ///
    /// If `paths` is empty the result is [`Selection::All`].
    fn from_paths(paths: &[String]) -> Self {
        if paths.is_empty() {
            return Self::All;
        }
        let mut map: HashMap<String, Selection> = HashMap::new();
        for path in paths {
            Self::insert_path(&mut map, path.as_str());
        }
        Self::Partial(map)
    }

    fn insert_path(map: &mut HashMap<String, Selection>, path: &str) {
        let (head, tail) = match path.split_once('.') {
            Some((h, t)) => (h, Some(t)),
            None => (path, None),
        };

        match map.get_mut(head) {
            // Already selecting the entire subtree; nothing to do.
            Some(Self::All) => {}
            Some(Self::Partial(children)) => {
                if let Some(rest) = tail {
                    Self::insert_path(children, rest);
                } else {
                    // A less-specific path supersedes; promote to All.
                    *map.get_mut(head).unwrap() = Self::All;
                }
            }
            None => {
                if let Some(rest) = tail {
                    let mut children = HashMap::new();
                    Self::insert_path(&mut children, rest);
                    map.insert(head.to_owned(), Self::Partial(children));
                } else {
                    map.insert(head.to_owned(), Self::All);
                }
            }
        }
    }
}

/// Project a [`RecordBatch`] to only the fields described by `field_paths`.
///
/// Each path is a dot-separated field name, e.g. `"position.x"`.  A bare
/// name like `"position"` selects the entire struct column including all its
/// children.  Specifying both `"position"` and `"position.x"` is equivalent
/// to specifying only `"position"`.
///
/// The projection is applied **before** flattening so that the result is
/// format-independent:
/// - For JSONL output (no flattening) `"position.x"` produces a `position`
///   struct column that contains only the `x` child field.
/// - For CSV / Parquet output the subsequent `flatten_record_batch` call then
///   expands that pruned struct to a single `position.x` column.
///
/// An empty `field_paths` slice returns the batch unchanged.
/// Columns not mentioned in `field_paths` are silently dropped.
///
/// # Errors
///
/// Returns [`ArrowError::InvalidArgumentError`] if a path refers to a
/// non-existent field.
pub fn project_record_batch(
    batch: &RecordBatch,
    field_paths: &[String],
) -> Result<RecordBatch, ArrowError> {
    let selection = Selection::from_paths(field_paths);
    let (fields, arrays) = project_fields(batch.schema().fields(), batch.columns(), &selection)?;
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays)
}

/// Recursively select fields from `fields`/`arrays` according to `selection`.
fn project_fields(
    fields: &Fields,
    arrays: &[ArrayRef],
    selection: &Selection,
) -> Result<(Vec<Field>, Vec<ArrayRef>), ArrowError> {
    match selection {
        Selection::All => {
            let out_fields = fields.iter().map(|f| f.as_ref().clone()).collect();
            Ok((out_fields, arrays.to_vec()))
        }
        Selection::Partial(children) => {
            // Validate that every requested name exists before collecting output.
            for name in children.keys() {
                if !fields.iter().any(|f| f.name() == name) {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "field '{name}' not found in schema"
                    )));
                }
            }

            let mut out_fields = Vec::new();
            let mut out_arrays = Vec::new();

            for (field, array) in fields.iter().zip(arrays.iter()) {
                let Some(child_sel) = children.get(field.name()) else {
                    continue;
                };
                let (f, a) = project_field(field, array, child_sel)?;
                out_fields.push(f);
                out_arrays.push(a);
            }

            Ok((out_fields, out_arrays))
        }
    }
}

/// Apply `selection` to a single field/array pair, recursing into structs as needed.
fn project_field(
    field: &Field,
    array: &ArrayRef,
    selection: &Selection,
) -> Result<(Field, ArrayRef), ArrowError> {
    match selection {
        Selection::All => Ok((field.as_ref().clone(), array.clone())),
        Selection::Partial(_) => match field.data_type() {
            DataType::Struct(child_fields) => {
                let struct_arr = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("DataType::Struct matches StructArray");
                let child_arrays: Vec<ArrayRef> = (0..child_fields.len())
                    .map(|i| struct_arr.column(i).clone())
                    .collect();

                let (pruned_fields, pruned_arrays) =
                    project_fields(child_fields, &child_arrays, selection)?;

                let pruned_schema_fields = Fields::from(pruned_fields.clone());
                let new_struct = StructArray::new(
                    pruned_schema_fields.clone(),
                    pruned_arrays,
                    struct_arr.nulls().cloned(),
                );
                // NOTE: We intentionally reconstruct parent fields without carrying metadata.
                // Current callers do not depend on schema metadata in projected output.
                let new_field = Field::new(
                    field.name(),
                    DataType::Struct(pruned_schema_fields),
                    field.is_nullable(),
                );
                Ok((new_field, Arc::new(new_struct)))
            }
            DataType::List(item_field) if matches!(item_field.data_type(), DataType::Struct(_)) => {
                let list_arr = array
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .expect("DataType::List matches ListArray");
                let (pruned_item_field, pruned_values) =
                    project_field(item_field.as_ref(), list_arr.values(), selection)?;
                let pruned_item_field = Arc::new(pruned_item_field);
                let new_list = ListArray::new(
                    pruned_item_field.clone(),
                    list_arr.offsets().clone(),
                    pruned_values,
                    list_arr.nulls().cloned(),
                );
                // NOTE: We intentionally reconstruct parent fields without carrying metadata.
                // Current callers do not depend on schema metadata in projected output.
                let new_field = Field::new(
                    field.name(),
                    DataType::List(pruned_item_field),
                    field.is_nullable(),
                );
                Ok((new_field, Arc::new(new_list)))
            }
            DataType::FixedSizeList(item_field, size)
                if matches!(item_field.data_type(), DataType::Struct(_)) =>
            {
                let fsl_arr = array
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .expect("DataType::FixedSizeList matches FixedSizeListArray");
                let (pruned_item_field, pruned_values) =
                    project_field(item_field.as_ref(), fsl_arr.values(), selection)?;
                let pruned_item_field = Arc::new(pruned_item_field);
                let size = *size;
                let new_fsl = FixedSizeListArray::new(
                    pruned_item_field.clone(),
                    size,
                    pruned_values,
                    fsl_arr.nulls().cloned(),
                );
                // NOTE: We intentionally reconstruct parent fields without carrying metadata.
                // Current callers do not depend on schema metadata in projected output.
                let new_field = Field::new(
                    field.name(),
                    DataType::FixedSizeList(pruned_item_field, size),
                    field.is_nullable(),
                );
                Ok((new_field, Arc::new(new_fsl)))
            }
            DataType::List(_) | DataType::FixedSizeList(_, _) => {
                // Keep behavior for lists with non-struct items (e.g. List<UInt8>):
                // a sub-path does not prune inner values and the whole column is kept.
                Ok((field.as_ref().clone(), array.clone()))
            }
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "field '{}' is not nested and cannot select sub-field",
                field.name()
            ))),
        },
    }
}

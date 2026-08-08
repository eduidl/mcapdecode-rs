use std::path::PathBuf;

use arrow::pyarrow::{PyArrowType, Table};
use mcapdecode::{McapReader, McapReaderError, TopicInfo};
use pyo3::{
    create_exception,
    exceptions::{PyException, PyOSError},
    prelude::*,
    types::{PyAny, PyModule},
    wrap_pyfunction,
};

create_exception!(mcapdecode, McapDecodeError, PyException);

type TopicInfoPickleArgs = (String, Option<u64>, Option<String>, String, String, usize);
type TopicInfoReduce<'py> = (Bound<'py, PyAny>, TopicInfoPickleArgs);

#[pyclass(module = "mcapdecode", frozen, get_all, name = "TopicInfo")]
struct PyTopicInfo {
    topic: String,
    message_count: Option<u64>,
    schema_name: Option<String>,
    schema_encoding: String,
    message_encoding: String,
    channel_count: usize,
}

impl From<TopicInfo> for PyTopicInfo {
    fn from(value: TopicInfo) -> Self {
        Self {
            topic: value.topic,
            message_count: value.message_count,
            schema_name: value.schema_name,
            schema_encoding: value.schema_encoding,
            message_encoding: value.message_encoding,
            channel_count: value.channel_count,
        }
    }
}

#[pymethods]
impl PyTopicInfo {
    #[staticmethod]
    #[pyo3(name = "_from_fields")]
    fn from_fields(
        topic: String,
        message_count: Option<u64>,
        schema_name: Option<String>,
        schema_encoding: String,
        message_encoding: String,
        channel_count: usize,
    ) -> Self {
        Self {
            topic,
            message_count,
            schema_name,
            schema_encoding,
            message_encoding,
            channel_count,
        }
    }

    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<TopicInfoReduce<'py>> {
        let rebuild = py
            .import("mcapdecode")?
            .getattr("_topic_info_from_fields")?;
        Ok((
            rebuild,
            (
                self.topic.clone(),
                self.message_count,
                self.schema_name.clone(),
                self.schema_encoding.clone(),
                self.message_encoding.clone(),
                self.channel_count,
            ),
        ))
    }
}

#[pyfunction]
fn list_topics(py: Python<'_>, path: PathBuf) -> PyResult<Vec<PyTopicInfo>> {
    let reader = default_reader();
    py.detach(|| reader.list_topics(&path))
        .map(|topics| topics.into_iter().map(PyTopicInfo::from).collect())
        .map_err(map_reader_error)
}

#[pyfunction]
fn read(py: Python<'_>, path: PathBuf, topic: &str) -> PyResult<PyArrowType<Table>> {
    let reader = default_reader();
    let topic = topic.to_owned();
    let (schema, batches) = py
        .detach(|| {
            let mut batches = Vec::new();
            let schema = reader.for_each_record_batch_with_schema(&path, &topic, |batch| {
                batches.push(batch);
                Ok(())
            })?;
            Ok::<_, McapReaderError>((schema, batches))
        })
        .map_err(map_reader_error)?;

    Table::try_new(batches, schema)
        .map(PyArrowType)
        .map_err(|error| McapDecodeError::new_err(error.to_string()))
}

fn default_reader() -> McapReader {
    McapReader::builder().with_default_decoders().build()
}

fn map_reader_error(error: McapReaderError) -> PyErr {
    match error {
        McapReaderError::Io(source) => PyOSError::new_err(source.to_string()),
        other => McapDecodeError::new_err(other.to_string()),
    }
}

#[pymodule]
fn _mcapdecode(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("McapDecodeError", module.py().get_type::<McapDecodeError>())?;
    module.add_class::<PyTopicInfo>()?;
    module.add_function(wrap_pyfunction!(list_topics, module)?)?;
    module.add_function(wrap_pyfunction!(read, module)?)?;
    Ok(())
}

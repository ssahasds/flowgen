use arrow::{
    array::{AsArray, ListArray, StringArray},
    datatypes::DataType,
};
use serde::{Deserialize, Serialize};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error with an Apache Arrow data.")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("Provided value is not an object.")]
    NotObject(),
    #[error("Missing required attributes.")]
    MissingRequiredAttribute(String),
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Input {
    pub value: String,
    pub is_static: bool,
    pub is_extension: bool,
    pub index: Option<usize>,
    pub nested: Option<Box<Self>>,
}

fn extract(record_batch: &arrow::array::RecordBatch, input: &Input) -> Result<String, Error> {
    let mut value = String::new();
    let array = record_batch.column_by_name(&input.value);
    if let Some(array) = array {
        let data_type = array.data_type();
        match data_type {
            DataType::Utf8 => {
                let array_data = array.as_any().downcast_ref::<StringArray>();
                for item in array_data.unwrap().into_iter().flatten() {
                    value = item.to_string();
                }
            }
            DataType::List(_) => {
                if let Some(i) = input.index {
                    let array_data: ListArray = array.to_data().into();
                    let value = array_data.value(i);
                    println!("{:?}", value.as_struct().column_by_name("status").unwrap());
                }
            }
            _ => {}
        }
    }
    Ok(value)
}
impl Input {
    pub fn extract_from(
        &self,
        data: &arrow::array::RecordBatch,
        extensions: &Option<arrow::array::RecordBatch>,
    ) -> Result<String, Error> {
        let mut value = String::new();
        if !self.is_extension {
            value = extract(data, self).unwrap();
        }
        if self.is_extension {
            if let Some(extensions) = extensions {
                value = extract(extensions, self).unwrap();
            }
        }
        if self.is_static {
            value = self.value.to_string()
        }
        Ok(value)
    }
}

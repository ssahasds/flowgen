use std::sync::Arc;

use arrow::{
    array::{Array, ListArray, StringArray, StructArray},
    datatypes::DataType,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

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
    pub index: usize,
    pub nested: Option<Box<Self>>,
}

fn extract_from_array(array: &Arc<dyn Array>, input: &Input) -> Result<Value, Error> {
    let mut value = Value::Null;
    let data_type = array.data_type();
    match data_type {
        DataType::Utf8 => {
            let array_data = array.as_any().downcast_ref::<StringArray>();
            value = Value::String(
                array_data
                    .unwrap()
                    .value(input.index)
                    .to_string()
                    .to_string(),
            );
        }
        DataType::List(_) => {
            let array_data = array.as_any().downcast_ref::<ListArray>();
            let nested_array = array_data.unwrap().value(input.index);
            if let Some(nested_input) = &input.nested {
                let input = &Input {
                    value: nested_input.value.clone(),
                    is_static: nested_input.is_static,
                    is_extension: nested_input.is_extension,
                    index: nested_input.index,
                    nested: nested_input.nested.clone(),
                };
                value = extract_from_array(&nested_array, input).unwrap();
            } else {
                value = extract_from_array(&nested_array, input).unwrap();
            }
        }
        DataType::Struct(_) => {
            let array_data = array.as_any().downcast_ref::<StructArray>();
            let nested_array = array_data.unwrap().column_by_name(&input.value).unwrap();
            if let Some(nested_input) = &input.nested {
                let input = &Input {
                    value: nested_input.value.clone(),
                    is_static: nested_input.is_static,
                    is_extension: nested_input.is_extension,
                    index: nested_input.index,
                    nested: nested_input.nested.clone(),
                };
                value = extract_from_array(nested_array, input).unwrap();
            } else {
                value = extract_from_array(nested_array, input).unwrap();
            }
        }
        _ => {}
    }
    Ok(value)
}

impl Input {
    pub fn extract(
        &self,
        data: &arrow::array::RecordBatch,
        extensions: &Option<arrow::array::RecordBatch>,
    ) -> Result<Value, Error> {
        let mut value = Value::Null;
        if !self.is_extension {
            let array: Option<&Arc<dyn Array>> = data.column_by_name(&self.value);
            if let Some(array) = array {
                value = extract_from_array(array, self).unwrap();
            }
        }
        if self.is_extension {
            if let Some(extensions) = extensions {
                let array: Option<&Arc<dyn Array>> = extensions.column_by_name(&self.value);
                if let Some(array) = array {
                    value = extract_from_array(array, self).unwrap();
                }
            }
        }
        if self.is_static {
            value = Value::String(self.value.to_string());
        }
        Ok(value)
    }
}

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Writer {
    pub credentials: String,
    pub path: PathBuf,
    pub columns: Vec<Column>,
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum DataType {
    #[default]
    Utf8,
}

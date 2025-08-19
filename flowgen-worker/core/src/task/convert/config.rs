use serde::{Deserialize, Serialize};

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Processor {
    pub label: Option<String>,
    pub target_format: TargetFormat,
    pub schema: Option<String>,
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum TargetFormat {
    #[default]
    Avro,
}

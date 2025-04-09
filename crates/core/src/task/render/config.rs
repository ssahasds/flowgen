use crate::config::input::Input;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Processor {
    pub label: Option<String>,
    pub template: String,
    pub inputs: Option<HashMap<String, Input>>,
}

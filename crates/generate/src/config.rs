use serde::{Deserialize, Serialize};

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Source {
    pub message: String,
    pub interval: u64,
    pub count: Option<u64>,
}

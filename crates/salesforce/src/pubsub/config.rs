use flowgen_core::config::input::Input;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Subscriber {
    pub credentials: String,
    pub topic_list: Vec<String>,
    pub durable_consumer_options: Option<DurableConsumerOptions>,
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Publisher {
    pub credentials: String,
    pub topic: String,
    pub payload: Map<String, Value>,
    pub inputs: Option<HashMap<String, Input>>,
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct DurableConsumerOptions {
    pub enabled: bool,
    pub managed_subscription: bool,
    pub name: String,
}

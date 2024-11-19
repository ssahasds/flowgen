use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub struct Source {
    pub credentials: String,
    pub topic_list: Vec<String>,
}

#[derive(Deserialize, Clone)]
pub struct Target {
    pub credentials: String,
    pub topic: String,
}

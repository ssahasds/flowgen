use crate::event::AvroData;

use super::super::super::{
    config::ConfigExt,
    event::{Event, EventBuilder, EventData},
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_avro_fast::{ser, Schema};
use serde_json::{json, Map, Value};
use std::{ptr::null, sync::Arc};
use tokio::{
    fs,
    sync::broadcast::{Receiver, Sender},
};
use tracing::{event, Level};

const DEFAULT_MESSAGE_SUBJECT: &str = "convert";

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    SendMessage(#[from] tokio::sync::broadcast::error::SendError<Event>),
    #[error(transparent)]
    Event(#[from] super::super::super::event::Error),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error("missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    #[error("provided attribute not found")]
    NotFound(),
    #[error("either payload json or payload input is required")]
    PayloadConfig(),
}

fn transform_keys(value: &mut Value) {
    if let Some(headers) = value.get_mut("headers") {
        if let Value::Object(headers_map) = headers {
            let mut new_map = Map::new();

            // Collect keys that need to be renamed
            let keys_to_rename: Vec<String> = headers_map
                .keys()
                .filter(|k| k.contains("-"))
                .cloned()
                .collect();

            // Remove old keys and insert with new names
            for old_key in keys_to_rename {
                if let Some(val) = headers_map.remove(&old_key) {
                    let new_key = old_key.replace("-", "_");
                    new_map.insert(new_key, val);
                }
            }

            // Add the new keys to the original map
            for (key, val) in new_map {
                headers_map.insert(key, val);
            }
        }
    }
}

/// Handles processing of https call outs.
#[derive(Clone, Debug)]
struct EventHandler {
    /// Processor configuration settings.
    config: Arc<super::config::Processor>,
    /// Channel sender for processed events
    tx: Sender<Event>,
    /// Task identifier for event tracking
    current_task_id: usize,
}

impl EventHandler {
    /// Processes an event and converts to selected target format.
    async fn handle(self, event: Event) -> Result<(), Error> {
        let data = match event.data {
            EventData::ArrowRecordBatch(record_batch) => todo!(),
            EventData::Avro(data) => EventData::Avro(data),
            EventData::Json(mut data) => match self.config.target_format {
                crate::task::convert::config::TargetFormat::Avro => match &self.config.schema {
                    Some(schema) => {
                        transform_keys(&mut data);
                        let parsed_schema: serde_avro_fast::Schema = schema.parse().unwrap();
                        let serializer_config = &mut ser::SerializerConfig::new(&parsed_schema);

                        let raw_bytes: Vec<u8> =
                            serde_avro_fast::to_datum_vec(&data, serializer_config).unwrap();

                        EventData::Avro(AvroData {
                            schema: schema.to_string(),
                            raw_bytes,
                        })
                    }
                    None => EventData::Json(data),
                },
            },
        };

        let timestamp = Utc::now().timestamp_micros();
        let subject = match &self.config.label {
            Some(label) => format!(
                "{}.{}.{}",
                DEFAULT_MESSAGE_SUBJECT,
                label.to_lowercase(),
                timestamp
            ),
            None => format!("{DEFAULT_MESSAGE_SUBJECT}.{timestamp}"),
        };

        // Send processor output as event.
        let e = EventBuilder::new()
            .data(data)
            .subject(subject.clone())
            .current_task_id(self.current_task_id)
            .build()?;

        self.tx.send(e)?;
        event!(Level::INFO, "Event processed: {}", subject);
        Ok(())
    }
}

#[derive(Debug)]
pub struct Processor {
    config: Arc<super::config::Processor>,
    tx: Sender<Event>,
    rx: Receiver<Event>,
    current_task_id: usize,
}

impl super::super::runner::Runner for Processor {
    type Error = Error;
    async fn run(mut self) -> Result<(), Error> {
        while let Ok(event) = self.rx.recv().await {
            if event.current_task_id == Some(self.current_task_id - 1) {
                let config = Arc::clone(&self.config);
                let tx = self.tx.clone();
                let current_task_id = self.current_task_id;

                let event_handler = EventHandler {
                    config,
                    current_task_id,
                    tx,
                };
                tokio::spawn(async move {
                    if let Err(err) = event_handler.handle(event).await {
                        event!(Level::ERROR, "{}", err);
                    }
                });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<super::config::Processor>>,
    tx: Option<Sender<Event>>,
    rx: Option<Receiver<Event>>,
    current_task_id: usize,
}

impl ProcessorBuilder {
    pub fn new() -> ProcessorBuilder {
        ProcessorBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Processor>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
        self
    }

    pub async fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingRequiredAttribute("sender".to_string()))?,
            current_task_id: self.current_task_id,
        })
    }
}

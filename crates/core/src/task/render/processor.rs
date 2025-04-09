use crate::{
    convert::{recordbatch::RecordBatchExt, render::Render},
    stream::event::{Event, EventBuilder},
};
use chrono::Utc;
use futures_util::future::try_join_all;
use serde_json::{Map, Value};
use std::sync::Arc;
use tokio::{
    sync::broadcast::{Receiver, Sender},
    task::JoinHandle,
};
use tracing::{event, Level};

const DEFAULT_MESSAGE_SUBJECT: &str = "render";
const DEFAULT_RENDERED_KEY_NAME: &str = "content";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error with sending event over channel")]
    SendMessage(#[source] tokio::sync::broadcast::error::SendError<Event>),
    #[error("error with creating event")]
    Event(#[source] crate::stream::event::Error),
    #[error("error with processing Record Batch")]
    RecordBatch(#[source] crate::convert::recordbatch::Error),
    #[error("error with rendering a given value")]
    Render(#[source] crate::convert::render::Error),
    #[error("error with parsing a given value")]
    SerdeJson(#[source] serde_json::Error),
    #[error("missing required attrubute")]
    MissingRequiredAttribute(String),
}
pub struct Processor {
    config: Arc<super::config::Processor>,
    tx: Sender<Event>,
    rx: Receiver<Event>,
    current_task_id: usize,
}

impl crate::task::runner::Runner for Processor {
    type Error = Error;
    async fn run(mut self) -> Result<(), Error> {
        let mut handle_list = Vec::new();
        while let Ok(event) = self.rx.recv().await {
            if event.current_task_id == Some(self.current_task_id - 1) {
                let config = Arc::clone(&self.config);
                let tx = self.tx.clone();
                let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                    // Get input dynamic values.
                    let mut data = Map::new();
                    if let Some(inputs) = &config.inputs {
                        for (key, input) in inputs {
                            let value = input.extract(&event.data, &event.extensions);
                            if let Ok(value) = value {
                                data.insert(key.to_owned(), value);
                            }
                        }
                    }

                    // Render content.
                    let template = config
                        .template
                        .to_string()
                        .render(&data)
                        .map_err(Error::Render)?;
                    let mut rendered_map = Map::new();
                    rendered_map.insert(
                        DEFAULT_RENDERED_KEY_NAME.to_string(),
                        Value::String(template),
                    );
                    let rendered =
                        serde_json::to_string(&rendered_map).map_err(Error::SerdeJson)?;

                    // Prepare processor output.
                    let recordbatch = rendered.to_recordbatch().map_err(Error::RecordBatch)?;
                    let extensions = Value::Object(data)
                        .to_string()
                        .to_recordbatch()
                        .map_err(Error::RecordBatch)?;

                    let timestamp = Utc::now().timestamp_micros();
                    let subject = match &config.label {
                        Some(label) => format!(
                            "{}.{}.{}",
                            DEFAULT_MESSAGE_SUBJECT,
                            label.to_lowercase(),
                            timestamp
                        ),
                        None => format!("{}.{}", DEFAULT_MESSAGE_SUBJECT, timestamp),
                    };
                    let event_message = format!("event processed: {}", subject);

                    // Send processor output as event.
                    let e = EventBuilder::new()
                        .data(recordbatch)
                        .extensions(extensions)
                        .subject(subject)
                        .current_task_id(self.current_task_id)
                        .build()
                        .map_err(Error::Event)?;

                    tx.send(e).map_err(Error::SendMessage)?;
                    event!(Level::INFO, "{}", event_message);
                    Ok(())
                });
                handle_list.push(handle);
            }
        }
        let _ = try_join_all(handle_list.iter_mut()).await;
        Ok(())
    }
}

#[derive(Default)]
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

use flowgen_core::{
    cache::Cache,
    connect::client::Client,
    event::{AvroData, Event, EventBuilder, EventData},
};
use chrono::Utc;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::{event, Level};

const DEFAULT_MESSAGE_SUBJECT: &str = "bulkapi";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    SendMessage(#[from] tokio::sync::broadcast::error::SendError<Event>),

    #[error("missing required attrubute")]
    MissingRequiredAttribute(String),
    #[error("no array data available")]
    EmptyArray(),
}

pub struct Processor {
    config: Arc<super::config::Processor>,
    tx: Sender<Event>,
    rx: Receiver<Event>,
    current_task_id: usize,
}

impl flowgen_core::task::runner::Runner for Processor {
    type Error = Error;
    async fn run(mut self) -> Result<(), Error> {

        while let Ok(event) = self.rx.recv().await {
            if event.current_task_id == Some(self.current_task_id - 1) {
                println!("Processing event: {:?}", event);
                println!("Processing config: {:?}", self.config.label);
            }
        }
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

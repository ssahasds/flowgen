use flowgen_core::{
    client::Client,
    event::Event,
    render::Render,
    serde::{MapExt, StringExt},
};
use salesforce_pubsub::eventbus::v1::{ProducerEvent, PublishRequest, SchemaRequest, TopicRequest};
use serde_json::{Map, Value};
use std::{path::Path, sync::Arc};
use tokio::sync::{broadcast::Receiver, Mutex};

#[derive(thiserror::Error, Debug)]
pub enum PublisherError {
    #[error("There was an error with PubSub context.")]
    SalesforcePubSubError(#[source] super::context::Error),
    #[error("There was an error with Salesforce authentication.")]
    SalesforceAuthError(#[source] crate::client::Error),
    #[error("There was an error with parsing a given value.")]
    SerdeError(#[source] flowgen_core::serde::Error),
    #[error("There was an error with parsing a given value.")]
    SerdeJsonError(#[source] serde_json::error::Error),
    #[error("There was an error with rendering a given value.")]
    RenderError(#[source] flowgen_core::render::Error),
    #[error("Missing required event attrubute.")]
    MissingRequiredAttributeError(String),
}

pub struct Publisher {
    service: flowgen_core::service::Service,
    config: Arc<super::config::Target>,
    rx: Receiver<Event>,
    current_task_id: usize,
}

impl flowgen_core::publisher::Publisher for Publisher {
    type Error = PublisherError;
    async fn publish(mut self) -> Result<(), Self::Error> {
        let config = self.config.as_ref();
        let a = Path::new(&config.credentials);
        let sfdc_client = crate::client::Builder::new()
            .with_credentials_path(a.to_path_buf())
            .build()
            .map_err(PublisherError::SalesforceAuthError)?
            .connect()
            .await
            .map_err(PublisherError::SalesforceAuthError)?;

        let pubsub = super::context::Builder::new(self.service)
            .with_client(sfdc_client)
            .build()
            .map_err(PublisherError::SalesforcePubSubError)?;

        let pubsub = Arc::new(Mutex::new(pubsub));

        let topic_info = pubsub
            .lock()
            .await
            .get_topic(TopicRequest {
                topic_name: self.config.topic.clone(),
            })
            .await
            .map_err(PublisherError::SalesforcePubSubError)?
            .into_inner();

        let schema_info = pubsub
            .lock()
            .await
            .get_schema(SchemaRequest {
                schema_id: topic_info.schema_id,
            })
            .await
            .map_err(PublisherError::SalesforcePubSubError)?
            .into_inner();

        let pubsub = pubsub.clone();

        let topic_name = &self.config.topic;
        let schema_id = &schema_info.schema_id;

        while let Ok(event) = self.rx.recv().await {
            if event.current_task_id == Some(self.current_task_id - 1) {
                let mut data = Map::new();
                if let Some(inputs) = &self.config.inputs {
                    for (key, input) in inputs {
                        let value = input.extract(&event.data, &event.extensions);
                        if let Ok(value) = value {
                            data.insert(key.to_string(), Value::String(value.to_string()));
                        }
                    }
                }

                let payload = self
                    .config
                    .payload
                    .to_string()
                    .map_err(PublisherError::SerdeError)?
                    .render(&data)
                    .map_err(PublisherError::RenderError)?
                    .to_value()
                    .map_err(PublisherError::SerdeError)?;

                let mut bytes: Vec<u8> = Vec::new();
                serde_json::to_writer(&mut bytes, &payload)
                    .map_err(PublisherError::SerdeJsonError)?;

                let mut events = Vec::new();
                let pe = ProducerEvent {
                    schema_id: schema_id.to_string(),
                    payload: bytes,
                    ..Default::default()
                };
                events.push(pe);

                let resp = pubsub
                    .lock()
                    .await
                    .publish(PublishRequest {
                        topic_name: topic_name.to_string(),
                        events,
                        ..Default::default()
                    })
                    .await
                    .map_err(PublisherError::SalesforcePubSubError)?;

                println!("{:?}", resp);
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct PublisherBuilder {
    service: Option<flowgen_core::service::Service>,
    config: Option<Arc<super::config::Target>>,
    rx: Option<Receiver<Event>>,
    current_task_id: usize,
}

impl PublisherBuilder {
    pub fn new() -> PublisherBuilder {
        PublisherBuilder {
            ..Default::default()
        }
    }

    pub fn service(mut self, service: flowgen_core::service::Service) -> Self {
        self.service = Some(service);
        self
    }

    pub fn config(mut self, config: Arc<super::config::Target>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
        self
    }

    pub async fn build(self) -> Result<Publisher, PublisherError> {
        Ok(Publisher {
            service: self
                .service
                .ok_or_else(|| PublisherError::MissingRequiredAttributeError("data".to_string()))?,
            config: self.config.ok_or_else(|| {
                PublisherError::MissingRequiredAttributeError("subject".to_string())
            })?,
            rx: self.rx.ok_or_else(|| {
                PublisherError::MissingRequiredAttributeError("subject".to_string())
            })?,
            current_task_id: self.current_task_id,
        })
    }
}

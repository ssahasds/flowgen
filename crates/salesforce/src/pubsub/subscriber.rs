use flowgen_core::{
    cache::Cache,
    connect::client::Client,
    convert::recordbatch::RecordBatchExt,
    stream::event::{Event, EventBuilder},
};

use salesforce_pubsub::eventbus::v1::{FetchRequest, SchemaRequest, TopicRequest};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::{broadcast::Sender, Mutex};
use tokio_stream::StreamExt;
use tracing::{event, Level};

const DEFAULT_MESSAGE_SUBJECT: &str = "salesforce.pubsub.in";
const DEFAULT_PUBSUB_URL: &str = "https://api.pubsub.salesforce.com";
const DEFAULT_PUBSUB_PORT: &str = "443";
const DEFAULT_NUM_REQUESTED: i32 = 1000;

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    SalesforcePubSub(#[from] super::context::Error),
    #[error(transparent)]
    SalesforceAuth(#[from] crate::client::Error),
    #[error(transparent)]
    Event(#[from] flowgen_core::stream::event::Error),
    #[error(transparent)]
    TaskJoin(#[from] tokio::task::JoinError),
    #[error(transparent)]
    SerdeAvroSchema(#[from] serde_avro_fast::schema::SchemaError),
    #[error(transparent)]
    SerdeAvroValue(#[from] serde_avro_fast::de::DeError),
    #[error(transparent)]
    SendMessage(#[from] tokio::sync::broadcast::error::SendError<Event>),
    #[error(transparent)]
    Bincode(#[from] bincode::Error),
    #[error(transparent)]
    RecordBatch(#[from] flowgen_core::convert::recordbatch::Error),
    #[error(transparent)]
    Service(#[from] flowgen_core::connect::service::Error),
    #[error("missing required attribute")]
    MissingRequiredAttribute(String),
    #[error("cache errors")]
    Cache(),
}

/// Handles the processing logic for a single incoming `Event`.
///
/// An `EventHandler` instance is created for each event that is received.
struct TopicListener<T: Cache> {
    /// The cache store used to put / retrieve schema etc.
    cache: Arc<T>,
    pubsub: Arc<Mutex<super::context::Context>>,
    topic: String,
    tx: Sender<Event>,
    /// Thread-safe reference to the subscriber configuration.
    config: Arc<super::config::Subscriber>,
    current_task_id: usize,
}

impl<T: Cache> TopicListener<T> {
    async fn run(self) -> Result<(), Error> {
        let topic_info = self
            .pubsub
            .lock()
            .await
            .get_topic(TopicRequest {
                topic_name: self.topic.clone(),
            })
            .await
            .map_err(Error::SalesforcePubSub)?
            .into_inner();

        let schema_info = self
            .pubsub
            .lock()
            .await
            .get_schema(SchemaRequest {
                schema_id: topic_info.schema_id,
            })
            .await
            .map_err(Error::SalesforcePubSub)?
            .into_inner();

        let num_requested = match self.config.num_requested {
            Some(num_requested) => num_requested,
            None => DEFAULT_NUM_REQUESTED,
        };

        let topic_name = topic_info.topic_name.as_str();
        let mut fetch_request = FetchRequest {
            topic_name: topic_name.to_string(),
            num_requested,
            ..Default::default()
        };

        if let Some(durable_consumer_opts) = self
            .config
            .durable_consumer_options
            .as_ref()
            .filter(|opts| opts.enabled && !opts.managed_subscription)
        {
            match self.cache.get(&durable_consumer_opts.name).await {
                Ok(reply_id) => {
                    fetch_request.replay_id = reply_id.into();
                    fetch_request.replay_preset = 2
                }
                Err(err) => {
                    println!("{:?}", err);
                }
            }
        }

        let mut stream = self
            .pubsub
            .lock()
            .await
            .subscribe(fetch_request)
            .await
            .map_err(Error::SalesforcePubSub)?
            .into_inner();

        while let Some(event) = stream.next().await {
            match event {
                Ok(fr) => {
                    for ce in fr.events {
                        // Cache replay_id as durable consumer is set to be local.
                        if let Some(durable_consumer_opts) = self
                            .config
                            .durable_consumer_options
                            .as_ref()
                            .filter(|opts| opts.enabled && !opts.managed_subscription)
                        {
                            self.cache
                                .put(&durable_consumer_opts.name, ce.replay_id.into())
                                .await
                                .map_err(|_| Error::Cache())?;
                        }

                        if let Some(event) = ce.event {
                            let schema: serde_avro_fast::Schema =
                                schema_info
                                    .schema_json
                                    .parse()
                                    .map_err(Error::SerdeAvroSchema)?;

                            let value = serde_avro_fast::from_datum_slice::<Value>(
                                &event.payload[..],
                                &schema,
                            )
                            .map_err(Error::SerdeAvroValue)?;

                            let recordbatch = value
                                .to_string()
                                .to_recordbatch()
                                .map_err(Error::RecordBatch)?;

                            let topic = topic_name.replace('/', ".").to_lowercase();

                            let subject =
                                format!("{}.{}.{}", DEFAULT_MESSAGE_SUBJECT, &topic[1..], event.id);

                            let e = EventBuilder::new()
                                .data(recordbatch)
                                .subject(subject)
                                .current_task_id(self.current_task_id)
                                .build()
                                .map_err(Error::Event)?;

                            event!(Level::INFO, "event processed: {}", e.subject);
                            self.tx.send(e).map_err(Error::SendMessage)?;
                        }
                    }
                }
                Err(e) => {
                    return Err(Error::SalesforcePubSub(super::context::Error::Tonic(e)));
                }
            }
        }

        Ok(())
    }
}

pub struct Subscriber<T: Cache> {
    config: Arc<super::config::Subscriber>,
    tx: Sender<Event>,
    current_task_id: usize,
    /// The cache store used to put / retrieve schema etc.
    cache: Arc<T>,
}

impl<T: Cache> flowgen_core::task::runner::Runner for Subscriber<T> {
    type Error = Error;
    async fn run(self) -> Result<(), Error> {
        let endpoint = match &self.config.endpoint {
            Some(endpoint) => endpoint,
            None => &format!("{0}:{1}", DEFAULT_PUBSUB_URL, DEFAULT_PUBSUB_PORT),
        };

        let service = flowgen_core::connect::service::ServiceBuilder::new()
            .endpoint(endpoint.to_owned())
            .build()
            .map_err(Error::Service)?
            .connect()
            .await
            .map_err(Error::Service)?;

        let sfdc_client = crate::client::Builder::new()
            .credentials_path(self.config.credentials.clone().into())
            .build()
            .map_err(Error::SalesforceAuth)?
            .connect()
            .await
            .map_err(Error::SalesforceAuth)?;

        let pubsub = super::context::Builder::new(service)
            .with_client(sfdc_client)
            .build()
            .map_err(Error::SalesforcePubSub)?;
        let pubsub = Arc::new(Mutex::new(pubsub));

        for topic in self.config.topic_list.clone().into_iter() {
            let pubsub: Arc<Mutex<super::context::Context>> = Arc::clone(&pubsub);
            let tx = self.tx.clone();
            let config = Arc::clone(&self.config);
            let cache = Arc::clone(&self.cache);
            let current_task_id = self.current_task_id;
            let topic_listener = TopicListener {
                cache,
                config,
                current_task_id,
                tx,
                topic,
                pubsub,
            };

            // Spawn a new asynchronous task to handle the topic listener for each topic.
            tokio::spawn(async move {
                if let Err(err) = topic_listener.run().await {
                    event!(Level::ERROR, "{}", err);
                }
            });
        }

        Ok(())
    }
}

#[derive(Default)]
pub struct SubscriberBuilder<T: Cache> {
    config: Option<Arc<super::config::Subscriber>>,
    tx: Option<Sender<Event>>,
    current_task_id: usize,
    cache: Option<Arc<T>>,
}

impl<T: Cache> SubscriberBuilder<T>
where
    T: Default,
{
    pub fn new() -> SubscriberBuilder<T> {
        SubscriberBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Subscriber>) -> Self {
        self.config = Some(config);
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

    pub fn cache(mut self, cache: Arc<T>) -> Self {
        self.cache = Some(cache);
        self
    }

    pub async fn build(self) -> Result<Subscriber<T>, Error> {
        Ok(Subscriber {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingRequiredAttribute("sender".to_string()))?,
            cache: self
                .cache
                .ok_or_else(|| Error::MissingRequiredAttribute("cache".to_string()))?,
            current_task_id: self.current_task_id,
        })
    }
}

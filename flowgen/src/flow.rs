use super::config;
use flowgen_core::client::Client;
use std::path::PathBuf;
use tracing::error;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Cannot open/read the credentials file at path {1}")]
    OpenFile(#[source] std::io::Error, PathBuf),
    #[error("Cannot parse config file")]
    ParseConfig(#[source] toml::de::Error),
    #[error("Cannot setup Flowgen Client")]
    FlowgenService(#[source] flowgen_core::service::Error),
    // #[error("Cannot auth to Salesforce using provided credentials")]
    // FlowgenSalesforceAuth(#[source] flowgen_salesforce::client::Error),
    // #[error("Cannot setup Salesforce PubSub context")]
    // FlowgenSalesforcePubSub(#[source] flowgen_salesforce::pubsub::context::Error),
    // #[error("Cannot establish connection with NATS server")]
    // NatsConnect(#[source] async_nats::ConnectError),
    // #[error("Cannot create stream with provided config")]
    // NatsCreateStream(#[source] async_nats::jetstream::context::CreateStreamError),
    // #[error("Cannot create key value store with provided config")]
    // NatsCreateKeyValue(#[source] async_nats::jetstream::context::CreateKeyValueError),
    // #[error("There was an error with putting kv into the store")]
    // NatsPutKeyValue(#[source] async_nats::jetstream::kv::PutError),
    // #[error("There was an error with async_nats publish")]
    // NatsPublish(#[source] async_nats::jetstream::context::PublishError),
    // #[error("Cannot execute async task")]
    // TokioJoin(#[source] tokio::task::JoinError),
    // #[error("There was an error with subscriber")]
    // Subscriber(#[source] subscriber::Error),
    // #[error("There was an error with bincode serialization / deserialization")]
    // Bincode(#[source] Box<bincode::ErrorKind>),
}

#[allow(non_camel_case_types)]
pub enum Source {
    salesforce_pubsub(flowgen_salesforce::pubsub::subscriber::Subscriber),
}

pub enum Target {
    nats_jetstream(flowgen_nats::jetstream::publisher::Publisher),
}

pub struct Flow {
    config: config::Config,
    pub source: Option<Source>,
    pub target: Option<Target>,
}

impl Flow {
    pub async fn init(mut self) -> Result<Self, Error> {
        // Setup Flowgen service.
        let service = flowgen_core::service::Builder::new()
            .with_endpoint(format!(
                "{0}:443",
                flowgen_salesforce::pubsub::eventbus::ENDPOINT
            ))
            .build()
            .map_err(Error::FlowgenService)?
            .connect()
            .await
            .map_err(Error::FlowgenService)?;

        // Get cloned version of the config.
        let config = self.config.clone();

        // Setup source subscribers.
        match config.flow.source {
            config::Source::salesforce_pubsub(config) => {
                let subscriber =
                    flowgen_salesforce::pubsub::subscriber::Builder::new(service.clone(), config)
                        .build()
                        .await
                        .unwrap();
                self.source = Some(Source::salesforce_pubsub(subscriber));
            }
        }

        // Setup target publishers.
        match config.flow.target {
            config::Target::nats_jetstream(config) => {
                let publisher = flowgen_nats::jetstream::publisher::Builder::new(service, config)
                    .build()
                    .await
                    .unwrap();
                self.target = Some(Target::nats_jetstream(publisher));
            }
        }
        Ok(self)
    }
}

#[derive(Default)]
pub struct Builder {
    config_path: PathBuf,
}

impl Builder {
    pub fn new(config_path: PathBuf) -> Builder {
        Builder { config_path }
    }
    pub fn build(&mut self) -> Result<Flow, Error> {
        let c = std::fs::read_to_string(&self.config_path)
            .map_err(|e| Error::OpenFile(e, self.config_path.clone()))?;
        let config: config::Config = toml::from_str(&c).map_err(Error::ParseConfig)?;
        let f = Flow {
            config,
            source: None,
            target: None,
        };
        Ok(f)
    }
}

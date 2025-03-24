use arrow::csv::reader::Format;
use arrow::csv::ReaderBuilder;
use async_nats::jetstream::{object_store::Config, object_store::GetErrorKind};
use flowgen_core::{connect::client::Client, stream::event::Event};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::sync::broadcast::Sender;
use tokio_stream::StreamExt;

const DEFAULT_MESSAGE_SUBJECT: &str = "nats.object.store.in";
const DEFAULT_BATCH_SIZE: usize = 1000;
const DEFAULT_HAS_HEADER: bool = true;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error authorizating to NATS client")]
    NatsClient(#[source] crate::client::Error),
    #[error("error with NATS JetStream Message")]
    NatsJetStreamMessage(#[source] crate::jetstream::message::Error),
    #[error("error with NATS JetStream durable consumer")]
    NatsJetStreamConsumer(#[source] async_nats::jetstream::stream::ConsumerError),
    #[error("error with NATS JetStream")]
    NatsJetStream(#[source] async_nats::jetstream::consumer::StreamError),
    #[error("error getting NATS JetStream")]
    NatsJetStreamGetStream(#[source] async_nats::jetstream::context::GetStreamError),
    #[error("error subscriging to NATS subject")]
    NatsSubscribe(#[source] async_nats::SubscribeError),
    #[error("error executing async task")]
    TaskJoin(#[source] tokio::task::JoinError),
    #[error("error with sending message over channel")]
    SendMessage(#[source] tokio::sync::broadcast::error::SendError<Event>),
    #[error("missing required attribute")]
    MissingRequiredAttribute(String),
    #[error("other error with subscriber")]
    Other(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("failed to get nats bucket")]
    NatsObjectStoreBucketError(#[source] async_nats::jetstream::context::CreateKeyValueError),
    #[error("failed to get nats bucket")]
    NatsObjectStoreFileError(#[source] async_nats::error::Error<GetErrorKind>),
    #[error("failed to open file")]
    FileOpenError(#[source] std::io::Error),
    #[error("failed to read file")]
    CSVFileReadError(#[source] std::string::FromUtf8Error),
    #[error("failed to loop file")]
    CSVLoopError(#[source] csv::Error),
    #[error("error deserializing data into binary format")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("error reading file")]
    IO(#[source] std::io::Error),
    #[error("failed to get key value from nats storage")]
    NatsObjectStoreWatchError(#[source] async_nats::jetstream::object_store::WatchError),
    #[error("error constructing Flowgen Event")]
    Event(#[source] flowgen_core::stream::event::Error),
}

pub struct Subscriber {
    config: Arc<super::config::Source>,
    tx: Sender<Event>,
    current_task_id: usize,
}

impl Subscriber {
    pub async fn subscribe(self) -> Result<(), Error> {
        let client = crate::client::ClientBuilder::new()
            .credentials_path(self.config.credentials.clone().into())
            .build()
            .map_err(Error::NatsClient)?
            .connect()
            .await
            .map_err(Error::NatsClient)?;

        if let Some(jetstream) = client.jetstream {
            let bucket_name = self.config.bucket.clone();
            let bucket = jetstream
                .create_object_store(Config {
                    bucket: bucket_name.to_string(),
                    ..Default::default()
                })
                .await
                .map_err(Error::NatsObjectStoreBucketError)?;
            let mut objects_stream = bucket
                .list()
                .await
                .map_err(Error::NatsObjectStoreWatchError)?;

            while let Some(Ok(object)) = objects_stream.next().await {
                let file_name = object.name;

                // Fetch file from the bucket
                let mut nats_obj_file = bucket
                    .get(file_name.clone())
                    .await
                    .map_err(Error::NatsObjectStoreFileError)?;

                let mut buffer = vec![];
                nats_obj_file
                    .read_to_end(&mut buffer)
                    .await
                    .map_err(Error::IO)?;

                let (schema, _) = Format::default()
                    .with_header(true)
                    .infer_schema(&mut buffer.as_slice(), Some(100))
                    .map_err(Error::Arrow)?;
                // buffer.rewind().map_err(Error::IO)?;

                let csv = ReaderBuilder::new(Arc::new(schema.clone()))
                    .with_header(DEFAULT_HAS_HEADER)
                    .with_batch_size(DEFAULT_BATCH_SIZE)
                    .build(buffer.as_slice())
                    .map_err(Error::Arrow)?;

                for batch in csv {
                    println!("{:?}", batch);
                }
                // // Convert buffer to string
                // let csv_content = String::from_utf8(buffer).map_err(Error::CSVFileReadError)?;
                // //print!("csv_content:: {:?}",csv_content);
                // let mut rdr = ReaderBuilder::new().from_reader(csv_content.as_bytes());
                // let header = rdr.byte_headers();
                // println!("header:: {:?}", header);
                // for result in rdr.records() {
                //     let record = result.map_err(Error::CSVLoopError)?;
                //     println!("record:: {:?}", record);
                // }
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct SubscriberBuilder {
    config: Option<Arc<super::config::Source>>,
    tx: Option<Sender<Event>>,
    current_task_id: usize,
}

impl SubscriberBuilder {
    pub fn new() -> SubscriberBuilder {
        SubscriberBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Source>) -> Self {
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

    pub async fn build(self) -> Result<Subscriber, Error> {
        Ok(Subscriber {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingRequiredAttribute("sender".to_string()))?,
            current_task_id: self.current_task_id,
        })
    }
}

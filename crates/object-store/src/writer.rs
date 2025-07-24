use apache_avro::from_avro_datum;
use bytes::Bytes;
use chrono::{DateTime, Datelike, Utc};
use flowgen_core::connect::client::Client;
use flowgen_core::stream::event::Event;
use object_store::PutPayload;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::{broadcast::Receiver, Mutex};
use tracing::{event, Level};

/// Default subject prefix for logging messages.
const DEFAULT_MESSAGE_SUBJECT: &str = "object_store.writer";
/// File extension for Avro format files.
const DEFAULT_AVRO_EXTENSION: &str = "avro";
/// File extension for CSV format files.
const DEFAULT_CSV_EXTENSION: &str = "csv";

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Avro(#[from] apache_avro::Error),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    ObjectStoreClient(#[from] super::client::Error),
    #[error("missing required attribute")]
    MissingRequiredAttribute(String),
    #[error("could not initialize object store context")]
    NoObjectStoreContext(),
}

/// Handles processing of individual events by writing them to object storage.
struct EventHandler {
    /// Writer configuration settings.
    config: Arc<super::config::Writer>,
    /// Object store client for writing data.
    client: Arc<Mutex<super::client::Client>>,
}

impl EventHandler {
    /// Processes an event and writes it to the configured object store.
    async fn handle(self, event: Event) -> Result<(), Error> {
        let mut client_guard = self.client.lock().await;
        let context = client_guard
            .context
            .as_mut()
            .ok_or_else(Error::NoObjectStoreContext)?;

        let mut path = PathBuf::from(context.path.to_string());

        let cd = Utc::now();
        if let Some(hive_options) = &self.config.hive_partition_options {
            if hive_options.enabled {
                for partition_key in &hive_options.partition_keys {
                    match partition_key {
                        crate::config::HiveParitionKeys::EventDate => {
                            let date_partition = self.format_date_partition(&cd);
                            path.push(date_partition);
                        }
                    }
                }
            }
        }
        let timestamp = cd.timestamp_micros();
        let filename = match event.id {
            Some(id) => id,
            _none => timestamp.to_string(),
        };
        path.push(&filename);

        let mut buffer = Vec::new();
        let object_path = match &event.data {
            flowgen_core::stream::event::EventData::ArrowRecordBatch(data) => {
                // Convert Arrow RecordBatch to CSV format.
                arrow::csv::WriterBuilder::new()
                    .with_header(true)
                    .build(&mut buffer)
                    .write(data)
                    .map_err(Error::Arrow)?;
                object_store::path::Path::from(format!(
                    "{}.{}",
                    path.to_string_lossy(),
                    DEFAULT_CSV_EXTENSION
                ))
            }
            flowgen_core::stream::event::EventData::Avro(data) => {
                // Parse Avro schema and deserialize raw bytes.
                let schema = apache_avro::Schema::parse_str(&data.schema).map_err(Error::Avro)?;
                let value = from_avro_datum(&schema, &mut &data.raw_bytes[..], None)
                    .map_err(Error::Avro)?;

                let mut writer = apache_avro::Writer::new(&schema, &mut buffer);
                writer.append(value).map_err(Error::Avro)?;
                writer.flush().map_err(Error::Avro)?;
                object_store::path::Path::from(format!(
                    "{}.{}",
                    path.to_string_lossy(),
                    DEFAULT_AVRO_EXTENSION
                ))
            }
        };

        // Upload processed data to object store
        let payload = PutPayload::from_bytes(Bytes::from(buffer));
        context
            .object_store
            .put(&object_path, payload)
            .await
            .map_err(Error::ObjectStore)?;

        let subject = format!("{DEFAULT_MESSAGE_SUBJECT}.{filename}");
        event!(Level::INFO, "event processed: {}", subject);
        Ok(())
    }

    /// Formats date into Hive partition format (year=YYYY/month=MM/day=DD).
    fn format_date_partition(&self, date: &DateTime<Utc>) -> String {
        format!(
            "year={}/month={}/day={}",
            date.year(),
            date.month(),
            date.day()
        )
    }
}

/// Object store writer that processes events from a broadcast receiver.
pub struct Writer {
    /// Writer configuration settings.
    config: Arc<super::config::Writer>,
    /// Broadcast receiver for incoming events.
    rx: Receiver<Event>,
    /// Current task identifier for event filtering.
    current_task_id: usize,
}

impl flowgen_core::task::runner::Runner for Writer {
    type Error = Error;

    async fn run(mut self) -> Result<(), Self::Error> {
        // Build object store client with conditional configuration
        let mut client_builder = super::client::ClientBuilder::new().path(self.config.path.clone());

        if let Some(options) = &self.config.client_options {
            client_builder = client_builder.options(options.clone());
        }
        if let Some(credentials) = &self.config.credentials {
            client_builder = client_builder.credentials(credentials.to_path_buf());
        }

        let client = Arc::new(Mutex::new(
            client_builder
                .build()
                .map_err(Error::ObjectStoreClient)?
                .connect()
                .await
                .map_err(Error::ObjectStoreClient)?,
        ));

        // Process incoming events, filtering by task ID.
        while let Ok(event) = self.rx.recv().await {
            if event.current_task_id == Some(self.current_task_id - 1) {
                let client = Arc::clone(&client);
                let config = Arc::clone(&self.config);
                let event_handler = EventHandler { client, config };
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

/// Builder pattern for constructing Writer instances.
#[derive(Default)]
pub struct WriterBuilder {
    /// Writer configuration settings.
    config: Option<Arc<super::config::Writer>>,
    /// Broadcast receiver for incoming events.
    rx: Option<Receiver<Event>>,
    /// Current task identifier for event filtering.
    current_task_id: usize,
}

impl WriterBuilder {
    pub fn new() -> WriterBuilder {
        WriterBuilder {
            ..Default::default()
        }
    }

    /// Sets the writer configuration.
    pub fn config(mut self, config: Arc<super::config::Writer>) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the event receiver.
    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    /// Sets the current task identifier.
    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
        self
    }

    /// Builds the Writer instance, validating required fields.
    pub async fn build(self) -> Result<Writer, Error> {
        Ok(Writer {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            current_task_id: self.current_task_id,
        })
    }
}

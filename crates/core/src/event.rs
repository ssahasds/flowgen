use crate::convert::serde::SerdeValueExt;
use apache_avro::from_avro_datum;
use chrono::Utc;
use serde::{Serialize, Serializer};

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("missing required attribute")]
    MissingRequiredAttribute(String),
}

#[derive(Debug, Clone)]
pub struct Event {
    pub data: EventData,
    pub extensions: Option<arrow::array::RecordBatch>,
    pub subject: String,
    pub current_task_id: Option<usize>,
    pub id: Option<String>,
    pub timestamp: i64,
}

#[derive(Debug, Clone)]
pub enum EventData {
    ArrowRecordBatch(arrow::array::RecordBatch),
    Avro(AvroData),
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AvroData {
    pub schema: String,
    pub raw_bytes: Vec<u8>,
}

impl Serialize for EventData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            EventData::ArrowRecordBatch(data) => {
                let json_value: serde_json::Value =
                    data.try_from().map_err(serde::ser::Error::custom)?;
                json_value.serialize(serializer)
            }
            EventData::Avro(data) => {
                let schema = apache_avro::Schema::parse_str(&data.schema)
                    .map_err(serde::ser::Error::custom)?;
                let avro_value = from_avro_datum(&schema, &mut &data.raw_bytes[..], None)
                    .map_err(serde::ser::Error::custom)?;
                let json_value =
                    serde_json::Value::try_from(avro_value).map_err(serde::ser::Error::custom)?;
                json_value.serialize(serializer)
            }
        }
    }
}

#[derive(Default)]
pub struct EventBuilder {
    pub data: Option<EventData>,
    pub extensions: Option<arrow::array::RecordBatch>,
    pub subject: Option<String>,
    pub current_task_id: Option<usize>,
    pub id: Option<String>,
    pub timestamp: i64,
}

impl EventBuilder {
    pub fn new() -> Self {
        EventBuilder {
            timestamp: Utc::now().timestamp_micros(),
            ..Default::default()
        }
    }
    pub fn data(mut self, data: EventData) -> Self {
        self.data = Some(data);
        self
    }
    pub fn subject(mut self, subject: String) -> Self {
        self.subject = Some(subject);
        self
    }
    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = Some(current_task_id);
        self
    }
    pub fn id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }
    pub fn time(mut self, timestamp: i64) -> Self {
        self.timestamp = timestamp;
        self
    }
    pub fn extensions(mut self, extensions: arrow::array::RecordBatch) -> Self {
        self.extensions = Some(extensions);
        self
    }

    pub fn build(self) -> Result<Event, Error> {
        Ok(Event {
            data: self
                .data
                .ok_or_else(|| Error::MissingRequiredAttribute("data".to_string()))?,
            extensions: self.extensions,
            subject: self
                .subject
                .ok_or_else(|| Error::MissingRequiredAttribute("subject".to_string()))?,
            id: self.id,
            timestamp: self.timestamp,
            current_task_id: self.current_task_id,
        })
    }
}

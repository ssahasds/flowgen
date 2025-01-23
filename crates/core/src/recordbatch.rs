use std::{io::BufReader, sync::Arc};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error with an Apache Arrow data.")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("Provided value is not an object.")]
    NotObject(),
    #[error("Missing required attributes.")]
    MissingRequiredAttribute(String),
}
pub trait RecordBatchExt {
    type Error;
    fn to_recordbatch(&self) -> Result<arrow::array::RecordBatch, Self::Error>;
}

impl RecordBatchExt for String {
    type Error = Error;
    fn to_recordbatch(&self) -> Result<arrow::array::RecordBatch, Self::Error> {
        let reader = BufReader::new(self.as_bytes());
        let (schema, _) = arrow_json::reader::infer_json_schema(reader, None).unwrap();

        let reader = BufReader::new(self.as_bytes());
        let record_batch = arrow_json::ReaderBuilder::new(Arc::new(schema))
            .build(reader)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();

        Ok(record_batch)
    }
}

impl RecordBatchExt for serde_json::Value {
    type Error = Error;
    fn to_recordbatch(&self) -> Result<arrow::array::RecordBatch, Self::Error> {
        let record_batch = self.to_string().to_recordbatch().unwrap();
        Ok(record_batch)
    }
}

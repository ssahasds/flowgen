//! Utilities for adjusting Arrow data precision within events.

use deltalake::arrow::{
    array::{Array, RecordBatch, TimestampMicrosecondArray, TimestampMillisecondArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
};
use std::sync::Arc;

/// Errors that can occur during event data processing and precision adjustment.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Wraps an error from the Apache Arrow library.
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    /// Indicates a failure to downcast an Arrow array to its expected concrete type.
    #[error("failed ot downcast type")]
    // Note: "ot" likely a typo for "to" in your original error message
    DowcastFailed(),
}

/// Trait for extending `flowgen_core::stream::event::Event` with data manipulation capabilities.
pub trait EventExt {
    /// The error type for operations defined in this trait.
    type Error;

    /// Adjusts the precision of timestamp data within the event.
    ///
    /// Typically converts millisecond precision timestamps to microsecond precision.
    fn adjust_data_precision(&mut self) -> Result<(), Self::Error>;
}

/// Implements `EventExt` for `flowgen_core::stream::event::Event`.
impl EventExt for flowgen_core::stream::event::Event {
    type Error = Error;

    /// Modifies the event's internal `RecordBatch` to adjust timestamp precision.
    ///
    /// This method iterates through columns:
    /// - If a column is `Timestamp(Millisecond, tz)`, it's converted to `Timestamp(Microsecond, tz)`.
    ///   Both the schema field and the array data (scaled by 1000) are updated.
    /// - Other data types are preserved.
    ///   The event's `data` field is updated in place with the new `RecordBatch`.
    ///
    /// # Errors
    /// Returns `Error::DowcastFailed` if a column typed as `TimestampMillisecond` in the schema
    /// cannot be downcast to `TimestampMillisecondArray`.
    /// Returns `Error::Arrow` if creating the new `RecordBatch` fails.
    fn adjust_data_precision(&mut self) -> Result<(), Self::Error> {
        let columns = self.data.columns();
        let schema = self.data.schema();

        // Prepare to build the new schema and columns.
        let mut new_fields: Vec<Arc<Field>> = Vec::with_capacity(schema.fields().len());
        let mut new_columns = Vec::with_capacity(columns.len());

        for (i, field) in schema.fields().iter().enumerate() {
            match field.data_type() {
                DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                    // Update schema field to microsecond precision.
                    new_fields.push(Arc::new(Field::new(
                        field.name(),
                        DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                        field.is_nullable(),
                    )));

                    // Convert array data from milliseconds to microseconds.
                    let old_array = &columns[i];
                    // Expect TimestampMillisecondArray based on schema; fail if downcast doesn't match.
                    let millis_array = old_array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .ok_or(Error::DowcastFailed())?;

                    let micros_data: Vec<Option<i64>> = millis_array
                        .iter()
                        .map(|val| val.map(|ms| ms * 1000)) // Scale ms to µs.
                        .collect();

                    new_columns
                        .push(Arc::new(TimestampMicrosecondArray::from(micros_data))
                            as Arc<dyn Array>);
                }
                _ => {
                    new_fields.push(field.clone());
                    new_columns.push(columns[i].clone());
                }
            }
        }

        // Reconstruct the RecordBatch with the (potentially) updated schema and columns.
        let new_schema = Arc::new(Schema::new(new_fields));
        let new_batch = RecordBatch::try_new(new_schema, new_columns).map_err(Error::Arrow)?;

        // Update the event's internal data in place.
        self.data = new_batch;
        Ok(())
    }
}

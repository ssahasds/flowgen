//! Utilities for adjusting Apache Arrow Schema timestamp precision.

use deltalake::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
// NOTE: `std::sync::Arc` is imported in your previous snippets but not used here.
// If `Field` objects were being wrapped in `Arc` for `Schema::new`, it would be used.
// Commenting based on the provided code where `adjusted_fields` is `Vec<Field>`.

/// Errors encountered during Schema precision adjustment.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Wraps an Apache Arrow error.
    #[error("error with an Apache Arrow data")]
    Arrow(#[source] arrow::error::ArrowError),
}

/// Extends `Schema` functionality, particularly for data type adjustments.
pub trait SchemaExt {
    /// The error type for operations in this trait.
    type Error;

    /// Consumes the schema, adjusts timestamp precision of its fields, and returns a new schema.
    ///
    /// Typically converts `Timestamp(Millisecond, _)` fields to `Timestamp(Microsecond, _)`.
    fn adjust_data_precision(self) -> Result<Schema, Self::Error>;
}

/// Implements `SchemaExt` for `arrow::datatypes::Schema`.
impl SchemaExt for Schema {
    type Error = Error;

    /// Consumes the original `Schema` and returns a new one with adjusted timestamp precision.
    ///
    /// This method iterates through all fields of the consumed schema:
    /// - If a field is `Timestamp(Millisecond, ...)`, its data type is changed to `Timestamp(Microsecond, ...)`.
    /// - Other field data types are preserved.
    /// - Field names, nullability, and metadata are retained for all fields.
    /// - The original schema's top-level metadata is also preserved in the new schema.
    ///
    /// # Errors
    /// Adheres to the trait's error signature. As implemented, direct operations
    /// (field and schema construction from existing valid parts) are unlikely to
    /// produce an error. The `Result` type is maintained for trait compatibility and
    /// potential future extensions that might introduce error conditions.
    fn adjust_data_precision(self) -> Result<Schema, Self::Error> {
        // Transform fields: adjust timestamp precision, preserve others.
        let adjusted_fields: Vec<Field> = self
            .fields()
            .iter()
            .map(|field| {
                // `field` here is &Arc<Field> or &Field depending on Arrow version's iter details
                let new_data_type = match field.data_type() {
                    DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                        DataType::Timestamp(TimeUnit::Microsecond, tz.clone())
                    }
                    other => other.clone(),
                };

                Field::new(field.name(), new_data_type, field.is_nullable())
                    .with_metadata(field.metadata().clone())
            })
            .collect();

        // Construct the new schema with adjusted fields and original top-level metadata.
        let new_schema = Schema::new(adjusted_fields).with_metadata(self.metadata().clone());
        Ok(new_schema)
    }
}

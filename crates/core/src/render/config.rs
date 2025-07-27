use crate::{
    convert::serde::{SerdeValueExt, StringExt},
    stream::event::EventData,
};

use super::super::convert::serde::MapExt;
use apache_avro::from_avro_datum;
use handlebars::Handlebars;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Render(#[from] handlebars::RenderError),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
}

pub trait ConfigExt {
    fn render(&self, data: &EventData) -> Result<Self, Error>
    where
        Self: Serialize + DeserializeOwned + Sized,
    {
        let template = serde_json::to_string(self)?;
        let data = match data {
            EventData::ArrowRecordBatch(data) => data.try_from().unwrap(),
            EventData::Avro(data) => {
                let schema = apache_avro::Schema::parse_str(&data.schema).unwrap();
                let avro_value = from_avro_datum(&schema, &mut &data.raw_bytes[..], None).unwrap();
                serde_json::Value::try_from(avro_value).unwrap()
            }
        };

        let handlebars = Handlebars::new();
        let rendered = handlebars.render_template(&template, &data)?;

        // Deserialize the rendered string back into Self
        let result: Self = serde_json::from_str(&rendered)?;
        Ok(result)
    }
}

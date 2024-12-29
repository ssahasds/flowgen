use futures::future::{join_all, JoinAll};
use glob::glob;
use std::env;
use std::process;
use tokio::task::JoinHandle;
use tracing::error;
pub const DEFAULT_TOPIC_NAME: &str = "/data/ChangeEvents";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error deserializing data into binary format.")]
    Bincode(#[source] bincode::Error),
    #[error("Failed to publish message in Nats Jetstream.")]
    NatsPublish(#[source] async_nats::jetstream::context::PublishError),
    #[error("Cannot execute async task.")]
    TokioJoin(#[source] tokio::task::JoinError),
    #[error("Failed to setup Salesforce PubSub as flow source.")]
    FlowgenSalesforcePubSubSubscriberError(#[source] flowgen_salesforce::pubsub::subscriber::Error),
    #[error("Failed to setup Salesforce PubSub as flow source.")]
    FlowgenFileSubscriberError(#[source] flowgen_file::subscriber::Error),
}

#[tokio::main]
async fn main() {
    // Install global log collector.
    tracing_subscriber::fmt::init();

    // Setup environment variables
    let config_dir = env::var("CONFIG_DIR").expect("env variable CONFIG_DIR should be set");

    // Run Flowgen service for each of individual configs.
    let mut all_handle_list = Vec::new();
    for config in glob(&config_dir).unwrap_or_else(|err| {
        error!("{:?}", err);
        process::exit(1);
    }) {
        let config_path = config.unwrap_or_else(|err| {
            error!("{:?}", err);
            process::exit(1);
        });

        let f = flowgen::flow::Builder::new(config_path)
            .build()
            .unwrap_or_else(|err| {
                error!("{:?}", err);
                process::exit(1);
            })
            .run()
            .await
            .unwrap_or_else(|err| {
                error!("{:?}", err);
                process::exit(1);
            });

        if let Some(handle_list) = f.handle_list {
            for handle in handle_list {
                all_handle_list.push(handle);
            }
        }
    }
    let result = futures::future::join_all(all_handle_list).await;
    for r in result {
        r.unwrap();
    }
}

// // Run an event relay service.
// async fn run(f: flow::Flow) -> Result<(), Error> {
//     if let Some(source) = f.source {
//         match source {
//             flow::Source::salesforce_pubsub(source) => {
//                 let mut rx = source.rx;
//                 let mut topic_info_list: Vec<TopicInfo> = Vec::new();
//                 let receiver_task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
//                     while let Some(cm) = rx.recv().await {
//                         match cm {
//                         flowgen_salesforce::pubsub::subscriber::ChannelMessage::FetchResponse(
//                             fr,
//                         ) => {
//                             event!(name: "fetch_response", Level::INFO, rpc_id = fr.rpc_id);
//                             for ce in fr.events {
//                                 if let Some(pe) = ce.event {

//                                     // Relay events only if event_id is present.
//                                     if !pe.id.is_empty() {
//                                     event!(name: "event_consumed", Level::INFO, event_id = pe.id);
//                                     // Get the relevant topic from the list.
//                                     let mut topic_list: Vec<TopicInfo> = topic_info_list
//                                         .iter()
//                                         .filter(|t| t.schema_id == pe.schema_id)
//                                         .cloned()
//                                         .collect();

//                                     // Use a default all change events topic if no schema_id is to be found.
//                                     if topic_list.is_empty() {
//                                          topic_list = topic_info_list
//                                         .iter()
//                                         .filter(|t| t.topic_name == DEFAULT_TOPIC_NAME)
//                                         .cloned()
//                                         .collect();
//                                     }

//                                     // Break if default topic was not setup.
//                                     if topic_list.is_empty() {
//                                         break;
//                                     }

//                                     // Setup nats subject and payload.
//                                     let s = topic_list[0].topic_name.replace('/', ".").to_lowercase();
//                                     let event_name = &s[1..];
//                                     let subject = format!("salesforce.pubsub.in.{en}.{eid}", en = event_name, eid = pe.id);
//                                     let event: Vec<u8> = bincode::serialize(&pe).map_err(Error::Bincode)?;

//                                     // Publish an event.
//                                     if let Some(target) = f.target.as_ref()
//                                     {
//                                         match target {
//                                                 flow::Target::nats_jetstream(context) => {
//                                                     context.jetstream.send_publish(
//                                                 subject.to_string(),
//                                                 Publish::build().payload(event.into())).await.map_err(Error::NatsPublish)?;
//                                                     }
//                                                 }
//                                     }
//                                  }
//                                 }
//                             }
//                         }
//                         flowgen_salesforce::pubsub::subscriber::ChannelMessage::TopicInfo(t) => {
//                             event!(name: "topic_info", Level::INFO, rpc_id = t.rpc_id);
//                             topic_info_list.push(t);
//                         }
//                     }
//                     }
//                     Ok(())
//                 });

//                 // Run all subscriber tasks.
//                 source
//                     .async_task_list
//                     .into_iter()
//                     .collect::<TryJoinAll<_>>()
//                     .await
//                     .map_err(Error::TokioJoin)?;

//                 // //Run all receiver tasks.
//                 try_join_all(vec![receiver_task])
//                     .await
//                     .map_err(Error::TokioJoin)?;
//             }
//             flow::Source::file(source) => {
//                 let mut rx = source.rx;
//                 let path = source.path.clone();

//                 let receiver_task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
//                     while let Some(m) = rx.recv().await {
//                         // Setup nats subject.
//                         let filename = match path.split("/").last() {
//                             Some(filename) => filename,
//                             None => break,
//                         };

//                         let timestamp = Utc::now().timestamp_micros();
//                         let subject = format!(
//                             "filedrop.in.{filename}.{timestamp}",
//                             filename = filename,
//                             timestamp = timestamp
//                         );

//                         // Convert message to bytes.
//                         let event = m.to_bytes().map_err(Error::FlowgenFileSubscriberError)?;

//                         // Publish an event.
//                         if let Some(target) = f.target.as_ref() {
//                             match target {
//                                 flow::Target::nats_jetstream(context) => {
//                                     context
//                                         .jetstream
//                                         .send_publish(
//                                             subject,
//                                             Publish::build().payload(event.into()),
//                                         )
//                                         .await
//                                         .map_err(Error::NatsPublish)?
//                                         .await
//                                         .map_err(Error::NatsPublish)?;

//                                     rx.close();
//                                 }
//                             }
//                         }
//                         event!(name: "file_processed", Level::INFO, "file processed: {}", format!(
//                             "{filename}.{timestamp}",
//                             filename = filename,
//                             timestamp = timestamp
//                         ));
//                     }
//                     Ok(())
//                 });

//                 // Run all subscriber tasks.
//                 source
//                     .async_task_list
//                     .into_iter()
//                     .collect::<TryJoinAll<_>>()
//                     .await
//                     .map_err(Error::TokioJoin)?;

//                 //Run all receiver tasks.
//                 try_join_all(vec![receiver_task])
//                     .await
//                     .map_err(Error::TokioJoin)?;
//             }
//             flow::Source::nats_jetstream(source) => {
//                 deltalake_gcp::register_handlers(None);
//                 let mut map = HashMap::new();
//                 map.insert(
//                     "google_service_account".to_string(),
//                     "/etc/gcp.json".to_string(),
//                 );
//                 let mut table =
//                     DeltaOps::try_from_uri_with_storage_options("gs://of-data/account", map)
//                         .await
//                         .unwrap()
//                         .create()
//                         .with_columns(vec![StructField::new(
//                             "Id".to_string(),
//                             DataType::Primitive(PrimitiveType::String),
//                             false,
//                         )])
//                         .await
//                         .unwrap();

//                 let writer_properties = WriterProperties::builder()
//                     .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
//                     .build();

//                 let mut writer = RecordBatchWriter::for_table(&table)
//                     .expect("Failed to make RecordBatchWriter")
//                     .with_writer_properties(writer_properties);

//                 let mut rx = source.rx;
//                 let receiver_task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
//                     while let Some(m) = rx.recv().await {
//                         let mut buffer = Buffer::from_vec(m);
//                         let mut decoder = StreamDecoder::new();
//                         if let Some(batch) = decoder.decode(&mut buffer).unwrap() {
//                             let metadata = table.metadata().unwrap();

//                             let arrow_schema = <deltalake::arrow::datatypes::Schema as TryFrom<
//                                 &StructType,
//                             >>::try_from(
//                                 &metadata.schema().unwrap()
//                             )
//                             .unwrap();

//                             let arrow_schema_ref = Arc::new(arrow_schema);

//                             println!("{:?}", batch.schema());

//                             let column = batch.column_by_name("Id").unwrap();
//                             println!("{column:?}");
//                             println!("Column type: {:?}", column.data_type());

//                             let c = column
//                                 .as_any()
//                                 .downcast_ref::<arrow::array::StringArray>()
//                                 .unwrap();

//                             let mut ids = vec![];
//                             for x in c.into_iter().flatten() {
//                                 ids.push(x.to_string());
//                             }
//                             let arrow_array: Vec<Arc<dyn Array>> =
//                                 vec![Arc::new(StringArray::from(ids))];
//                             let b = deltalake::arrow::record_batch::RecordBatch::try_new(
//                                 arrow_schema_ref,
//                                 arrow_array,
//                             )
//                             .unwrap();

//                             writer.write(b).await.unwrap();

//                             let adds = writer
//                                 .flush_and_commit(&mut table)
//                                 .await
//                                 .expect("Failed to flush write");
//                         }
//                         decoder.finish().unwrap();
//                     }
//                     Ok(())
//                 });
//             }
//             _ => {
//                 error!("unimplemented")
//             }
//         }
//     }

//     Ok(())
// }

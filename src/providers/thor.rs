use std::{
    collections::HashMap,
    error::Error,
    sync::{Arc, Mutex},
};

use futures_util::{stream::StreamExt, sink::SinkExt};
use tokio::{sync::broadcast, task};
use publisher::{
    event_publisher_client::EventPublisherClient,
    Empty, StreamResponse, SubscribeWalletRequest,
    
};
use thor_streamer::types::{
    MessageWrapper, SlotStatusEvent, TransactionEvent, TransactionEventWrapper, UpdateAccountEvent,
    message_wrapper::EventMessage,
};
use prost::Message;
use tonic::transport::Uri;
use tonic::{Request, Streaming};
use tokio_stream::Stream;

use crate::{
    config::{Config, Endpoint},
    utils::{Comparator, TransactionData, get_current_timestamp, open_log_file, write_log_entry},
};

use super::GeyserProvider;

pub mod thor_streamer {
    #![allow(clippy::clone_on_ref_ptr)]
    #![allow(clippy::missing_const_for_fn)]
    
    pub mod types {
        include!(concat!(env!("OUT_DIR"), "/thor_streamer.types.rs"));
    }
}

pub mod publisher {
    #![allow(clippy::clone_on_ref_ptr)]
    #![allow(clippy::missing_const_for_fn)]
    
    include!(concat!(env!("OUT_DIR"), "/publisher.rs"));
    
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("proto_descriptors");
}


pub struct ThorProvider;

impl GeyserProvider for ThorProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        shutdown_tx: broadcast::Sender<()>,
        shutdown_rx: broadcast::Receiver<()>,
        start_time: f64,
        comparator: Arc<Mutex<Comparator>>,
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(async move {
            process_thor_endpoint(
                endpoint,
                config,
                shutdown_tx,
                shutdown_rx,
                start_time,
                comparator,
            )
            .await
        })
    }
}

async fn process_thor_endpoint(
    endpoint: Endpoint,
    config: Config,
    shutdown_tx: broadcast::Sender<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
    start_time: f64,
    comparator: Arc<Mutex<Comparator>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut transaction_count = 0;

    let mut log_file = open_log_file(&endpoint.name)?;

    log::info!(
        "[{}] Connecting to endpoint: {}",
        endpoint.name,
        endpoint.url
    );

    let grpc_url = &endpoint.url;
    let grpc_token = &endpoint.x_token;
    // Connect to the gRPC server
    let uri = grpc_url.parse::<tonic::transport::Uri>()?;
    let mut client = EventPublisherClient::connect(uri).await?;
    log::info!("[{}] Connected successfully", endpoint.name);

    let mut request = Request::new(Empty {});
    request
        .metadata_mut()
        .insert("authorization", grpc_token.parse()?);
    
    // Subscribe to transactions stream
    let mut stream: Streaming<StreamResponse> = client
        .subscribe_to_transactions(request)
        .await?
        .into_inner();

    'ploop: loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                log::info!("[{}] Received stop signal...", endpoint.name);
                break;
            }

            message = stream.next() => {
                if let Some(Ok(msg)) = message {
                    if let Ok(message_wrapper) = MessageWrapper::decode(&*msg.data) {
                        if let Some(EventMessage::Transaction(transaction_event_wrapper)) = message_wrapper.event_message {
                            if let Some(transaction_event) = transaction_event_wrapper.transaction {
                                if let Some(transaction) = transaction_event.transaction.as_ref() {
                                    if let Some(message) = transaction.message.as_ref() {
                                        let accounts: Vec<String> = message.account_keys
                                            .iter()
                                            .map(|key| bs58::encode(key).into_string())
                                            .collect();
                                            
                                        if accounts.contains(&config.account) {
                                            let timestamp = get_current_timestamp();
                                            let signature = bs58::encode(&transaction_event.signature).into_string();
                                            let slot = transaction_event.slot;

                                            write_log_entry(&mut log_file, timestamp, &endpoint.name, &signature)?;

                                            let mut comp = comparator.lock().unwrap();

                                            comp.add(
                                                endpoint.name.clone(),
                                                TransactionData {
                                                    timestamp,
                                                    signature: signature.clone(),
                                                    start_time,
                                                },
                                            );

                                            if comp.get_valid_count() == config.transactions as usize {
                                                log::info!("Endpoint {} shutting down after {} transactions seen and {} by all workers",
                                                    endpoint.name, transaction_count, config.transactions);
                                                shutdown_tx.send(()).unwrap();
                                                break 'ploop;
                                            }

                                            log::info!("[{:.3}] [{}] {}", timestamp, endpoint.name, signature);
                                            transaction_count += 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    log::info!("[{}] Stream closed", endpoint.name);
    Ok(())
}
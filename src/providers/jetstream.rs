use std::{ collections::HashMap, error::Error, sync::{ Arc, Mutex } };
use futures::channel::mpsc::unbounded;
use futures_util::{ stream::StreamExt, sink::SinkExt };
use tokio::{ sync::broadcast, task };
use tokio_stream::Stream;

use crate::{
    config::{ Config, Endpoint },
    utils::{ Comparator, TransactionData, get_current_timestamp, open_log_file, write_log_entry },
};

use super::GeyserProvider;

pub mod jetstream {
    #![allow(clippy::clone_on_ref_ptr)]
    #![allow(clippy::missing_const_for_fn)]

    include!(concat!(env!("OUT_DIR"), "/jetstream.rs"));

    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("proto_descriptors");
}

use jetstream::{
    jetstream_client::JetstreamClient,
    SubscribeRequest, SubscribeUpdate,
    SubscribeRequestFilterTransactions,
};

pub struct JetstreamProvider;

impl GeyserProvider for JetstreamProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        shutdown_tx: broadcast::Sender<()>,
        shutdown_rx: broadcast::Receiver<()>,
        start_time: f64,
        comparator: Arc<Mutex<Comparator>>
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(async move {
            process_jetstream_endpoint(
                endpoint,
                config,
                shutdown_tx,
                shutdown_rx,
                start_time,
                comparator
            ).await
        })
    }
}

async fn process_jetstream_endpoint(
    endpoint: Endpoint,
    config: Config,
    shutdown_tx: broadcast::Sender<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
    start_time: f64,
    comparator: Arc<Mutex<Comparator>>
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut transaction_count = 0;

    let mut log_file = open_log_file(&endpoint.name)?;

    log::info!("[{}] Connecting to endpoint: {}", endpoint.name, endpoint.url);

    let mut client = JetstreamClient::connect(endpoint.url).await?;
    log::info!("[{}] Connected successfully", endpoint.name);

    let mut transactions: HashMap<
        String,
        jetstream::SubscribeRequestFilterTransactions
    > = HashMap::new();
    transactions.insert(String::from("account"), jetstream::SubscribeRequestFilterTransactions {
        account_exclude: vec![],
        account_include: vec![],
        account_required: vec![config.account.clone()],
    });

    let request = jetstream::SubscribeRequest { 
        transactions,
        accounts: HashMap::new(),
        ping: None,
    };

    let (mut subscribe_tx, subscribe_rx) = unbounded::<jetstream::SubscribeRequest>();
    subscribe_tx.send(request).await?;
    let mut stream = client.subscribe(subscribe_rx).await?.into_inner();

    'ploop: loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                log::info!("[{}] Received stop signal...", endpoint.name);
                break;
            }

            message = stream.next() => {
                if let Some(Ok(msg)) = message {
                    if let Some(jetstream::subscribe_update::UpdateOneof::Transaction(tx)) = msg.update_oneof {
                        if let Some(tx_info) = &tx.transaction {
                            let account_keys = tx_info.account_keys
                                .iter()
                                .map(|key| bs58::encode(key).into_string())
                                .collect::<Vec<String>>();

                            if account_keys.contains(&config.account) {
                                let timestamp = get_current_timestamp();
                                let signature = bs58::encode(&tx_info.signature).into_string();

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

    log::info!("[{}] Stream closed", endpoint.name);
    Ok(())
}
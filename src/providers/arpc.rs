use std::{
    collections::HashMap,
    error::Error,
    sync::{Arc, Mutex},
};

use futures_util::{stream::StreamExt, sink::SinkExt};
use tokio::{sync::broadcast, task};
use tokio_stream::Stream;

use crate::{
    config::{Config, Endpoint},
    utils::{Comparator, TransactionData, get_current_timestamp, open_log_file, write_log_entry},
};

use super::GeyserProvider;

pub mod arpc {
    #![allow(clippy::clone_on_ref_ptr)]
    #![allow(clippy::missing_const_for_fn)]

    include!(concat!(env!("OUT_DIR"), "/arpc.rs"));

    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("proto_descriptors");
}

use arpc::{
    arpc_service_client::ArpcServiceClient,
    SubscribeRequest as ArpcSubscribeRequest,
    SubscribeRequestFilterTransactions,
};

pub struct ArpcProvider;

impl GeyserProvider for ArpcProvider {
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
            process_arpc_endpoint(
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

async fn process_arpc_endpoint(
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

    let mut client = ArpcServiceClient::connect(endpoint.url).await?;
    log::info!("[{}] Connected successfully", endpoint.name);

    fn reqstream(account: String) -> impl Stream<Item = ArpcSubscribeRequest> {
        let mut transactions = HashMap::new();
        transactions.insert(
            String::from("account"),
            SubscribeRequestFilterTransactions {
                account_include: vec![account],
                account_exclude: vec![],
                account_required: vec![],
            },
        );
        tokio_stream::iter(vec![ArpcSubscribeRequest {
            transactions,
            ping_id: Some(0),
        }])
    }

    let in_stream = reqstream(config.account.clone());

    let mut stream = client.subscribe(in_stream).await?.into_inner();

    'ploop: loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                log::info!("[{}] Received stop signal...", endpoint.name);
                break;
            }

            message = stream.next() => {
                if let Some(Ok(msg)) = message {
                    if let Some(tx) = msg.transaction {
                        let accounts = tx.account_keys
                            .iter()
                            .map(|key| bs58::encode(key).into_string())
                            .collect::<Vec<String>>();

                        if accounts.contains(&config.account) {
                            let timestamp = get_current_timestamp();
                            let signature = bs58::encode(&tx.signatures[0]).into_string();

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

    log::info!("[{}] Stream closed", endpoint.name);
    Ok(())
}
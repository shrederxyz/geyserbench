use futures::{SinkExt, channel::mpsc::unbounded};
use futures_util::stream::StreamExt;
use solana_pubkey::Pubkey;
use std::{collections::HashMap, error::Error};
use tracing::{Level, error, info};

use crate::{
    aggregator::AggregatorMessage,
    config::{Config, Endpoint},
    utils::{TransactionData, get_current_timestamp, open_log_file, write_log_entry},
};

use super::{
    GeyserProvider, ProviderContext,
    common::{TransactionAccumulator, fatal_connection_error},
};

#[allow(clippy::all, dead_code)]
pub mod jetstream {
    include!(concat!(env!("OUT_DIR"), "/jetstream.rs"));
}

use jetstream::jetstream_client::JetstreamClient;

pub struct JetstreamProvider;

impl GeyserProvider for JetstreamProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> std::thread::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        let shutdown_tx = context.shutdown_tx.clone();
        let endpoint_name_for_log = endpoint.name.clone();
        std::thread::Builder::new()
            .name(format!("provider-{}", endpoint.name))
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                let result = rt.block_on(process_jetstream_endpoint(endpoint, config, context));
                if result.is_err() {
                    error!(endpoint = %endpoint_name_for_log, "Provider failed; broadcasting shutdown");
                    let _ = shutdown_tx.send(());
                }
                result
            })
            .expect("failed to spawn provider thread")
    }
}

async fn process_jetstream_endpoint(
    endpoint: Endpoint,
    config: Config,
    context: ProviderContext,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let ProviderContext {
        shutdown_tx: _,
        mut shutdown_rx,
        start_wallclock_secs,
        start_instant,
        observation_tx,
    } = context;
    let account_pubkey = config.account.parse::<Pubkey>()?;
    let endpoint_name = endpoint.name.clone();
    let mut log_file = if tracing::enabled!(Level::TRACE) {
        Some(open_log_file(&endpoint_name)?)
    } else {
        None
    };

    let endpoint_url = endpoint.url.clone();

    info!(endpoint = %endpoint_name, url = %endpoint_url, "Connecting");

    let mut client = JetstreamClient::connect(endpoint_url.clone())
        .await
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));
    info!(endpoint = %endpoint_name, "Connected");

    let mut transactions: HashMap<String, jetstream::SubscribeRequestFilterTransactions> =
        HashMap::new();
    transactions.insert(
        String::from("account"),
        jetstream::SubscribeRequestFilterTransactions {
            account_exclude: vec![],
            account_include: vec![],
            account_required: vec![config.account.clone()],
        },
    );

    let request = jetstream::SubscribeRequest {
        transactions,
        accounts: HashMap::new(),
        ping: None,
    };

    let (mut subscribe_tx, subscribe_rx) = unbounded::<jetstream::SubscribeRequest>();
    subscribe_tx.send(request).await?;

    let mut stream = client.subscribe(subscribe_rx).await?.into_inner();

    let mut accumulator = TransactionAccumulator::new();

    let mut transaction_count = 0usize;

    loop {
        tokio::select! { biased;
            _ = shutdown_rx.recv() => {
                info!(endpoint = %endpoint_name, "Received stop signal");
                break;
            }

            message = stream.next() => {
                let Some(Ok(msg)) = message else { continue };
                let Some(jetstream::subscribe_update::UpdateOneof::Transaction(tx)) = msg.update_oneof else { continue };
                let Some(tx_info) = &tx.transaction else { continue };

                let has_account = tx_info
                    .account_keys
                    .iter()
                    .any(|key| key.as_slice() == account_pubkey.as_ref());
                if !has_account { continue }

                let wallclock = get_current_timestamp();
                let elapsed = start_instant.elapsed();
                let signature = bs58::encode(&tx_info.signature).into_string();

                if let Some(file) = log_file.as_mut() {
                    write_log_entry(file, wallclock, &endpoint_name, &signature)?;
                }

                let tx_data = TransactionData {
                    wallclock_secs: wallclock,
                    elapsed_since_start: elapsed,
                    start_wallclock_secs,
                };

                let updated = accumulator.record(signature.clone(), tx_data.clone());

                if updated {
                    let _ = observation_tx.send(AggregatorMessage::Observation {
                        endpoint: endpoint_name.clone(),
                        signature: signature.clone(),
                        data: tx_data,
                    });
                }

                transaction_count += 1;
            }
        }
    }

    let unique_signatures = accumulator.len();
    let collected = accumulator.into_inner();
    let _ = observation_tx.send(AggregatorMessage::Batch {
        endpoint: endpoint_name.clone(),
        transactions: collected,
    });
    info!(
        endpoint = %endpoint_name,
        total_transactions = transaction_count,
        unique_signatures,
        "Stream closed after dispatching transactions"
    );
    Ok(())
}

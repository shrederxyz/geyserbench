use futures_util::stream::StreamExt;
use std::error::Error;
use tracing::{Level, error, info};

use solana_pubkey::Pubkey;

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
pub mod shredstream {
    include!(concat!(env!("OUT_DIR"), "/shredstream.rs"));
}

pub struct ShredstreamProvider;

impl GeyserProvider for ShredstreamProvider {
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
                let result = rt.block_on(process_shredstream_endpoint(endpoint, config, context));
                if result.is_err() {
                    error!(endpoint = %endpoint_name_for_log, "Provider failed; broadcasting shutdown");
                    let _ = shutdown_tx.send(());
                }
                result
            })
            .expect("failed to spawn provider thread")
    }
}

async fn process_shredstream_endpoint(
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

    let mut client = shredstream::shredstream_proxy_client::ShredstreamProxyClient::connect(
        endpoint_url.clone(),
    )
    .await
    .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));
    info!(endpoint = %endpoint_name, "Connected");

    let request = shredstream::SubscribeEntriesRequest {};
    let mut stream = client.subscribe_entries(request).await?.into_inner();

    let mut accumulator = TransactionAccumulator::new();
    let mut transaction_count = 0usize;

    loop {
        tokio::select! { biased;
        _ = shutdown_rx.recv() => {
            info!(endpoint = %endpoint_name, "Received stop signal");
            break;
        }

        Some(Ok(slot_entry)) = stream.next() => {
            let entries = match bincode::deserialize::<Vec<solana_entry::entry::Entry>>(
                &slot_entry.entries,
            ) {
                Ok(e) => e,
                Err(e) => {
                    error!(endpoint = %endpoint_name, error = %e, "Failed to deserialize shredstream entries");
                    continue;
                }
            };
            for entry in entries {
                for tx in entry.transactions {
                    let has_account = tx
                        .message
                        .static_account_keys()
                        .iter()
                        .any(|key| key == &account_pubkey);

                    if !has_account {
                        continue;
                    }

                    let wallclock = get_current_timestamp();
                    let elapsed = start_instant.elapsed();
                    let signature = tx.signatures[0].to_string();

                    if let Some(file) = log_file.as_mut() {
                        write_log_entry(file, wallclock, &endpoint_name, &signature)?;
                    }

                    let tx_data = TransactionData {
                        wallclock_secs: wallclock,
                        elapsed_since_start: elapsed,
                        start_wallclock_secs,
                    };

                    let updated = accumulator.record(
                        signature.clone(),
                        tx_data.clone(),
                    );

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

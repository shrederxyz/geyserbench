use std::{collections::HashMap, error::Error};

use futures::{SinkExt, channel::mpsc::unbounded};
use futures_util::stream::StreamExt;
use solana_pubkey::Pubkey;
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
pub mod arpc {
    include!(concat!(env!("OUT_DIR"), "/arpc.rs"));
}

use arpc::{
    SubscribeRequest as ArpcSubscribeRequest, SubscribeRequestFilterTransactions,
    arpc_service_client::ArpcServiceClient,
};

pub struct ArpcProvider;

impl GeyserProvider for ArpcProvider {
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
                let result = rt.block_on(process_arpc_endpoint(endpoint, config, context));
                if result.is_err() {
                    error!(endpoint = %endpoint_name_for_log, "Provider failed; broadcasting shutdown");
                    let _ = shutdown_tx.send(());
                }
                result
            })
            .expect("failed to spawn provider thread")
    }
}

async fn process_arpc_endpoint(
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

    let mut client = ArpcServiceClient::connect(endpoint_url.clone())
        .await
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));
    info!(endpoint = %endpoint_name, "Connected");

    let transactions = HashMap::from([(
        "account".to_string(),
        SubscribeRequestFilterTransactions {
            account_include: vec![config.account.clone()],
            account_exclude: vec![],
            account_required: vec![],
        },
    )]);

    let request = ArpcSubscribeRequest {
        transactions,
        ping_id: Some(0),
    };

    let (mut subscribe_tx, subscribe_rx) = unbounded::<ArpcSubscribeRequest>();
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
                let Some(tx) = msg.transaction else { continue };

                let has_account = tx.account_keys
                    .iter()
                    .any(|k| k.as_slice() == account_pubkey.as_ref());
                if !has_account { continue }

                let wallclock = get_current_timestamp();
                let elapsed = start_instant.elapsed();
                let signature = tx.signatures
                    .first()
                    .map(|s| bs58::encode(s).into_string())
                    .unwrap_or_default();

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

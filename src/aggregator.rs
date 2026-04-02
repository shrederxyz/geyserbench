use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
};

use crossbeam_queue::ArrayQueue;
use tracing::{info, warn};

use crate::{
    backend::{SignatureEnvelope, SignatureObservation},
    utils::{Comparator, ProgressTracker, TransactionData},
};

pub enum AggregatorMessage {
    Observation {
        endpoint: String,
        signature: String,
        data: TransactionData,
    },
    Batch {
        endpoint: String,
        transactions: HashMap<String, TransactionData>,
    },
}

pub struct AggregatorConfig {
    pub rx: mpsc::Receiver<AggregatorMessage>,
    pub shutdown_tx: tokio::sync::broadcast::Sender<()>,
    pub shared_counter: Arc<AtomicUsize>,
    pub shared_shutdown: Arc<AtomicBool>,
    pub target_transactions: Option<usize>,
    pub total_producers: usize,
    pub progress: Option<Arc<ProgressTracker>>,
    pub signature_tx: Option<Arc<ArrayQueue<SignatureEnvelope>>>,
}

pub fn run_aggregator(config: AggregatorConfig) -> Comparator {
    let AggregatorConfig {
        rx,
        shutdown_tx,
        shared_counter,
        shared_shutdown,
        target_transactions,
        total_producers,
        progress,
        signature_tx,
    } = config;

    let mut comparator = Comparator::new();

    while let Ok(msg) = rx.recv() {
        match msg {
            AggregatorMessage::Observation {
                endpoint,
                signature,
                data,
            } => {
                if let Some(envelope) = build_signature_envelope(
                    &mut comparator,
                    &endpoint,
                    &signature,
                    data,
                    total_producers,
                ) {
                    if let Some(target) = target_transactions {
                        let shared = shared_counter.fetch_add(1, Ordering::AcqRel) + 1;
                        if let Some(tracker) = progress.as_ref() {
                            tracker.record(shared);
                        }
                        if shared >= target
                            && !shared_shutdown.swap(true, Ordering::AcqRel)
                        {
                            info!(target, "Reached shared signature target; broadcasting shutdown");
                            let _ = shutdown_tx.send(());
                        }
                    }

                    if let Some(sender) = signature_tx.as_ref() {
                        if sender.push(envelope).is_err() {
                            warn!(
                                endpoint = %endpoint,
                                signature = %signature,
                                "Signature queue full; dropping observation"
                            );
                        }
                    }
                }
            }
            AggregatorMessage::Batch {
                endpoint,
                transactions,
            } => {
                comparator.add_batch(&endpoint, transactions);
            }
        }
    }

    comparator
}

fn build_signature_envelope(
    comparator: &mut Comparator,
    endpoint: &str,
    signature: &str,
    data: TransactionData,
    total_producers: usize,
) -> Option<SignatureEnvelope> {
    comparator
        .record_observation(endpoint, signature, data, total_producers)
        .map(|observations| {
            let mut payload = observations
                .into_iter()
                .map(|(endpoint, tx_data)| SignatureObservation {
                    endpoint,
                    timestamp: tx_data.wallclock_secs,
                    backfilled: tx_data.wallclock_secs < tx_data.start_wallclock_secs,
                })
                .collect::<Vec<_>>();
            payload.sort_by(|lhs, rhs| lhs.endpoint.cmp(&rhs.endpoint));
            SignatureEnvelope {
                signature: signature.to_owned(),
                observations: payload,
            }
        })
}

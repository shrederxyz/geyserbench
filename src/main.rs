pub use {
    bs58,
    bytes::Bytes,
    futures_util::stream::StreamExt,
    serde::{Deserialize, Serialize},
    std::{
        env,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
            mpsc as std_mpsc,
        },
        thread,
        time::{Duration, Instant},
    },
    tokio::{signal::ctrl_c, sync::broadcast, task},
};

mod aggregator;
mod analysis;
mod backend;
mod config;
mod proto;
mod providers;
mod utils;

use anyhow::{Result, anyhow};
use backend::{BackendStatus, StreamOptions};
use crossbeam_queue::ArrayQueue;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;
use utils::{ProgressTracker, get_current_timestamp};
const DEFAULT_CONFIG_PATH: &str = "config.toml";
const DEFAULT_BACKEND_STREAM_URL: &str = "wss://gb.solstack.app/v1/benchmarks/stream";
const MAX_STREAM_TRANSACTIONS: i32 = 100_000;
const SIGNATURE_QUEUE_CAPACITY: usize = 1_024;

struct CliArgs {
    config_path: Option<String>,
    disable_streaming: bool,
}

impl CliArgs {
    fn parse() -> Self {
        let mut args = env::args().skip(1);
        let mut parsed = CliArgs {
            config_path: None,
            disable_streaming: false,
        };

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--config" => {
                    let value = args.next().unwrap_or_else(|| {
                        eprintln!("Missing value for --config");
                        print_usage();
                        std::process::exit(1);
                    });
                    parsed.config_path = Some(value);
                }
                "--private" => {
                    parsed.disable_streaming = true;
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => {
                    eprintln!("Unknown argument: {}", other);
                    print_usage();
                    std::process::exit(1);
                }
            }
        }

        parsed
    }
}

fn print_usage() {
    eprintln!("Usage: geyserbench [--config <PATH>] [--private]");
}

#[tokio::main]
async fn main() -> Result<()> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .compact()
        .try_init()
        .map_err(|err| anyhow!(err))?;

    let cli = CliArgs::parse();
    let config_path = cli.config_path.as_deref().unwrap_or(DEFAULT_CONFIG_PATH);
    let config = config::ConfigToml::load_or_create(config_path)?;
    info!(config_path = config_path, "Loaded configuration");

    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    let start_time_local = get_current_timestamp();
    let start_instant = Instant::now();
    let clock_offset_ms: f64;
    let server_started_at_unix_ms: Option<i64>;
    let shared_counter = Arc::new(AtomicUsize::new(0));
    let shared_shutdown = Arc::new(AtomicBool::new(false));
    let aborted = Arc::new(AtomicBool::new(false));

    let high_transaction_volume = config.config.transactions > MAX_STREAM_TRANSACTIONS;
    if high_transaction_volume {
        warn!(
            transactions = config.config.transactions,
            threshold = MAX_STREAM_TRANSACTIONS,
            "Disabling backend streaming for high-volume run; backend streaming is unavailable when transactions exceed the threshold"
        );
    }

    let mut backend_settings = config.backend.clone();
    backend_settings.enabled = !(cli.disable_streaming || high_transaction_volume);
    backend_settings.url = Some(DEFAULT_BACKEND_STREAM_URL.to_string());

    let mut backend_handle = None;
    let mut signature_queue: Option<Arc<ArrayQueue<backend::SignatureEnvelope>>> = None;
    let mut signature_forwarder: Option<thread::JoinHandle<()>> = None;
    let mut forwarder_stop: Option<Arc<AtomicBool>> = None;
    let mut backend_run_id = None;

    if backend_settings.enabled {
        let url = backend_settings
            .url
            .clone()
            .ok_or_else(|| anyhow!("backend streaming enabled but no URL configured"))?;
        let options = StreamOptions { url, summary: None };
        let handle = backend::connect_stream(options, &config.config, &config.endpoint).await?;
        clock_offset_ms = handle.clock_offset_ms();
        server_started_at_unix_ms = handle.server_started_at_unix_ms();
        let run_id = handle.run_id().to_string();
        info!(
            run_id = %run_id,
            started_at_unix_ms = server_started_at_unix_ms,
            clock_offset_ms,
            "Streaming backend session initialised"
        );
        backend_run_id = Some(run_id.clone());

        let queue = Arc::new(ArrayQueue::new(SIGNATURE_QUEUE_CAPACITY));
        let queue_handle = queue.clone();
        signature_queue = Some(queue);

        let mut status_rx = handle.status();
        let shutdown_for_backend = shutdown_tx.clone();
        let run_id_for_status = run_id.clone();
        tokio::spawn(async move {
            while status_rx.changed().await.is_ok() {
                match status_rx.borrow().clone() {
                    BackendStatus::Failed { message } => {
                        error!(run_id = %run_id_for_status, error = %message, "Backend streaming failed");
                        let _ = shutdown_for_backend.send(());
                        break;
                    }
                    BackendStatus::Completed { .. } => break,
                    BackendStatus::Ready { run_id } => {
                        debug!(run_id = %run_id, "Backend stream ready");
                    }
                    BackendStatus::Initializing => {}
                }
            }
        });

        let backend_sender = handle.signature_sender();
        let run_id_for_forwarder = run_id.clone();
        let stop_flag = Arc::new(AtomicBool::new(false));
        forwarder_stop = Some(stop_flag.clone());
        let forwarder = thread::spawn(move || {
            let queue = queue_handle;
            loop {
                let mut did_work = false;
                while let Some(envelope) = queue.pop() {
                    did_work = true;
                    if backend_sender.blocking_send(envelope).is_err() {
                        warn!(run_id = %run_id_for_forwarder, "Failed to forward signature to backend");
                        return;
                    }
                }

                let should_stop = stop_flag.load(Ordering::Acquire);
                if should_stop && queue.is_empty() {
                    break;
                }

                if !did_work {
                    thread::sleep(Duration::from_millis(1));
                }
            }
        });
        signature_forwarder = Some(forwarder);
        backend_handle = Some(handle);
    } else {
        info!("Backend streaming disabled; collecting metrics locally");
    }

    // Create observation channel for providers -> aggregator
    let (observation_tx, observation_rx) = std_mpsc::channel::<aggregator::AggregatorMessage>();

    let endpoint_names: Vec<String> = config.endpoint.iter().map(|e| e.name.clone()).collect();
    let global_target = if config.config.transactions > 0 {
        Some(config.config.transactions as usize)
    } else {
        None
    };
    let progress_tracker = global_target.map(|target| Arc::new(ProgressTracker::new(target)));

    let total_producers = config.endpoint.len();

    // Spawn aggregator in dedicated OS thread
    let aggregator_shutdown_tx = shutdown_tx.clone();
    let aggregator_shared_counter = shared_counter.clone();
    let aggregator_shared_shutdown = shared_shutdown.clone();
    let aggregator_progress = progress_tracker.clone();
    let aggregator_signature_tx = signature_queue.clone();
    let aggregator_handle = thread::Builder::new()
        .name("aggregator".to_string())
        .spawn(move || {
            aggregator::run_aggregator(aggregator::AggregatorConfig {
                rx: observation_rx,
                shutdown_tx: aggregator_shutdown_tx,
                shared_counter: aggregator_shared_counter,
                shared_shutdown: aggregator_shared_shutdown,
                target_transactions: global_target,
                total_producers,
                progress: aggregator_progress,
                signature_tx: aggregator_signature_tx,
            })
        })
        .expect("failed to spawn aggregator thread");

    // Spawn providers in dedicated OS threads
    let mut handles = Vec::new();
    for (_index, endpoint) in config.endpoint.clone().into_iter().enumerate() {
        let provider = providers::create_provider(&endpoint.kind);
        let shared_config = config.config.clone();
        let context = providers::ProviderContext {
            shutdown_tx: shutdown_tx.clone(),
            shutdown_rx: shutdown_tx.subscribe(),
            start_wallclock_secs: start_time_local,
            start_instant,
            observation_tx: observation_tx.clone(),
        };

        handles.push(provider.process(endpoint, shared_config, context));
    }

    // Drop our copy of the sender so aggregator sees channel close when all providers finish
    drop(observation_tx);

    tokio::spawn({
        let shutdown_tx = shutdown_tx.clone();
        let shared_shutdown = shared_shutdown.clone();
        let aborted = aborted.clone();
        async move {
            match ctrl_c().await {
                Ok(()) => {
                    let already_aborting = aborted.swap(true, Ordering::AcqRel);
                    if already_aborting {
                        info!("Received additional Ctrl+C; shutdown already in progress");
                    } else {
                        info!("Received Ctrl+C; initiating shutdown");
                    }
                    shared_shutdown.store(true, Ordering::Release);
                    let _ = shutdown_tx.send(());
                }
                Err(err) => error!(error = %err, "Failed to listen for Ctrl+C"),
            }
        }
    });

    // Wait for all provider threads to finish (in a blocking context)
    let join_result = task::spawn_blocking(move || {
        for handle in handles {
            match handle.join() {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => error!(error = ?e, "Provider task returned error"),
                Err(e) => error!(error = ?e, "Provider join error"),
            }
        }
    })
    .await;

    if let Err(e) = join_result {
        error!(error = ?e, "Failed to join provider threads");
    }

    // Wait for aggregator to finish and return the Comparator
    let comparator = task::spawn_blocking(move || {
        aggregator_handle
            .join()
            .expect("aggregator thread panicked")
    })
    .await?;

    let run_aborted = aborted.load(Ordering::Acquire);

    let run_summary = if !run_aborted {
        Some(analysis::compute_run_summary(
            &comparator,
            &endpoint_names,
        ))
    } else {
        None
    };

    if let Some(handle) = backend_handle {
        if run_aborted {
            if let Some(run_id) = backend_run_id.as_ref() {
                info!(run_id = %run_id, "Skipping backend finalisation due to user abort");
            } else {
                info!("Skipping backend finalisation due to user abort");
            }
        } else {
            let run_id = backend_run_id
                .clone()
                .unwrap_or_else(|| "unknown".to_string());
            match handle.finish().await {
                Ok(result) => {
                    info!(run_id = %run_id, "Backend completed run");
                    debug!(run_id = %run_id, response = %result.response, "Backend completion payload");
                }
                Err(err) => {
                    error!(run_id = %run_id, error = %err, "Backend streaming ended with error");
                }
            }
        }
    }

    let _ = signature_queue.take();
    if let Some(stop) = forwarder_stop.as_ref() {
        stop.store(true, Ordering::Release);
    }

    if let Some(join) = signature_forwarder
        && let Err(err) = join.join()
    {
        warn!(
            "Signature forwarder thread terminated unexpectedly: {:?}",
            err
        );
    }

    if !run_aborted {
        if let Some(summary) = run_summary.as_ref() {
            analysis::display_run_summary(summary);
            let metrics_json = analysis::build_metrics_report(summary);
            debug!(metrics = %metrics_json, "Computed run metrics");
        }

        if let Some(run_id) = backend_run_id {
            println!("🔗 Share this benchmark run: https://runs.solstack.app/run/{run_id}");
        }
    } else {
        info!("Benchmark aborted before completion; no results were generated");
    }

    Ok(())
}

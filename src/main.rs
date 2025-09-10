pub use {

    bs58,
    bytes::Bytes,
    env_logger,
    futures_util::stream::StreamExt,
    log,
    serde::{Deserialize, Serialize},
    std::{
        sync::{Arc, Mutex},
        env,
    },
    tokio::{signal::ctrl_c, sync::broadcast, task},
};

mod config;
mod utils;
mod analysis;
mod providers;

use providers::GeyserProvider;
use utils::{Comparator, get_current_timestamp};

const CONFIG_PATH: &str = "config.toml";


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();


    let config = config::ConfigToml::load_or_create(CONFIG_PATH)?;
    log::info!("Loaded configuration");

    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    let endpoint_count = config.endpoint.len();

    let start_time = get_current_timestamp();
    let comparator = Arc::new(Mutex::new(Comparator::new(config.config.transactions as usize)));


    let mut handles = Vec::new();
    let endpoint_names: Vec<String> = config.endpoint.iter().map(|e| e.name.clone()).collect();

    for endpoint in config.endpoint.clone() {
        let provider = providers::create_provider(&endpoint.kind);
        let shared_config = config.config.clone();
        let stx = shutdown_tx.clone();
        let shutdown_rx = shutdown_tx.subscribe();
        let shared_comparator = comparator.clone();

        handles.push(provider.process(
            endpoint,
            shared_config,
            stx,
            shutdown_rx,
            start_time,
            shared_comparator,
        ));

    }

    tokio::spawn(async move {
        if let Ok(_) = ctrl_c().await {
            println!("\nReceived Ctrl+C signal. Shutting down...");
            let _ = shutdown_tx.send(());
        }
    });

    for handle in handles {
        match handle.await {
            Ok(Ok(_)) => {},
            Ok(Err(e)) => log::error!("Provider error: {:?}", e),
            Err(e) => log::error!("Task join error: {:?}", e),
        }
    }


    analysis::analyze_delays(&comparator.lock().unwrap(), endpoint_names);


    Ok(())
}
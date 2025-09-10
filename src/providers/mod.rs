use std::{sync::Arc, error::Error};
use tokio::sync::broadcast;

use crate::{
    utils::{Comparator, TransactionData},
    config::{Config, Endpoint, EndpointKind},
};

pub mod arpc;
pub mod yellowstone;
pub mod thor;
pub mod shreder;
pub mod jetstream;

pub trait GeyserProvider: Send + Sync {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        shutdown_tx: broadcast::Sender<()>,
        shutdown_rx: broadcast::Receiver<()>,
        start_time: f64,
        comparator: Arc<std::sync::Mutex<Comparator>>,
    ) -> tokio::task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>;
}

pub fn create_provider(kind: &EndpointKind) -> Box<dyn GeyserProvider> {
    match kind {
        EndpointKind::Yellowstone => Box::new(yellowstone::YellowstoneProvider),
        EndpointKind::Arpc => Box::new(arpc::ArpcProvider),
        EndpointKind::Thor => Box::new(thor::ThorProvider),
        EndpointKind::Shreder => Box::new(shreder::ShrederProvider),
        EndpointKind::Jetstream => Box::new(jetstream::JetstreamProvider),
    }
}
use std::{
    error::Error,
    sync::mpsc,
    thread,
    time::Instant,
};
use tokio::sync::broadcast;

use crate::{
    aggregator::AggregatorMessage,
    config::{Config, Endpoint, EndpointKind},
};

pub mod arpc;
pub mod common;
pub mod jetstream;
pub mod laserstream;
pub mod shreder;
pub mod shreder_binary;
pub mod shredstream;
pub mod thor;
pub mod yellowstone;
mod yellowstone_client;

pub trait GeyserProvider: Send + Sync {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> thread::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>;
}

pub fn create_provider(kind: &EndpointKind) -> Box<dyn GeyserProvider> {
    match kind {
        EndpointKind::Yellowstone => Box::new(yellowstone::YellowstoneProvider),
        EndpointKind::Arpc => Box::new(arpc::ArpcProvider),
        EndpointKind::Thor => Box::new(thor::ThorProvider),
        EndpointKind::Shreder => Box::new(shreder::ShrederProvider),
        EndpointKind::ShrederBinary => Box::new(shreder_binary::ShrederBinaryProvider),
        EndpointKind::Shredstream => Box::new(shredstream::ShredstreamProvider),
        EndpointKind::Jetstream => Box::new(jetstream::JetstreamProvider),
        EndpointKind::Laserstream => Box::new(laserstream::LaserstreamProvider),
    }
}

pub struct ProviderContext {
    pub shutdown_tx: broadcast::Sender<()>,
    pub shutdown_rx: broadcast::Receiver<()>,
    pub start_wallclock_secs: f64,
    pub start_instant: Instant,
    pub observation_tx: mpsc::Sender<AggregatorMessage>,
}

use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::Write,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::config::ConfigToml;

#[derive(Debug, Clone)]
pub struct TransactionData {
    pub timestamp: f64,
    pub signature: String,
    pub start_time: f64,
}

#[derive(Debug, Clone, Default)]
pub struct Comparator {
    pub data: HashMap<String, HashMap<String, TransactionData>>,
    pub worker_count: usize,
}

impl Comparator {
    pub fn new(worker_count: usize) -> Self {
        Self {
            data: HashMap::new(),
            worker_count,
        }
    }

    pub fn add(&mut self, from: String, data: TransactionData) {
        self.data
            .entry(data.signature.clone())
            .or_insert_with(HashMap::new)
            .insert(from.clone(), data.clone());

        let valid_count = self.get_valid_count();
        log::info!(
            "{}/{} total valid transactions",
            valid_count,
            self.worker_count
        );
    }

    pub fn get_valid_count(&self) -> usize {
        self.data.len()
    }
}

pub fn get_current_timestamp() -> f64 {
    let start = SystemTime::now();
    let since_epoch = start.duration_since(UNIX_EPOCH).expect("Time went backwards");
    since_epoch.as_secs_f64()
}

pub fn percentile(sorted_data: &[f64], p: f64) -> f64 {
    if sorted_data.is_empty() {
        return 0.0;
    }
    let index = (p * (sorted_data.len() - 1) as f64).round() as usize;
    sorted_data[index]
}

pub fn open_log_file(name: &str) -> std::io::Result<impl Write> {
    let log_filename = format!("transaction_log_{}.txt", name);
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_filename)
}

pub fn write_log_entry(
    file: &mut impl Write,
    timestamp: f64,
    endpoint_name: &str,
    signature: &str,
) -> std::io::Result<()> {
    let log_entry = format!(
        "[{:.3}] [{}] {}\n",
        timestamp,
        endpoint_name,
        signature
    );
    file.write_all(log_entry.as_bytes())
}
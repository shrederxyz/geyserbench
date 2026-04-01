use dashmap::{DashMap, DashSet};
use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::Write,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct TransactionData {
    pub wallclock_secs: f64,
    pub elapsed_since_start: Duration,
    pub start_wallclock_secs: f64,
}

#[derive(Debug)]
pub struct Comparator {
    data: DashMap<String, HashMap<String, TransactionData>>,
    emitted: DashSet<String>,
}

impl Comparator {
    pub fn new() -> Self {
        Self {
            data: DashMap::new(),
            emitted: DashSet::new(),
        }
    }

    pub fn add_batch(&self, from: &str, transactions: HashMap<String, TransactionData>) {
        for (signature, data) in transactions {
            let mut entry = self.data.entry(signature).or_default();
            entry.insert(from.to_owned(), data);
        }
    }

    pub fn record_observation(
        &self,
        endpoint: &str,
        signature: &str,
        data: TransactionData,
        expected_producers: usize,
    ) -> Option<HashMap<String, TransactionData>> {
        if expected_producers == 0 {
            return None;
        }

        let mut entry = self.data.entry(signature.to_owned()).or_default();

        let mut updated = false;
        entry
            .entry(endpoint.to_owned())
            .and_modify(|existing| {
                if data.elapsed_since_start < existing.elapsed_since_start {
                    *existing = data.clone();
                    updated = true;
                }
            })
            .or_insert_with(|| {
                updated = true;
                data.clone()
            });

        if !updated {
            return None;
        }

        if entry.len() != expected_producers {
            return None;
        }

        let snapshot = entry.clone();
        drop(entry);

        if self.emitted.insert(signature.to_owned()) {
            Some(snapshot)
        } else {
            None
        }
    }

    pub fn iter(&self) -> dashmap::iter::Iter<'_, String, HashMap<String, TransactionData>> {
        self.data.iter()
    }
}

#[derive(Debug)]
pub struct ProgressTracker {
    target: usize,
    next_checkpoint: AtomicUsize,
}

impl ProgressTracker {
    pub fn new(target: usize) -> Self {
        Self {
            target,
            next_checkpoint: AtomicUsize::new(5),
        }
    }

    pub fn record(&self, current: usize) {
        if self.target == 0 {
            return;
        }

        let percent = (current.saturating_mul(100)) / self.target.max(1);

        loop {
            let next = self.next_checkpoint.load(Ordering::Acquire);
            if next > 100 {
                break;
            }

            if percent < next {
                break;
            }

            if self
                .next_checkpoint
                .compare_exchange(next, next + 5, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let clamped = next.min(100);
                info!(
                    progress = %format!("{}%", clamped),
                    current,
                    target = self.target,
                );
                break;
            }
        }
    }
}

pub fn get_current_timestamp() -> f64 {
    let now = SystemTime::now();
    let since_epoch: Duration = match now.duration_since(UNIX_EPOCH) {
        Ok(d) => d,
        Err(e) => {
            // System clock went backwards; log and clamp to 0
            warn!("SystemTime error (clock skew): {}", e);
            Duration::from_secs(0)
        }
    };
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
    let safe_name = sanitize_filename(name);
    let log_filename = format!("transaction_log_{}.txt", safe_name);
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
    let log_entry = format!("[{:.3}] [{}] {}\n", timestamp, endpoint_name, signature);
    file.write_all(log_entry.as_bytes())
}

fn sanitize_filename(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| match c {
            '\\' | '/' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect();

    let trimmed = sanitized.trim_matches('.');
    if trimmed.is_empty() {
        "endpoint".to_string()
    } else {
        trimmed.to_string()
    }
}

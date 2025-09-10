use std::collections::HashMap;
use crate::utils::{Comparator, percentile};

#[derive(Default)]
pub struct EndpointStats {
    pub first_detections: usize,
    pub total_valid_transactions: usize,
    pub delays: Vec<f64>,
    pub old_transactions: usize,
}

pub fn analyze_delays(comparator: &Comparator, endpoint_names: Vec<String>) {
    let all_signatures = &comparator.data;
    let mut endpoint_stats: HashMap<String, EndpointStats> = HashMap::new();

    for endpoint_name in endpoint_names {
        endpoint_stats.insert(endpoint_name, EndpointStats::default());
    }

    let mut fastest_endpoint = None;
    let mut highest_first_detection_rate = 0.0;

    for sig_data in all_signatures.values() {
        let mut is_historical = false;
        for tx_data in sig_data.values() {
            if tx_data.timestamp < tx_data.start_time {
                is_historical = true;
                break;
            }
        }

        if is_historical {
            for (endpoint, _) in sig_data {
                if let Some(stats) = endpoint_stats.get_mut(endpoint) {
                    stats.old_transactions += 1;
                }
            }
            continue;
        }

        if let Some((first_endpoint, first_tx)) = sig_data
            .iter()
            .min_by(|a, b| a.1.timestamp.partial_cmp(&b.1.timestamp).unwrap())
        {
            if let Some(stats) = endpoint_stats.get_mut(first_endpoint) {
                stats.first_detections += 1;
                stats.total_valid_transactions += 1;
            }

            for (endpoint, tx) in sig_data {
                if endpoint != first_endpoint {
                    if let Some(stats) = endpoint_stats.get_mut(endpoint) {
                        stats
                            .delays
                            .push((tx.timestamp - first_tx.timestamp) * 1000.0);
                        stats.total_valid_transactions += 1;
                    }
                }
            }
        }
    }

    for (endpoint, stats) in &endpoint_stats {
        if stats.total_valid_transactions > 0 {
            let detection_rate =
                stats.first_detections as f64 / stats.total_valid_transactions as f64;
            if detection_rate > highest_first_detection_rate {
                highest_first_detection_rate = detection_rate;
                fastest_endpoint = Some(endpoint.clone());
            }
        }
    }

    println!("\nFinished test results");
    println!("--------------------------------------------");

    if let Some(fastest) = fastest_endpoint.as_ref() {
        let fastest_stats = &endpoint_stats[fastest];
        let win_rate = (fastest_stats.first_detections as f64
            / fastest_stats.total_valid_transactions as f64)
            * 100.0;
        println!(
            "{}: Win rate {:.2}%, avg delay 0.00ms (fastest)",
            fastest, win_rate
        );

        for (endpoint, stats) in &endpoint_stats {
            if endpoint != fastest && stats.total_valid_transactions > 0 {
                let win_rate =
                    (stats.first_detections as f64 / stats.total_valid_transactions as f64) * 100.0;
                let avg_delay = if stats.delays.is_empty() {
                    0.0
                } else {
                    stats.delays.iter().sum::<f64>() / stats.delays.len() as f64
                };

                println!(
                    "{}: Win rate {:.2}%, avg delay {:.2}ms",
                    endpoint, win_rate, avg_delay
                );
            }
        }
    } else {
        println!("Not enough data");
    }

    println!("\nDetailed test results");
    println!("--------------------------------------------");

    if let Some(fastest) = fastest_endpoint {
        println!("\nFastest Endpoint: {}", fastest);
        let fastest_stats = &endpoint_stats[&fastest];
        println!(
            "  First detections: {} out of {} valid transactions ({:.2}%)",
            fastest_stats.first_detections,
            fastest_stats.total_valid_transactions,
            (fastest_stats.first_detections as f64 / fastest_stats.total_valid_transactions as f64)
                * 100.0
        );
        if fastest_stats.old_transactions > 0 {
            println!(
                "  Historical transactions detected: {}",
                fastest_stats.old_transactions
            );
        }

        println!("\nDelays relative to fastest endpoint:");
        for (endpoint, stats) in &endpoint_stats {
            if endpoint != &fastest && !stats.delays.is_empty() {
                let avg_delay = stats.delays.iter().sum::<f64>() / stats.delays.len() as f64;
                let max_delay = stats
                    .delays
                    .iter()
                    .fold(f64::NEG_INFINITY, |a, &b| a.max(b));
                let min_delay = stats.delays.iter().fold(f64::INFINITY, |a, &b| a.min(b));

                let mut sorted_delays = stats.delays.clone();
                sorted_delays.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let p50 = percentile(&sorted_delays, 0.5);
                let p95 = percentile(&sorted_delays, 0.95);

                println!("\n{}:", endpoint);
                println!("  Average delay: {:.2} ms", avg_delay);
                println!("  Median delay: {:.2} ms", p50);
                println!("  95th percentile: {:.2} ms", p95);
                println!("  Min/Max delay: {:.2}/{:.2} ms", min_delay, max_delay);
                println!("  Valid transactions: {}", stats.total_valid_transactions);
                if stats.old_transactions > 0 {
                    println!(
                        "  Historical transactions detected: {}",
                        stats.old_transactions
                    );
                }
            }
        }
    } else {
        println!("Not enough data");
    }
}
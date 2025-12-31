//! Implementation using the metrics crate

use super::Label;

/// Increment a counter metric
pub fn increment_counter(name: &'static str, value: u64, labels: &[Label]) {
    let metric_labels: Vec<metrics::Label> = labels
        .iter()
        .map(|l| metrics::Label::new(l.key, l.value.clone()))
        .collect();
    metrics::counter!(name, &metric_label).increment(value);
}

/// Record a value to a histogram metric
pub fn record_histogram(name: &'static str, value: f64, labels: &[Label]) {
    let metric_labels: Vec<metrics::Label> = labels
        .iter()
        .map(|l| metrics::Label::new(l.key, l.value.clone()))
        .collect();
    metrics::histogram!(name, &metric_labels).record(value);
}

/// Set a gauge metric value
pub fn set_gauge(name: &'static str, value: f64, labels: &[Label]) {
    let metric_labels: Vec<metrics::Label> = labels
        .iter()
        .map(|l| metrics::Label::new(l.key, l.value.clone()))
        .collect();
    metrics::gauge!(name, &metric_labels).set(value);
}

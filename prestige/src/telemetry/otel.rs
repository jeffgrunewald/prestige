//! Implementation using opentelemetry crate
use super::Label;
use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Gauge, Histogram, Meter},
};
use std::{
    collections::HashMap,
    sync::{OnceLock, RwLock},
};

// ====================================================================
// Meter and Instrument Caching
// ====================================================================

static METER: OnceLock<Meter> = OnceLock::new();
static COUNTERS: OnceLock<RwLock<HashMap<&'static str, Counter<u64>>>> = OnceLock::new();
static HISTOGRAMS: OnceLock<RwLock<HashMap<&'static str, Histogram<f64>>>> = OnceLock::new();
static GAUGES: OnceLock<RwLock<HashMap<&'static str, Gauge<f64>>>> = OnceLock::new();

fn get_meter() -> &'static Meter {
    METER.get_or_init(|| global::meter("prestige"))
}

fn to_key_values(labels: &[Label]) -> Vec<KeyValue> {
    labels
        .iter()
        .map(|l| KeyValue::new(l.key, l.value.clone()))
        .collect()
}

// ====================================================================
// Counter Implementation
// ====================================================================

fn get_or_create_counter(name: &'static str) -> Counter<u64> {
    let counters = COUNTERS.get_or_init(|| RwLock::new(HashMap::new()));

    // Fast path; check if counter exists
    if let Ok(guard) = counters.read()
        && let Some(counter) = guard.get(name)
    {
        return counter.clone();
    }

    // Slow path: create counter
    let mut guard = counters.write().unwrap();
    guard
        .entry(name)
        .or_insert_with(|| get_meter().u64_counter(name).build())
        .clone()
}

/// Increment a counter metric
pub fn increment_counter(name: &'static str, value: u64, labels: &[Label]) {
    let counter = get_or_create_counter(name);
    let attrs = to_key_values(labels);
    counter.add(value, &attrs);
}

// ====================================================================
// Histogram Implementation
// ====================================================================

fn get_or_create_histogram(name: &'static str) -> Histogram<f64> {
    let histograms = HISTOGRAMS.get_or_init(|| RwLock::new(HashMap::new()));

    // Fast path: return already existing histogram
    if let Ok(guard) = histograms.read()
        && let Some(histogram) = guard.get(name)
    {
        return histogram.clone();
    }

    // Slow path: create new histogram
    let mut guard = histograms.write().unwrap();
    guard
        .entry(name)
        .or_insert_with(|| get_meter().f64_histogram(name).build())
        .clone()
}

/// Record a value to a histogram metric
pub fn record_histogram(name: &'static str, value: f64, labels: &[Label]) {
    let histogram = get_or_create_histogram(name);
    let attrs = to_key_values(labels);
    histogram.record(value, &attrs);
}

// ====================================================================
// Gauge Implementation
// ====================================================================

fn get_or_create_gauge(name: &'static str) -> Gauge<f64> {
    let gauges = GAUGES.get_or_init(|| RwLock::new(HashMap::new()));

    // Fast path: return already existing gauge
    if let Ok(guard) = gauges.read()
        && let Some(gauge) = guard.get(name)
    {
        return gauge.clone();
    }

    // Slow path: create and return a new gauge
    let mut guard = gauges.write().unwrap();
    guard
        .entry(name)
        .or_insert_with(|| get_meter().f64_gauge(name).build())
        .clone()
}

// Set a gauge metric value
pub fn set_gauge(name: &'static str, value: f64, labels: &[Label]) {
    let gauge = get_or_create_gauge(name);
    let attrs = to_key_values(labels);
    gauge.record(value, &attrs);
}

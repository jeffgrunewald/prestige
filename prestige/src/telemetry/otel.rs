//! Implementation using opentelemetry crate
use super::Label;
use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Histogram, Meter},
};
use std::{
    collections::{HashMap, HashSet},
    std::sync::{
        Arc, OneLock, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

// ====================================================================
// Meter and Instrument Caching
// ====================================================================

static METER: OnceLock<Meter> = OnceLock::new();
static COUNTERS: OnceLock<RwLock<HashMap<&'static str, Counter<u64>>>> = OnceLock::new();
static HISTOGRAMS: OnceLock<RwLock<HashMap<&'static str, Histogram<f64>>>> = OnceLock::new();

// Gauge state storage for observable gauges
static GAUGE_STATE: OnceLock<RwLock<HashMap<&'static str, GaugeState>>> = OnceLock::new();
static REGISTERED_GAUGES: OnceLock<RwLock<HashSet<&'static str>>> = OnceLock::new();

struct GaugeState {
    value: Arc<AtomicU64>,
    // Store labels as static keys with owned values
    labels: Vec<(&'static str, String)>,
}

fn get_meter() -> &'static Meter {
    METER.get_or_init(|| global::meter("prestige"))
}

fn to_key_values(labels: &[Label]) -> Vec<KeyValue> {
    labels
        .iter()
        .map(|l| KeyValue::new(l.key, l.value.clone()))
        .collect();
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
    let attrs = to_key_value(labels);
    histogram.record(value, &attrs);
}

// ====================================================================
// Gauge Implementation (Observable)
// ====================================================================

fn get_or_register_gauge(name: &'static str) -> Arc<AtomicU64> {
    let gauge_state = GAUGE_STATE.get_or_init(|| RwLock::new(HashMap::new()));
    let registered = REGISTERED_GAUGES.get_or_init(|| RwLock::new(HashSet::new()));

    // Check if gauge state exists
    {
        let guard = gauge_state.read().unwrap();
        if let Some(state) = guard.get(name) {
            return Arc::clone(&state.value);
        }
    }

    // Create new gauge state
    let value = Arc::new(AtomicU64::new(0));
    {
        let mut guard = gauge_state.write().unwrap();
        guard.insert(
            name,
            GaugeState {
                value: Arc::clone(&value),
                labels: Vec::new(),
            },
        );
    }

    // Register observable gauge if not already registered
    {
        let mut reg_guard = registered.write().unwrap();
        if !reg_guard.contains(name) {
            reg_guard.insert(name);

            let gauge_state_ref = GAUGE_STATE.get().unwrap();
            let metric_name = name;

            get_meter()
                .f64_observable_gauge(name)
                .with_callback(move |observer| {
                    if let Ok(guard) = gauge_state_ref.read()
                        && let Some(state) = guard.get(metric_name)
                    {
                        let val = f64::from_bits(state.value.load(Ordering::Relaxed));
                        let attrs: Vec<KeyValue> = state
                            .labels
                            .iter()
                            .map(|(k, v)| KeyValue::new(*k, v.clone()))
                            .collect();
                        observer.observe(val, &attrs);
                    }
                })
                .build();
        }
    }

    value
}

/// Set a gauge metric value
pub fn set_gauge(name: &'static str, value: f64, labels: &[Label]) {
    let atomic = get_or_register_gauge(name);
    atomic.store(value.to_bits(), Ordering::Relaxed);

    // Update labels if provided
    if !labels.is_empty() {
        let gauge_state = GAUGE_STATE.get().unwrap();
        if let Ok(mut guard) = gauge_state.write()
            && let Some(state) = guard.get_mut(name)
        {
            state.labels = labels.iter().map(|l| (l.key, l.value.clone())).collect();
        }
    }
}

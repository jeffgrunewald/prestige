# prestige-macros

Proc macros for deriving Prestige parquet trait implementations. This crate is used internally by the [`prestige`](https://crates.io/crates/prestige) crate and re-exported through it — you typically don't need to depend on this crate directly.

## Available Macros

### `#[prestige::prestige_schema]`

The primary attribute macro. Generates all Arrow schema, reader, and writer implementations in one shot. Automatically injects `Serialize` and `Deserialize` derives if not already present.

```rust
#[prestige::prestige_schema]
#[derive(Debug, Clone)]
struct SensorData {
    timestamp: u64,
    sensor_id: String,
    temperature: f32,
}
```

### Individual Derive Macros

For advanced use cases where you need only a subset of functionality:

- `#[derive(ArrowGroup)]` — Schema generation (`arrow_schema()`)
- `#[derive(ArrowReader)]` — Deserialization from Arrow/Parquet (`from_arrow_records()`, `from_arrow_reader()`)
- `#[derive(ArrowWriter)]` — Serialization to Arrow/Parquet (`to_arrow_arrays()`, `write_arrow_file()`, `write_arrow_stream()`)

## Field-Level Attributes

### `#[prestige(as_binary)]`

Encode byte fields as native Arrow binary types instead of the default list representation:

- `[u8; N]` → `FixedSizeBinary(N)` (default without attribute: `FixedSizeList(N, UInt8)`)
- `Vec<u8>` → `Binary` (default without attribute: `List(UInt8)`)

### `#[prestige(identifier)]`

Mark fields as identifier columns (used for deduplication in Iceberg tables). Identifier fields must not be `Option<T>`.

### `#[prestige(partition)]` / `#[prestige(partition(transform))]`

Define Iceberg partition fields. Available transforms:

- `identity` (default when no transform specified)
- `year`, `month`, `day`, `hour`
- `bucket(n)`, `truncate(n)`

### `#[prestige(sort_key)]` / `#[prestige(sort_key(options))]`

Define Iceberg sort order. Options:

- `sort_key` — ascending (default)
- `sort_key(desc)` — descending
- `sort_key(order = N)` — explicit ordering
- `sort_key(desc, order = N)` — descending with explicit order

## Struct-Level Attributes

### `#[prestige(table = "name", namespace = "ns")]`

Set default Iceberg table name and namespace (dot-separated for nested namespaces):

```rust
#[prestige::prestige_schema]
#[prestige(table = "sensor_readings", namespace = "telemetry.prod")]
#[derive(Debug, Clone)]
struct SensorReading {
    #[prestige(identifier)]
    sensor_id: String,

    #[prestige(partition(day), sort_key(order = 1))]
    timestamp: i64,

    #[prestige(sort_key(order = 2))]
    temperature: f64,

    #[prestige(partition)]
    location: String,
}
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](../LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

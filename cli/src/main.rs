use chrono::{DateTime, Utc};
use clap::Parser;
use parquet::basic::Compression;
use std::str::FromStr;

#[cfg(feature = "iceberg")]
use futures::TryStreamExt;

#[derive(Parser)]
#[command(name = "prestige")]
#[command(about = "Prestige CLI for S3 Parquet operations")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Parser)]
enum Commands {
    /// Compact parquet files in S3
    Compact(CompactArgs),

    /// Compact an iceberg table (scan, deduplicate, rewrite)
    #[cfg(feature = "iceberg")]
    IcebergCompact(IcebergCompactArgs),

    /// Scan and display records from an iceberg table
    #[cfg(feature = "iceberg")]
    IcebergScan(IcebergScanArgs),

    /// Display iceberg table metadata (schema, snapshots, properties)
    #[cfg(feature = "iceberg")]
    IcebergInfo(IcebergInfoArgs),
}

#[derive(Parser)]
struct CompactArgs {
    #[arg(long)]
    prefix: String,

    #[arg(long)]
    bucket: String,

    /// Unix timestamp in seconds (exclusive lower bound)
    #[arg(long)]
    start: i64,

    /// Unix timestamp in seconds (inclusive upper bound)
    #[arg(long)]
    end: i64,

    #[arg(long, default_value = "104857600")]
    target_bytes: usize,

    #[arg(long, default_value_t = true)]
    delete_originals: bool,

    #[arg(long)]
    compression: Option<CompressionArg>,

    #[arg(long)]
    row_group_size: Option<usize>,

    #[arg(long, env = "AWS_ACCESS_KEY_ID")]
    access_key_id: Option<String>,

    #[arg(long, env = "AWS_SECRET_ACCESS_KEY")]
    secret_access_key: Option<String>,

    #[arg(long, env = "AWS_REGION")]
    region: Option<String>,

    #[arg(long, env = "AWS_ENDPOINT_URL")]
    endpoint: Option<String>,

    /// Dry-run: output statistics without modifying files
    #[arg(long)]
    plan: bool,

    /// Enable row-level deduplication
    #[arg(long, default_value_t = false)]
    deduplicate: bool,
}

#[derive(Clone, Debug)]
struct CompressionArg(Compression);

impl FromStr for CompressionArg {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let c = match s.to_lowercase().as_str() {
            "snappy" => Compression::SNAPPY,
            "gzip" => Compression::GZIP(Default::default()),
            "lzo" => Compression::LZO,
            "brotli" => Compression::BROTLI(Default::default()),
            "lz4" => Compression::LZ4,
            "zstd" => Compression::ZSTD(Default::default()),
            "uncompressed" => Compression::UNCOMPRESSED,
            _ => anyhow::bail!("Invalid compression: {}", s),
        };
        Ok(CompressionArg(c))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Compact(args) => compact_command(args).await,
        #[cfg(feature = "iceberg")]
        Commands::IcebergCompact(args) => iceberg_compact_command(args).await,
        #[cfg(feature = "iceberg")]
        Commands::IcebergScan(args) => iceberg_scan_command(args).await,
        #[cfg(feature = "iceberg")]
        Commands::IcebergInfo(args) => iceberg_info_command(args).await,
    }
}

async fn compact_command(args: CompactArgs) -> anyhow::Result<()> {
    // Validate and convert timestamps
    let start_ts = DateTime::from_timestamp(args.start, 0)
        .ok_or_else(|| anyhow::anyhow!("Invalid start timestamp: {}", args.start))?;
    let end_ts = DateTime::from_timestamp(args.end, 0)
        .ok_or_else(|| anyhow::anyhow!("Invalid end timestamp: {}", args.end))?;

    if start_ts >= end_ts {
        anyhow::bail!("Start timestamp must be before end timestamp");
    }

    // Create S3 client
    let client = prestige::new_client(
        args.region.clone(),
        args.endpoint.clone(),
        args.access_key_id.clone(),
        args.secret_access_key.clone(),
    )
    .await;

    if args.plan {
        execute_plan(&client, &args, start_ts, end_ts).await?;
    } else {
        execute_compaction(&client, &args, start_ts, end_ts).await?;
    }

    Ok(())
}

async fn execute_compaction(
    client: &prestige::Client,
    args: &CompactArgs,
    start_ts: DateTime<Utc>,
    end_ts: DateTime<Utc>,
) -> anyhow::Result<()> {
    let mut builder = prestige::FileCompactorConfigBuilder::default();

    builder = builder
        .client(client.clone())
        .bucket(args.bucket.clone())
        .prefix(args.prefix.clone())
        .after_timestamp(Some(start_ts))
        .until_timestamp(end_ts)
        .max_bytes_per_file(args.target_bytes)
        .delete_originals(args.delete_originals)
        .enable_deduplication(args.deduplicate);

    if let Some(comp) = &args.compression {
        builder = builder.compression(comp.0);
    }

    if let Some(rg_size) = args.row_group_size {
        builder = builder.row_group_size(rg_size);
    }

    let result = builder.execute_schema_agnostic().await?;

    // Output JSON result
    let output = serde_json::json!({
        "status": "success",
        "files_processed": result.files_processed,
        "files_created": result.files_created,
        "records_consolidated": result.records_consolidated,
        "bytes_saved": result.bytes_saved,
        "duplicate_records_eliminated": result.duplicate_records_eliminated,
        "last_processed_timestamp": result.last_processed_timestamp,
        "deletion_failures": result.deletion_failures,
    });

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

async fn execute_plan(
    client: &prestige::Client,
    args: &CompactArgs,
    start_ts: DateTime<Utc>,
    end_ts: DateTime<Utc>,
) -> anyhow::Result<()> {
    let mut builder = prestige::FileCompactorConfigBuilder::default();

    builder = builder
        .client(client.clone())
        .bucket(args.bucket.clone())
        .prefix(args.prefix.clone())
        .after_timestamp(Some(start_ts))
        .until_timestamp(end_ts)
        .max_bytes_per_file(args.target_bytes)
        .enable_deduplication(args.deduplicate);

    if let Some(comp) = &args.compression {
        builder = builder.compression(comp.0);
    }

    if let Some(rg_size) = args.row_group_size {
        builder = builder.row_group_size(rg_size);
    }

    let result = builder.plan_schema_agnostic().await?;

    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

// ---------------------------------------------------------------------------
// Iceberg CLI subcommands
// ---------------------------------------------------------------------------

#[cfg(feature = "iceberg")]
#[derive(Parser)]
struct IcebergCatalogArgs {
    /// REST catalog URI (e.g. http://localhost:8181)
    #[arg(long, env = "ICEBERG_CATALOG_URI")]
    catalog_uri: String,

    /// Catalog name
    #[arg(long, default_value = "default")]
    catalog_name: String,

    /// Warehouse identifier
    #[arg(long, env = "ICEBERG_WAREHOUSE")]
    warehouse: String,

    /// S3 endpoint override
    #[arg(long, env = "AWS_ENDPOINT_URL")]
    s3_endpoint: Option<String>,

    /// S3 region
    #[arg(long, env = "AWS_REGION")]
    s3_region: Option<String>,

    /// S3 access key
    #[arg(long, env = "AWS_ACCESS_KEY_ID")]
    s3_access_key: Option<String>,

    /// S3 secret key
    #[arg(long, env = "AWS_SECRET_ACCESS_KEY")]
    s3_secret_key: Option<String>,
}

#[cfg(feature = "iceberg")]
#[derive(Parser)]
struct IcebergTableArgs {
    /// Iceberg namespace (e.g. "db" or "db.schema")
    #[arg(long)]
    namespace: String,

    /// Table name
    #[arg(long)]
    table: String,
}

#[cfg(feature = "iceberg")]
#[derive(Parser)]
struct IcebergCompactArgs {
    #[command(flatten)]
    catalog: IcebergCatalogArgs,
    #[command(flatten)]
    table: IcebergTableArgs,

    /// Target file size in bytes (default: 100MB)
    #[arg(long, default_value = "104857600")]
    target_bytes: usize,

    /// Enable row-level deduplication
    #[arg(long, default_value_t = false)]
    deduplicate: bool,

    /// Compression codec
    #[arg(long)]
    compression: Option<CompressionArg>,

    /// Minimum number of files before compaction triggers
    #[arg(long, default_value = "5")]
    min_files: usize,
}

#[cfg(feature = "iceberg")]
#[derive(Parser)]
struct IcebergScanArgs {
    #[command(flatten)]
    catalog: IcebergCatalogArgs,
    #[command(flatten)]
    table: IcebergTableArgs,

    /// Maximum number of records to display
    #[arg(long, default_value = "20")]
    limit: usize,

    /// Specific snapshot ID to scan
    #[arg(long)]
    snapshot_id: Option<i64>,
}

#[cfg(feature = "iceberg")]
#[derive(Parser)]
struct IcebergInfoArgs {
    #[command(flatten)]
    catalog: IcebergCatalogArgs,
    #[command(flatten)]
    table: IcebergTableArgs,
}

#[cfg(feature = "iceberg")]
async fn connect_iceberg(
    args: &IcebergCatalogArgs,
) -> anyhow::Result<std::sync::Arc<dyn iceberg::Catalog>> {
    let mut builder = prestige::iceberg::CatalogConfigBuilder::default();
    builder = builder
        .name(args.catalog_name.clone())
        .uri(args.catalog_uri.clone())
        .warehouse(args.warehouse.clone())
        .s3_endpoint(args.s3_endpoint.clone())
        .s3_region(args.s3_region.clone())
        .s3_access_key_id(args.s3_access_key.clone())
        .s3_secret_access_key(args.s3_secret_key.clone());

    let config = builder.build()?;
    let catalog = prestige::iceberg::connect_catalog(&config).await?;
    Ok(catalog)
}

#[cfg(feature = "iceberg")]
async fn load_iceberg_table(
    catalog: &std::sync::Arc<dyn iceberg::Catalog>,
    args: &IcebergTableArgs,
) -> anyhow::Result<iceberg::table::Table> {
    let ns_parts: Vec<String> = args.namespace.split('.').map(String::from).collect();
    let table = prestige::iceberg::load_table(catalog, &ns_parts, &args.table).await?;
    Ok(table)
}

#[cfg(feature = "iceberg")]
async fn iceberg_compact_command(args: IcebergCompactArgs) -> anyhow::Result<()> {
    let catalog = connect_iceberg(&args.catalog).await?;
    let table = load_iceberg_table(&catalog, &args.table).await?;

    let compression = args
        .compression
        .as_ref()
        .map(|c| c.0)
        .unwrap_or(Compression::SNAPPY);

    let config = prestige::iceberg::IcebergCompactorConfigBuilder::default()
        .table(table)
        .catalog(catalog)
        .target_file_size_bytes(args.target_bytes)
        .min_files_to_compact(args.min_files)
        .deduplicate(args.deduplicate)
        .compression(compression)
        .build()?;

    let result = config.execute().await?;

    let output = serde_json::json!({
        "status": "success",
        "files_read": result.files_read,
        "files_written": result.files_written,
        "records_consolidated": result.records_consolidated,
        "bytes_before": result.bytes_before,
        "bytes_after": result.bytes_after,
        "duplicates_eliminated": result.duplicates_eliminated,
    });

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

#[cfg(feature = "iceberg")]
async fn iceberg_scan_command(args: IcebergScanArgs) -> anyhow::Result<()> {
    let catalog = connect_iceberg(&args.catalog).await?;
    let table = load_iceberg_table(&catalog, &args.table).await?;

    let stream = match args.snapshot_id {
        Some(sid) => prestige::iceberg::scan_snapshot(&table, sid).await?,
        None => prestige::iceberg::scan_table(&table).await?,
    };

    let mut pinned = std::pin::pin!(stream);
    let mut total_rows = 0usize;

    while let Some(batch) = pinned.try_next().await? {
        let remaining = args.limit.saturating_sub(total_rows);
        if remaining == 0 {
            break;
        }

        let num_rows = batch.num_rows();
        let display_batch = if num_rows > remaining {
            batch.slice(0, remaining)
        } else {
            batch
        };

        println!(
            "{}",
            arrow::util::pretty::pretty_format_batches(&[display_batch])?
        );

        total_rows += num_rows;
    }

    println!("\n({total_rows} rows scanned, limit {limit})", limit = args.limit);
    Ok(())
}

#[cfg(feature = "iceberg")]
async fn iceberg_info_command(args: IcebergInfoArgs) -> anyhow::Result<()> {
    let catalog = connect_iceberg(&args.catalog).await?;
    let table = load_iceberg_table(&catalog, &args.table).await?;

    let metadata = table.metadata();

    // Schema
    let schema = metadata.current_schema();
    println!("Table: {}", table.identifier());
    println!("Location: {}", metadata.location());
    println!("Format version: {:?}", metadata.format_version());
    println!();

    println!("Schema (id={}):", schema.schema_id());
    for field in schema.as_struct().fields() {
        let nullable = if field.required { "NOT NULL" } else { "NULL" };
        println!("  {} {} {}", field.name, field.field_type, nullable);
    }
    println!();

    // Partition spec
    let partition_spec = metadata.default_partition_spec();
    if partition_spec.is_unpartitioned() {
        println!("Partition spec: unpartitioned");
    } else {
        println!("Partition spec:");
        for field in partition_spec.fields() {
            println!(
                "  {} ({:?}, source_id={})",
                field.name, field.transform, field.source_id
            );
        }
    }
    println!();

    // Snapshots
    let snapshots: Vec<_> = metadata.snapshots().collect();
    println!("Snapshots ({}):", snapshots.len());
    for snap in &snapshots {
        let ts = chrono::DateTime::from_timestamp_millis(snap.timestamp_ms())
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_else(|| "unknown".to_string());
        println!(
            "  id={} timestamp={} parent={:?}",
            snap.snapshot_id(),
            ts,
            snap.parent_snapshot_id()
        );
    }
    println!();

    // Properties
    let props = metadata.properties();
    if !props.is_empty() {
        println!("Properties ({}):", props.len());
        for (k, v) in props {
            println!("  {k} = {v}");
        }
    }

    Ok(())
}

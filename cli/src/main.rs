use chrono::{DateTime, Utc};
use clap::Parser;
use parquet::basic::Compression;
use std::str::FromStr;

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

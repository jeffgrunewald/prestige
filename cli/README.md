# Prestige CLI

Command-line interface for S3 Parquet file operations.

## Installation

Build from source:

```bash
cargo build --release --package prestige-cli
```

The binary will be available at `target/release/prestige`.

## Commands

### compact

Consolidate small parquet files in S3 into larger files.

#### Basic Usage

```bash
prestige compact \
  --bucket my-bucket \
  --prefix sensor_data \
  --start 1704067200 \
  --end 1704153600 \
  --target-bytes 104857600
```

#### Options

- `--bucket <BUCKET>` - S3 bucket name (required)
- `--prefix <PREFIX>` - File prefix to compact (required)
- `--start <START>` - Unix timestamp in seconds, exclusive lower bound (required)
- `--end <END>` - Unix timestamp in seconds, inclusive upper bound (required)
- `--target-bytes <BYTES>` - Target size per output file in bytes (default: 104857600 = 100MB)
- `--delete-originals` - Delete original files after successful compaction (default: true)
- `--compression <TYPE>` - Compression algorithm: snappy, gzip, lzo, brotli, lz4, zstd, uncompressed (default: snappy)
- `--row-group-size <SIZE>` - Parquet row group size (default: 10000)
- `--deduplicate` - Enable row-level deduplication (default: false)
- `--plan` - Dry-run mode: show statistics without modifying files

#### Authentication

AWS credentials can be provided via:

1. Command-line arguments:
   ```bash
   prestige compact \
     --access-key-id AKIAXXXXXXXX \
     --secret-access-key XXXXXXXX \
     --region us-east-1 \
     ...
   ```

2. Environment variables:
   ```bash
   export AWS_ACCESS_KEY_ID=AKIAXXXXXXXX
   export AWS_SECRET_ACCESS_KEY=XXXXXXXX
   export AWS_REGION=us-east-1
   prestige compact ...
   ```

3. AWS credentials file (`~/.aws/credentials`)

#### Plan Mode

Use `--plan` to estimate compaction results without modifying files:

```bash
prestige compact \
  --bucket my-bucket \
  --prefix sensor_data \
  --start 1704067200 \
  --end 1704153600 \
  --plan
```

Output:
```json
{
  "compacted_files_produced": 5,
  "uncompacted_files_deleted": 127,
  "records_processed": 1234567,
  "duplicate_records_eliminated": 0,
  "storage_saved_bytes": 45678900
}
```

#### Deduplication

Enable row-level deduplication to eliminate duplicate records:

```bash
prestige compact \
  --bucket my-bucket \
  --prefix sensor_data \
  --start 1704067200 \
  --end 1704153600 \
  --deduplicate
```

Output:
```json
{
  "status": "success",
  "files_processed": 127,
  "files_created": 5,
  "records_consolidated": 1234567,
  "bytes_saved": 45678900,
  "duplicate_records_eliminated": 61728,
  "last_processed_timestamp": "2024-01-02T00:00:00Z",
  "deletion_failures": []
}
```

**Note:** Deduplication uses row hashing and may increase processing time and memory usage proportional to the number of unique records.

#### LocalStack Testing

For local S3 testing with LocalStack:

```bash
docker run -d -p 4566:4566 localstack/localstack

prestige compact \
  --bucket test-bucket \
  --prefix data \
  --start 1704067200 \
  --end 1704153600 \
  --endpoint http://localhost:4566 \
  --region us-east-1
```

### iceberg-compact

Compact an Iceberg table by rewriting small files into larger, sorted files. Requires the `iceberg` feature flag.

```bash
cargo build --release --package prestige-cli --features iceberg
```

#### Basic Usage

```bash
prestige iceberg-compact \
  --catalog-uri http://localhost:8181 \
  --warehouse s3://my-warehouse \
  --namespace telemetry \
  --table sensor_readings \
  --target-bytes 134217728 \
  --min-files 5
```

#### Options

- `--catalog-uri <URI>` - REST catalog URI (env: `ICEBERG_CATALOG_URI`) (required)
- `--catalog-name <NAME>` - Catalog name (default: "default")
- `--warehouse <WAREHOUSE>` - Warehouse identifier (env: `ICEBERG_WAREHOUSE`) (required)
- `--namespace <NAMESPACE>` - Iceberg namespace, dot-separated (required)
- `--table <TABLE>` - Table name (required)
- `--target-bytes <BYTES>` - Target file size in bytes (default: 104857600 = 100MB)
- `--deduplicate` - Enable row-level deduplication by identifier fields (default: false)
- `--min-files <N>` - Minimum number of files before compaction triggers (default: 5)
- `--compression <TYPE>` - Compression algorithm: snappy, gzip, lzo, brotli, lz4, zstd, uncompressed
- `--s3-endpoint <URL>` - S3 endpoint override (env: `AWS_ENDPOINT_URL`)
- `--s3-region <REGION>` - S3 region (env: `AWS_REGION`)
- `--s3-access-key <KEY>` - S3 access key (env: `AWS_ACCESS_KEY_ID`)
- `--s3-secret-key <KEY>` - S3 secret key (env: `AWS_SECRET_ACCESS_KEY`)

### iceberg-scan

Scan and display records from an Iceberg table.

#### Basic Usage

```bash
prestige iceberg-scan \
  --catalog-uri http://localhost:8181 \
  --warehouse s3://my-warehouse \
  --namespace telemetry \
  --table sensor_readings \
  --limit 50
```

#### Options

- `--catalog-uri <URI>` - REST catalog URI (env: `ICEBERG_CATALOG_URI`) (required)
- `--catalog-name <NAME>` - Catalog name (default: "default")
- `--warehouse <WAREHOUSE>` - Warehouse identifier (env: `ICEBERG_WAREHOUSE`) (required)
- `--namespace <NAMESPACE>` - Iceberg namespace, dot-separated (required)
- `--table <TABLE>` - Table name (required)
- `--limit <N>` - Maximum number of records to display (default: 20)
- `--snapshot-id <ID>` - Scan a specific snapshot (time travel)
- `--filter <EXPR>` - Row filter expression (repeatable, ANDed together). Format: `"column op value"` where op is `=`, `!=`, `>`, `>=`, `<`, or `<=`
- S3/catalog connection options (same as iceberg-compact)

#### Filter Examples

```bash
prestige iceberg-scan \
  --catalog-uri http://localhost:8181 \
  --warehouse s3://my-warehouse \
  --namespace telemetry \
  --table sensor_readings \
  --filter "temperature > 100.0" \
  --filter "location = us-east-1"
```

### iceberg-info

Display Iceberg table metadata including schema, partition spec, snapshots, and properties.

```bash
prestige iceberg-info \
  --catalog-uri http://localhost:8181 \
  --warehouse s3://my-warehouse \
  --namespace telemetry \
  --table sensor_readings
```

Connection options are the same as iceberg-compact and iceberg-scan.

## Examples

### Compact last hour of data

```bash
START=$(date -u -d '1 hour ago' +%s)
END=$(date -u +%s)

prestige compact \
  --bucket production-data \
  --prefix events \
  --start $START \
  --end $END \
  --target-bytes 209715200  # 200MB
```

### Compact with maximum compression

```bash
prestige compact \
  --bucket archive-data \
  --prefix historical \
  --start 1704067200 \
  --end 1704153600 \
  --compression zstd \
  --target-bytes 524288000  # 500MB
```

### Compact without deleting originals

```bash
prestige compact \
  --bucket backup-data \
  --prefix logs \
  --start 1704067200 \
  --end 1704153600 \
  --delete-originals=false
```

## File Naming Convention

The compactor follows these naming conventions:

- **Original files**: `{prefix}.{timestamp_millis}.parquet`
- **Compacted files**: `{prefix}.{timestamp_millis}.c.parquet`
- **Processed markers**: `{prefix}.{timestamp_millis}.parquet.processed`

## Idempotency

The compactor is idempotent:

1. After successful compaction, `.processed` markers are created for source files
2. Subsequent runs skip files that have `.processed` markers
3. If deletion fails, the marker prevents reprocessing
4. Use `last_processed_timestamp` for checkpoint-based processing

## Error Handling

- Deletion failures are tracked in `deletion_failures` but don't fail the operation
- Schema mismatches cause files to be skipped
- Empty or invalid parquet files are skipped
- All errors are logged to stderr

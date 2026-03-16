use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use iceberg::{
    ErrorKind,
    spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, PrimitiveLiteral,
        Struct,
    },
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::error::Result;

/// Lightweight local manifest that records S3 data file paths written since
/// the last successful catalog commit. Provides crash recovery without
/// buffering raw data to disk.
pub(crate) struct CommitManifest {
    path: PathBuf,
}

/// Serializable proxy for `DataFile` — the iceberg crate's `DataFile` doesn't
/// implement `Serialize`/`Deserialize`, so we capture the fields needed to
/// reconstruct one for `fast_append`.
#[derive(Serialize, Deserialize)]
struct PendingDataFile {
    file_path: String,
    record_count: u64,
    file_size_in_bytes: u64,
    partition_spec_id: i32,
    partition_values: Vec<Option<PendingLiteral>>,
}

/// Serializable subset of `iceberg::spec::Literal` for partition values.
/// Partition values are always primitive types.
#[derive(Serialize, Deserialize)]
enum PendingLiteral {
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(String),
    Binary(Vec<u8>),
    Int128(i128),
    UInt128(u128),
}

impl PendingDataFile {
    fn from_data_file(df: &DataFile, partition_spec_id: i32) -> Self {
        let partition_values = df
            .partition()
            .iter()
            .map(|opt| opt.and_then(PendingLiteral::from_literal))
            .collect();

        Self {
            file_path: df.file_path().to_string(),
            record_count: df.record_count(),
            file_size_in_bytes: df.file_size_in_bytes(),
            partition_spec_id,
            partition_values,
        }
    }

    fn into_data_file(self) -> std::result::Result<DataFile, String> {
        let partition = Struct::from_iter(
            self.partition_values
                .into_iter()
                .map(|opt| opt.map(PendingLiteral::into_literal)),
        );

        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(self.file_path)
            .file_format(DataFileFormat::Parquet)
            .partition(partition)
            .partition_spec_id(self.partition_spec_id)
            .record_count(self.record_count)
            .file_size_in_bytes(self.file_size_in_bytes)
            .build()
            .map_err(|e| e.to_string())
    }
}

impl PendingLiteral {
    fn from_literal(lit: &Literal) -> Option<Self> {
        let prim = lit.as_primitive_literal()?;
        Some(match prim {
            PrimitiveLiteral::Boolean(v) => Self::Boolean(v),
            PrimitiveLiteral::Int(v) => Self::Int(v),
            PrimitiveLiteral::Long(v) => Self::Long(v),
            PrimitiveLiteral::Float(v) => Self::Float(v.0),
            PrimitiveLiteral::Double(v) => Self::Double(v.0),
            PrimitiveLiteral::String(v) => Self::String(v),
            PrimitiveLiteral::Binary(v) => Self::Binary(v),
            PrimitiveLiteral::Int128(v) => Self::Int128(v),
            PrimitiveLiteral::UInt128(v) => Self::UInt128(v),
            PrimitiveLiteral::AboveMax | PrimitiveLiteral::BelowMin => return None,
        })
    }

    fn into_literal(self) -> Literal {
        match self {
            Self::Boolean(v) => Literal::bool(v),
            Self::Int(v) => Literal::int(v),
            Self::Long(v) => Literal::long(v),
            Self::Float(v) => Literal::float(v),
            Self::Double(v) => Literal::double(v),
            Self::String(v) => Literal::string(v),
            Self::Binary(v) => Literal::binary(v),
            Self::Int128(v) => Literal::decimal(v),
            Self::UInt128(v) => Literal::uuid(uuid::Uuid::from_u128(v)),
        }
    }
}

impl CommitManifest {
    pub(crate) fn new(dir: &Path, label: &str) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        let path = dir.join(format!("{label}.manifest"));
        Ok(Self { path })
    }

    /// Append data file metadata as NDJSON and fsync.
    pub(crate) fn record_files(
        &self,
        data_files: &[DataFile],
        partition_spec_id: i32,
    ) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;

        for df in data_files {
            let pending = PendingDataFile::from_data_file(df, partition_spec_id);
            let line = serde_json::to_string(&pending).map_err(io::Error::other)?;
            writeln!(file, "{line}")?;
        }

        file.sync_all()?;
        Ok(())
    }

    /// Read and parse all pending entries. Returns empty vec if file doesn't
    /// exist or is empty.
    pub(crate) fn pending(&self) -> Result<Vec<DataFile>> {
        if !self.has_pending() {
            return Ok(Vec::new());
        }

        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut data_files = Vec::new();

        for line in reader.lines() {
            let line = line?;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let pending: PendingDataFile = serde_json::from_str(trimmed)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let df = pending
                .into_data_file()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            data_files.push(df);
        }

        Ok(data_files)
    }

    /// Check if manifest file exists and is non-empty.
    pub(crate) fn has_pending(&self) -> bool {
        self.path.metadata().map(|m| m.len() > 0).unwrap_or(false)
    }

    /// Delete the manifest file entirely.
    pub(crate) fn delete(self) -> io::Result<()> {
        if self.path.exists() {
            fs::remove_file(&self.path)?;
        }
        Ok(())
    }

    /// Find all manifest files in `dir` whose name starts with `{label}-`
    /// and ends with `.manifest`. Used at startup to discover per-commit
    /// manifests left behind by the pipelined commit system.
    pub(crate) fn scan_pending(dir: &Path, label: &str) -> io::Result<Vec<Self>> {
        let prefix = format!("{label}-");
        let mut manifests = Vec::new();

        if !dir.exists() {
            return Ok(manifests);
        }

        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with(&prefix) && name_str.ends_with(".manifest") {
                manifests.push(Self { path: entry.path() });
            }
        }

        Ok(manifests)
    }
}

/// Returns true if an iceberg error indicates that the data files were
/// already committed — either a catalog commit conflict (optimistic
/// concurrency race) or a duplicate file validation failure.
fn is_iceberg_already_committed(err: &iceberg::Error) -> bool {
    match err.kind() {
        // Catalog rejected the commit because metadata was updated between
        // our read and write (another commit landed first).
        ErrorKind::CatalogCommitConflicts => true,
        // Duplicate file check found the files already present in a live
        // manifest entry. The error message from iceberg contains
        // "already referenced by table".
        ErrorKind::DataInvalid => {
            let msg = err.to_string();
            msg.contains("already referenced")
        }
        _ => false,
    }
}

/// Returns true if a prestige error indicates that the data files were
/// already committed. Covers both iceberg-level errors (duplicate files,
/// commit conflicts via `Transaction`) and prestige-level errors (409
/// from the REST endpoint used by WAP branch commits).
pub(crate) fn is_already_committed(err: &crate::Error) -> bool {
    match err {
        crate::Error::Iceberg(iceberg_err) => is_iceberg_already_committed(iceberg_err),
        crate::Error::CatalogHttp(msg) => msg.contains("commit conflict"),
        _ => false,
    }
}

/// Attempt to recover and commit data files recorded in the manifest from a
/// previous crash. Returns `Some(updated_table)` if recovery committed new
/// files, `None` if nothing to recover or files were already committed.
pub(crate) async fn recover_pending_commit(
    manifest: CommitManifest,
    table: &Table,
    catalog: &dyn iceberg::Catalog,
) -> Result<Option<Table>> {
    let pending_files = manifest.pending()?;
    if pending_files.is_empty() {
        manifest.delete()?;
        return Ok(None);
    }

    let file_count = pending_files.len();
    info!(
        files = file_count,
        "recovering pending commit from local manifest"
    );

    // Attempt commit with duplicate check disabled — files may already be
    // committed if the previous process crashed after catalog commit but
    // before manifest delete.
    let tx = Transaction::new(table);
    let action = tx
        .fast_append()
        .with_check_duplicate(false)
        .add_data_files(pending_files);
    let tx = action.apply(tx)?;

    match tx.commit(catalog).await {
        Ok(updated_table) => {
            manifest.delete()?;
            info!(files = file_count, "recovery commit succeeded");
            Ok(Some(updated_table))
        }
        Err(err) if is_iceberg_already_committed(&err) => {
            info!(
                files = file_count,
                "pending files already committed, deleting manifest"
            );
            manifest.delete()?;
            Ok(None)
        }
        Err(err) => Err(err.into()),
    }
}

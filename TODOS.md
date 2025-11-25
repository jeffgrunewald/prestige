# Prestige TODO List

This document tracks remaining work for the prestige parquet file store library. All core functionality (Phases 1-4) has been implemented and tested.

## Current Status

✅ **Core Implementation Complete**:
- File metadata handling (file_meta.rs)
- Parquet writer with rotation (file_sink.rs)
- S3 upload with retry (file_upload.rs)
- Parquet reader (file_source.rs)
- S3 poller with state management (file_poller.rs)
- PostgreSQL state tracking (feature-gated)
- Derive macros for parquet serialization
- Crash recovery
- Metrics tracking
- 36 unit tests passing

---

## Testing & Validation

### Integration Tests
- [ ] MinIO/LocalStack S3 integration tests
  - [ ] Write records → upload → verify in MinIO
  - [ ] List files from MinIO
  - [ ] Download and read files from MinIO
- [ ] PostgreSQL integration tests
  - [ ] FilePoller with real PostgreSQL
  - [ ] State tracking across restarts
  - [ ] Deduplication with database
  - [ ] Cleanup removes old records
- [ ] End-to-end pipeline test
  - [ ] Write → upload → poll → process workflow
  - [ ] Verify atomicity with transactions
  - [ ] Test crash scenarios

### Performance Testing
- [ ] Write throughput benchmarks
  - [ ] Measure records/sec (target: 100k+)
  - [ ] Test with different record sizes
  - [ ] Memory usage profiling
- [ ] Read throughput benchmarks
  - [ ] Measure records/sec (target: 500k+)
  - [ ] Compare sequential vs parallel reading
  - [ ] Memory usage profiling
- [ ] Compression benchmarks
  - [ ] SNAPPY vs ZSTD vs LZ4 performance
  - [ ] Compression ratio comparison
  - [ ] CPU usage comparison
- [ ] Configuration tuning
  - [ ] Optimize row group sizes
  - [ ] Optimize batch sizes
  - [ ] Optimize buffer sizes

---

## Documentation

### API Documentation
- [ ] Comprehensive rustdoc for all public APIs
- [ ] Module-level documentation
- [ ] Usage examples in doc comments
- [ ] Document error conditions
- [ ] Document performance characteristics

### Guides
- [ ] Getting Started guide
  - [ ] Installation
  - [ ] Basic usage examples
  - [ ] Common patterns
- [ ] Configuration guide
  - [ ] All configuration options
  - [ ] Performance tuning
  - [ ] Best practices
- [ ] Migration guide from protobuf file_store
  - [ ] API differences
  - [ ] Type system changes
  - [ ] Feature comparison
- [ ] State Management guide
  - [ ] Using PostgreSQL state
  - [ ] Implementing custom state backends
  - [ ] Trade-offs and considerations
- [ ] Troubleshooting guide
  - [ ] Common errors
  - [ ] Debugging techniques
  - [ ] Performance issues

### Examples
- [ ] Basic write example
- [ ] Basic read example
- [ ] File poller example
- [ ] Custom state implementation example
- [ ] End-to-end pipeline example

---

## Production Readiness

### Observability
- [ ] Structured logging with tracing
- [ ] Distributed tracing (OpenTelemetry)
- [ ] Additional metrics
  - [ ] Polling lag metrics
  - [ ] Files processed/sec
  - [ ] Cache hit rate
  - [ ] Queue depth metrics
- [ ] Health check endpoints
  - [ ] Sink health
  - [ ] Poller health
  - [ ] Database connectivity

### Operations
- [ ] Graceful shutdown improvements
  - [ ] Drain in-flight operations
  - [ ] Configurable shutdown timeout
- [ ] Resource limits
  - [ ] Configurable memory limits
  - [ ] Backpressure handling
- [ ] Retry policies
  - [ ] Configurable retry strategies
  - [ ] Exponential backoff options

---

## Future Enhancements

### Features
- [ ] Schema evolution support
  - [ ] Automatic schema versioning
  - [ ] Backward compatibility
- [ ] Partitioned writes
  - [ ] Hive-style partitioning
  - [ ] Date-based partitioning
- [ ] Encryption support
  - [ ] Client-side encryption
  - [ ] S3 SSE integration
- [ ] Additional formats
  - [ ] Delta Lake format
  - [ ] Apache Iceberg format
  - [ ] Arrow IPC streaming

### Performance
- [ ] Zero-copy optimizations
  - [ ] Arrow IPC for in-memory transfers
  - [ ] Memory mapping for reads
- [ ] Streaming improvements
  - [ ] Range requests for S3 (Option B)
  - [ ] Parallel file downloads
- [ ] Compression improvements
  - [ ] Per-column compression
  - [ ] Compression level tuning

---

## Notes

- This is a living document - update as work progresses
- Core functionality is production-ready
- Most remaining items are enhancements and validation
- Integration/performance tests can be added as needed
- Documentation should be prioritized for external adoption

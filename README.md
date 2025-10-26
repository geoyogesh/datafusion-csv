# datafusion-csv

An independent CSV file format integration for [Apache DataFusion](https://arrow.apache.org/datafusion/), providing native CSV parsing with automatic type inference.

## Features

- **Independent CSV Reader**: Native implementation using the `csv` crate for parsing
- **Batch-Level Streaming**: Processes records incrementally - only one batch (8,192 rows) held in memory at a time
- **Automatic Type Inference**: Intelligently detects column types (Int64, Float64, Boolean, Utf8)
- **Remote Object Store Support**: Read CSV files from HTTP/HTTPS URLs (S3, R2, Azure, etc.)
- **DataFusion Integration**: Seamless integration with DataFusion's query engine
- **Flexible Configuration**: Support for custom delimiters, headers, batch sizes
- **High Performance**: Direct conversion to Arrow RecordBatches with minimal allocations
- **Memory Efficient**: Batch streaming reduces memory pressure for large result sets
- **SQL Support**: Query CSV files using SQL through DataFusion
- **DataFrame API**: Use DataFusion's DataFrame API for programmatic queries

## Quick Start

```rust
use datafusion::prelude::*;
use datafusion_csv::SessionContextCsvExt;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register a CSV file
    ctx.register_csv_file("users", "data/users.csv").await?;

    // Query with SQL
    let df = ctx.sql("SELECT * FROM users WHERE country = 'USA'").await?;
    df.show().await?;

    Ok(())
}
```

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
datafusion = "43.0.0"
datafusion-csv = "0.1.0"
tokio = { version = "1.0", features = ["macros", "rt-multi-thread"] }
```

## Key Capabilities

### Automatic Type Detection

The CSV reader automatically infers column types by sampling data:

```rust
// CSV with mixed types
// id,name,age,active
// 1,Alice,30,true
// 2,Bob,25,false

// Automatically inferred as:
// id: Int64
// name: Utf8
// age: Int64
// active: Boolean
```

### Custom Delimiters

Support for TSV and other delimited formats:

```rust
// Read a TSV file
ctx.register_csv_with_delimiter("sales", "data/sales.tsv", b'\t').await?;

// Read semicolon-delimited CSV
ctx.register_csv_with_delimiter("data", "data.csv", b';').await?;
```

### Advanced Configuration

```rust
use datafusion_csv::CsvFormatOptions;

let options = CsvFormatOptions::new()
    .with_delimiter(b';')
    .with_has_header(false)
    .with_batch_size(16384)
    .with_schema_infer_max_rec(Some(5000));

ctx.register_csv_with_options("data", "file.csv", options).await?;
```

### Remote Object Store Support

Read CSV files from remote HTTP/HTTPS sources (S3, R2, Azure, etc.):

```rust
// Read from HTTP/HTTPS URL
ctx.register_csv_file(
    "remote_data",
    "https://your-bucket.r2.dev/data.csv"
).await?;

// Query remote CSV files
let df = ctx.sql("SELECT * FROM remote_data").await?;
df.show().await?;

// Join local and remote data
ctx.register_csv_file("local_users", "data/users.csv").await?;
ctx.register_csv_file("remote_orders", "https://bucket.com/orders.csv").await?;

let df = ctx.sql(r#"
    SELECT u.name, o.total
    FROM local_users u
    JOIN remote_orders o ON u.id = o.user_id
"#).await?;
```

The HTTP object store is automatically registered when using HTTP/HTTPS URLs.

## Architecture

This implementation provides:

- **`CsvFormat`**: Implements DataFusion's `FileFormat` trait for CSV files
- **`CsvExec`**: Custom execution plan for reading CSV data
- **`CsvOpener`**: Implements `FileOpener` for async file reading and parsing
- **`CsvStream`**: Streaming implementation that yields RecordBatches incrementally
- **Independent Schema Inference**: Samples CSV data to detect column types
- **Direct Arrow Conversion**: Converts CSV strings to typed Arrow arrays

### Batch-Level Streaming

The CSV reader uses a batch-level streaming architecture:

```
┌─────────────┐
│ Object Store│ → get().bytes() → ┌──────────────┐
└─────────────┘                   │ File in RAM  │
                                  └──────┬───────┘
                                         │
                                         ↓
                                  ┌─────────────┐
                                  │ CsvStream   │ (Stream impl)
                                  └──────┬──────┘
                                         │ poll_next()
                                         ├→ RecordBatch₁ (8,192 rows)
                                         ├→ RecordBatch₂ (8,192 rows)
                                         └→ ...
```

**Memory Profile:**
- File loaded once into memory (unavoidable with csv crate)
- Only one RecordBatch active at a time during processing
- For a 1GB CSV: ~1.01GB peak (file + 1 batch) vs ~2GB (file + all batches)

See [STREAMING_ARCHITECTURE.md](STREAMING_ARCHITECTURE.md) for detailed analysis.

## Documentation

- [User Guide](USER_GUIDE.md) - Comprehensive usage guide with examples
- [Development Guide](DEVELOPMENT.md) - Architecture and contribution guidelines
- [Streaming Architecture](STREAMING_ARCHITECTURE.md) - Deep dive into memory and streaming
- [E2E Tests](tests/e2e_test.rs) - Real-world usage examples

## Examples

### Basic Query

```rust
ctx.register_csv_file("employees", "data/employees.csv").await?;
let df = ctx.sql("SELECT name, salary FROM employees WHERE dept = 'Engineering'").await?;
df.show().await?;
```

### Joins and Aggregations

```rust
ctx.register_csv_file("orders", "data/orders.csv").await?;
ctx.register_csv_file("customers", "data/customers.csv").await?;

let df = ctx.sql(r#"
    SELECT c.name, SUM(o.amount) as total
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    GROUP BY c.name
    ORDER BY total DESC
"#).await?;
```

### DataFrame API

```rust
let df = ctx.read_csv_file("data/sales.csv").await?;
let result = df
    .filter(col("revenue").gt(lit(1000)))?
    .select(vec![col("region"), col("revenue")])?
    .aggregate(
        vec![col("region")],
        vec![sum(col("revenue")).alias("total")],
    )?;
result.show().await?;
```

## Testing

Run the test suite:

```bash
# All tests (local files only)
cargo test

# Unit tests only
cargo test --lib

# End-to-end tests
cargo test --test e2e_test

# Specific test
cargo test test_join_users_and_orders

# Run remote object store tests (requires CSV files to be uploaded)
cargo test -- --ignored
```

All 30 tests passing (8 unit tests + 16 e2e tests + 6 doc tests), including 5 remote object store tests reading from Cloudflare R2.

## Performance

- **Batch Streaming**: Only one RecordBatch in memory during query execution (default: 8,192 rows)
- **Configurable Batch Sizes**: Tune for your workload (larger for analytics, smaller for memory-constrained)
- **Parallel Execution**: Leverages DataFusion's partition-based parallelism
- **Memory Efficient**: Batch streaming significantly reduces memory vs. loading all rows
- **Type-Specific Arrays**: Direct conversion to typed Arrow arrays without intermediate copies
- **Minimal Allocations**: Reuses buffers where possible

### Memory Characteristics

For typical workloads:
- **File Size**: Loaded once into memory
- **Active Batch**: ~10MB for 8,192 rows with 10 columns
- **Query Processing**: DataFusion streams batches, only 1-2 batches active

**Example**: 1GB CSV file with 10M rows
- Without streaming: ~2GB (file + all parsed batches)
- With batch streaming: ~1.01GB (file + current batch)
- Reduction: ~50% memory savings during execution

## Supported Types

- `Int64` - Integer numbers
- `Float64` - Floating point numbers
- `Boolean` - true/false values
- `Utf8` - String data (default fallback)

## Contributing

Contributions are welcome! Please see [DEVELOPMENT.md](DEVELOPMENT.md) for:

- Architecture overview
- Module descriptions
- Testing guidelines
- Development workflow

## License

MIT License - see [LICENSE](LICENSE) for details

## Related Projects

- [Apache Arrow](https://arrow.apache.org/) - Columnar memory format
- [Apache DataFusion](https://arrow.apache.org/datafusion/) - Query engine
- [csv crate](https://github.com/BurntSushi/rust-csv) - CSV parsing library

## Acknowledgments

This implementation follows the architectural patterns established by `datafusion-orc` and other DataFusion file format integrations.

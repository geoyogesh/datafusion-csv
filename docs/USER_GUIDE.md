# DataFusion CSV - User Guide

Welcome to the DataFusion CSV User Guide! This guide will help you get started with reading and querying CSV files using Apache DataFusion.

**Note**: This is an independent CSV reader implementation for DataFusion. It provides:
- Native CSV parsing with automatic type inference
- Batch-level streaming (only one batch in memory at a time during processing)
- Direct conversion to Arrow RecordBatches without relying on DataFusion's built-in CSV support

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Basic Usage](#basic-usage)
- [Advanced Features](#advanced-features)
- [Configuration Options](#configuration-options)
- [Examples](#examples)
- [Troubleshooting](#troubleshooting)

## Installation

Add `datafusion-csv` to your `Cargo.toml`:

```toml
[dependencies]
datafusion = "43.0.0"
datafusion-csv = "0.1.0"
tokio = { version = "1.0", features = ["macros", "rt-multi-thread"] }
```

## Quick Start

Here's a simple example to get you started:

```rust
use datafusion::prelude::*;
use datafusion_csv::SessionContextCsvExt;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register a CSV file as a table
    ctx.register_csv_file("users", "data/users.csv").await?;

    // Query the CSV file using SQL
    let df = ctx.sql("SELECT * FROM users WHERE country = 'USA'").await?;
    df.show().await?;

    Ok(())
}
```

## Basic Usage

### Registering CSV Files

#### Register with Default Options

```rust
use datafusion::prelude::*;
use datafusion_csv::SessionContextCsvExt;

let ctx = SessionContext::new();
ctx.register_csv_file("my_table", "path/to/file.csv").await?;
```

#### Register with Custom Delimiter

Perfect for TSV files or other delimited formats:

```rust
// Register a TSV file
ctx.register_csv_with_delimiter("sales", "data/sales.tsv", b'\t').await?;

// Register a semicolon-delimited file
ctx.register_csv_with_delimiter("data", "data.csv", b';').await?;
```

#### Register with Custom Options

For more control over CSV parsing:

```rust
use datafusion_csv::CsvFormatOptions;

let options = CsvFormatOptions::new()
    .with_delimiter(b';')
    .with_has_header(false)
    .with_batch_size(1024);

ctx.register_csv_with_options("data", "file.csv", options).await?;
```

### Reading CSV Files

#### Read into DataFrame

```rust
use datafusion::prelude::*;
use datafusion_csv::SessionContextCsvExt;

let ctx = SessionContext::new();

// Read CSV directly into a DataFrame
let df = ctx.read_csv_file("data/users.csv").await?;

// Apply transformations
let result = df
    .filter(col("age").gt(lit(25)))?
    .select(vec![col("name"), col("email")])?;

result.show().await?;
```

#### Read with Custom Options

```rust
use datafusion_csv::CsvFormatOptions;

let options = CsvFormatOptions::new()
    .with_delimiter(b'\t')
    .with_batch_size(2048);

let df = ctx.read_csv_with_options("data.tsv", options).await?;
```

## Advanced Features

### Querying Multiple Tables

```rust
// Register multiple CSV files
ctx.register_csv_file("users", "data/users.csv").await?;
ctx.register_csv_file("orders", "data/orders.csv").await?;
ctx.register_csv_file("products", "data/products.csv").await?;

// Perform complex joins
let df = ctx.sql(
    r#"
    SELECT
        u.name,
        p.product_name,
        o.quantity,
        o.total_amount
    FROM orders o
    JOIN users u ON o.user_id = u.user_id
    JOIN products p ON o.product_id = p.product_id
    WHERE o.total_amount > 100
    ORDER BY o.total_amount DESC
    "#
).await?;

df.show().await?;
```

### Aggregations and Analytics

```rust
let df = ctx.sql(
    r#"
    SELECT
        country,
        COUNT(*) as user_count,
        AVG(order_total) as avg_order_value
    FROM users u
    JOIN orders o ON u.user_id = o.user_id
    GROUP BY country
    HAVING COUNT(*) > 10
    ORDER BY avg_order_value DESC
    "#
).await?;
```

### Using DataFrame API

Instead of SQL, you can use the DataFrame API:

```rust
use datafusion::prelude::*;

let df = ctx.read_csv_file("data/sales.csv").await?;

let result = df
    .filter(col("revenue").gt(lit(1000)))?
    .select(vec![
        col("region"),
        col("revenue"),
        col("units_sold"),
    ])?
    .aggregate(
        vec![col("region")],
        vec![
            sum(col("revenue")).alias("total_revenue"),
            sum(col("units_sold")).alias("total_units"),
        ],
    )?
    .sort(vec![col("total_revenue").sort(false, true)])?;

result.show().await?;
```

### Working with Large Files

For large CSV files, tune batch size for optimal memory/performance trade-off:

```rust
use datafusion_csv::CsvFormatOptions;

// For analytics workloads (more memory, better throughput)
let options = CsvFormatOptions::new()
    .with_batch_size(16384)  // Larger batches
    .with_schema_infer_max_rec(Some(5000));  // Sample more rows

ctx.register_csv_with_options("large_data", "big_file.csv", options).await?;

// For memory-constrained environments
let options = CsvFormatOptions::new()
    .with_batch_size(2048);  // Smaller batches, less memory

ctx.register_csv_with_options("data", "file.csv", options).await?;
```

**Memory Considerations:**
- The CSV reader uses batch-level streaming
- Only one RecordBatch is active in memory during query execution
- File is loaded once, then batches are parsed on-demand
- For very large files (multi-GB), ensure sufficient RAM for the file itself

See [STREAMING_ARCHITECTURE.md](../STREAMING_ARCHITECTURE.md) for details.

## Configuration Options

### CsvFormatOptions

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `has_header` | `bool` | `true` | Whether CSV has header row |
| `delimiter` | `u8` | `b','` | Delimiter character |
| `schema_infer_max_rec` | `Option<usize>` | `Some(1000)` | Max rows for schema inference |
| `batch_size` | `usize` | `8192` | Number of rows per batch |
| `file_extension` | `String` | `".csv"` | File extension to match |

### Builder Pattern

All options use a fluent builder pattern:

```rust
let options = CsvFormatOptions::new()
    .with_has_header(true)
    .with_delimiter(b',')
    .with_batch_size(8192)
    .with_schema_infer_max_rec(Some(1000))
    .with_file_extension(".csv");
```

## Examples

### Example 1: Basic CSV Query

```rust
use datafusion::prelude::*;
use datafusion_csv::SessionContextCsvExt;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    ctx.register_csv_file("employees", "data/employees.csv").await?;

    let df = ctx.sql(
        "SELECT name, salary FROM employees WHERE department = 'Engineering'"
    ).await?;

    df.show().await?;
    Ok(())
}
```

### Example 2: TSV File with Aggregation

```rust
use datafusion::prelude::*;
use datafusion_csv::SessionContextCsvExt;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register TSV file
    ctx.register_csv_with_delimiter("sales", "quarterly_sales.tsv", b'\t').await?;

    // Aggregate by region
    let df = ctx.sql(
        r#"
        SELECT
            region,
            SUM(revenue) as total_revenue,
            AVG(revenue) as avg_revenue
        FROM sales
        GROUP BY region
        ORDER BY total_revenue DESC
        "#
    ).await?;

    df.show().await?;
    Ok(())
}
```

### Example 3: Multi-Table Join

```rust
use datafusion::prelude::*;
use datafusion_csv::SessionContextCsvExt;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register multiple tables
    ctx.register_csv_file("customers", "data/customers.csv").await?;
    ctx.register_csv_file("orders", "data/orders.csv").await?;
    ctx.register_csv_file("products", "data/products.csv").await?;

    // Complex join query
    let df = ctx.sql(
        r#"
        SELECT
            c.customer_name,
            p.product_name,
            o.order_date,
            o.quantity * p.price as total_value
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        JOIN products p ON o.product_id = p.id
        WHERE o.order_date >= '2024-01-01'
        ORDER BY total_value DESC
        LIMIT 10
        "#
    ).await?;

    df.show().await?;
    Ok(())
}
```

### Example 4: Custom Options for European CSV

```rust
use datafusion::prelude::*;
use datafusion_csv::{SessionContextCsvExt, CsvFormatOptions};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // European CSV format: semicolon delimiter, comma decimal separator
    let options = CsvFormatOptions::new()
        .with_delimiter(b';')
        .with_has_header(true);

    ctx.register_csv_with_options("data", "european_data.csv", options).await?;

    let df = ctx.sql("SELECT * FROM data").await?;
    df.show().await?;

    Ok(())
}
```

### Example 5: DataFrame API with Filters

```rust
use datafusion::prelude::*;
use datafusion_csv::SessionContextCsvExt;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let df = ctx.read_csv_file("data/transactions.csv").await?;

    // Chain operations using DataFrame API
    let result = df
        .filter(col("amount").gt(lit(1000)))?
        .filter(col("status").eq(lit("completed")))?
        .select(vec![
            col("transaction_id"),
            col("amount"),
            col("customer_id"),
        ])?
        .sort(vec![col("amount").sort(false, true)])?
        .limit(0, Some(20))?;

    result.show().await?;
    Ok(())
}
```

## Troubleshooting

### Common Issues

#### 1. File Not Found

**Error**: `"No such file or directory"`

**Solution**: Ensure the file path is correct and relative to your working directory:

```rust
// Use absolute path
ctx.register_csv_file("data", "/absolute/path/to/file.csv").await?;

// Or relative to project root
ctx.register_csv_file("data", "./data/file.csv").await?;
```

#### 2. Schema Mismatch

**Error**: `"Schema mismatch"` or parsing errors

**Solution**: Check delimiter and header settings:

```rust
let options = CsvFormatOptions::new()
    .with_delimiter(b'\t')  // Use tab for TSV
    .with_has_header(true); // Explicitly set header option
```

#### 3. Large File Performance

**Problem**: Slow performance with large files

**Solution**: Increase batch size and sampling:

```rust
let options = CsvFormatOptions::new()
    .with_batch_size(16384)
    .with_schema_infer_max_rec(Some(10000));
```

#### 4. Memory Issues

**Problem**: Out of memory with very large files

**Solution**: Process in smaller batches and use streaming:

```rust
let options = CsvFormatOptions::new()
    .with_batch_size(4096);  // Smaller batches

// Process results in batches
let df = ctx.read_csv_with_options("huge_file.csv", options).await?;
let stream = df.execute_stream().await?;
// Process stream batch by batch
```

## Best Practices

1. **Tune Batch Sizes for Your Workload**:
   - Analytics: 16,384 rows (better throughput)
   - Balanced: 8,192 rows (default, good for most cases)
   - Memory-constrained: 2,048-4,096 rows (lower memory footprint)

2. **Schema Inference**: Sample enough rows for accurate schema detection (1000-5000 rows)

3. **Use Column Projections**: Only select columns you need to reduce memory per batch:
   ```sql
   SELECT name, email FROM users  -- Much less memory than SELECT *
   ```

4. **File Extensions**: Use explicit file extensions when working with non-standard formats

5. **Error Handling**: Always handle errors gracefully:
   ```rust
   match ctx.register_csv_file("data", path).await {
       Ok(_) => println!("Table registered successfully"),
       Err(e) => eprintln!("Error: {}", e),
   }
   ```

6. **Large Files**: For files larger than available RAM, consider:
   - Splitting the file into smaller chunks
   - Using external tools to filter/sample data first
   - Upgrading to a machine with more memory

7. **Memory Monitoring**: The implementation loads the file into memory, then streams batches:
   ```
   Peak Memory ≈ File Size + (Batch Size × Row Size)
   ```

## Next Steps

- Check out the [DEVELOPMENT.md](DEVELOPMENT.md) for contributing
- See the [examples](tests/e2e_test.rs) for more usage patterns
- Explore DataFusion's [official documentation](https://arrow.apache.org/datafusion/)

## Getting Help

- GitHub Issues: Report bugs or request features
- DataFusion Discord: Join the community
- Stack Overflow: Tag questions with `datafusion`

## License

This project is licensed under the MIT License - see the LICENSE file for details.

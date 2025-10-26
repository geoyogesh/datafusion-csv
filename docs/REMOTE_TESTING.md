# Remote Object Store Testing Guide

This guide explains how to test reading CSV files from remote object stores (S3, R2, Azure, etc.).

## Quick Start

✅ **All remote tests are now working!** The implementation supports reading from HTTP/HTTPS URLs, and all 5 remote object store tests are passing using the publicly accessible R2 bucket.

## Current Test Setup

The remote tests use a publicly accessible Cloudflare R2 bucket:

```
https://pub-f49e62c2a4114dc1bbbb62a1167ab950.r2.dev/readers/csv/customers-100.csv
```

This CSV file contains 100 sample customer records with the following columns:
- Index
- Customer Id
- First Name, Last Name
- Company
- City, Country
- Phone 1, Phone 2
- Email
- Subscription Date
- Website

## Running Remote Tests

All remote tests are enabled and passing:

```bash
# Run all tests (includes remote tests)
cargo test

# Run only remote object store tests
cargo test remote_object_store

# Run a specific remote test
cargo test test_read_from_remote_object_store
```

## Available Remote Tests

All 5 remote tests are passing:

1. **test_read_from_remote_object_store** - Basic remote CSV reading and filtering by country
2. **test_filter_remote_object_store** - Multi-condition filtering on remote data
3. **test_aggregation_remote_object_store** - GROUP BY aggregations with LIMIT
4. **test_column_selection_remote_object_store** - Column projection and filtering
5. **test_read_remote_csv_to_dataframe** - DataFrame API with column selection

## Using Remote CSV Files in Your Code

Once the HTTP object store is working, you can use it in your application:

```rust
use datafusion::prelude::*;
use datafusion_csv::SessionContextCsvExt;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register remote CSV file
    ctx.register_csv_file(
        "users",
        "https://pub-f49e62c2a4114dc1bbbb62a1167ab950.r2.dev/users.csv"
    ).await?;

    // Query remote data
    let df = ctx.sql("SELECT * FROM users WHERE country = 'USA'").await?;
    df.show().await?;

    Ok(())
}
```

## Supported Object Stores

The implementation automatically detects HTTP/HTTPS URLs and registers the appropriate object store:

- **HTTP/HTTPS**: Any public URL (R2, S3 with public access, etc.)
- **Local Files**: File system paths (relative or absolute)

For authenticated S3/Azure access, you would need to configure the runtime environment separately.

## Troubleshooting

### 404 Not Found

If you get a 404 error:
1. Verify the file exists at the exact URL
2. Check that public access is enabled
3. Ensure there's no typo in the file path

### CORS Errors

If accessing from a web browser:
1. Configure CORS policy in R2 bucket settings
2. Add appropriate `Access-Control-Allow-Origin` headers

### Performance Considerations

- **File Size**: The entire file is loaded into memory, then streamed in batches
- **Network Latency**: Remote files have initial download overhead
- **Caching**: DataFusion doesn't cache remote files - they're fetched on each query

For production use with large remote files, consider:
- Using a CDN for better performance
- Implementing application-level caching
- Partitioning large files into smaller chunks

## Example Upload Script

Upload test files to R2 using the R2 CLI:

```bash
# Install R2 CLI (wrangler)
npm install -g wrangler

# Upload files
wrangler r2 object put your-bucket/users.csv --file tests/e2e_data/users.csv
wrangler r2 object put your-bucket/orders.csv --file tests/e2e_data/orders.csv
wrangler r2 object put your-bucket/products.csv --file tests/e2e_data/products.csv
wrangler r2 object put your-bucket/sales_data.tsv --file tests/e2e_data/sales_data.tsv
```

Or using AWS CLI (works with R2):

```bash
# Configure R2 credentials
aws configure --profile r2

# Upload files
aws s3 cp tests/e2e_data/users.csv s3://your-bucket/ --profile r2 --endpoint-url=https://your-account-id.r2.cloudflarestorage.com
aws s3 cp tests/e2e_data/orders.csv s3://your-bucket/ --profile r2 --endpoint-url=https://your-account-id.r2.cloudflarestorage.com
aws s3 cp tests/e2e_data/products.csv s3://your-bucket/ --profile r2 --endpoint-url=https://your-account-id.r2.cloudflarestorage.com
aws s3 cp tests/e2e_data/sales_data.tsv s3://your-bucket/ --profile r2 --endpoint-url=https://your-account-id.r2.cloudflarestorage.com
```

## Next Steps

Once remote tests pass:
1. Remove `#[ignore]` attribute from tests in `tests/e2e_test.rs`
2. Update test count in README (30 tests passing instead of 25)
3. Consider adding more remote object store examples to documentation

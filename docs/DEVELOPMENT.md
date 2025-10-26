# DataFusion CSV - Development Guide

This guide is for developers who want to contribute to or understand the internal architecture of datafusion-csv.

## Overview

This is an **independent CSV reader implementation** for Apache DataFusion. Unlike simple wrappers, this implementation provides:

- Native CSV parsing using the `csv` crate
- Automatic type inference (Int64, Float64, Boolean, Utf8)
- Direct conversion to Arrow RecordBatches
- Custom `FileFormat`, `ExecutionPlan`, and `FileOpener` implementations
- Following architectural patterns from DataFusion's file format plugins

## Table of Contents

- [Project Structure](#project-structure)
- [Architecture](#architecture)
- [Development Setup](#development-setup)
- [Building and Testing](#building-and-testing)
- [Code Organization](#code-organization)
- [Contributing](#contributing)
- [Design Decisions](#design-decisions)

## Project Structure

```
datafusion-csv/
├── src/
│   ├── lib.rs                 # Main API and SessionContext extensions
│   ├── file_format.rs         # CSV format configuration
│   ├── file_source.rs         # CSV source builders and table providers
│   ├── physical_exec.rs       # Physical execution utilities
│   └── object_store_reader.rs # Object store integration helpers
├── tests/
│   ├── e2e_test.rs           # End-to-end integration tests
│   └── e2e_data/             # Test data files
│       ├── users.csv
│       ├── orders.csv
│       ├── products.csv
│       └── sales_data.tsv
├── Cargo.toml                 # Project dependencies
├── USER_GUIDE.md             # User documentation
├── DEVELOPMENT.md            # This file
└── README.md                 # Project overview
```

## Architecture

The project follows a modular design that separates concerns:

### Module Hierarchy

```
┌─────────────────────────────────────────┐
│           User Application              │
└─────────────────┬───────────────────────┘
                  │
┌─────────────────▼───────────────────────┐
│  lib.rs - SessionContextCsvExt trait    │
│  - register_csv_file()                  │
│  - register_csv_with_delimiter()        │
│  - register_csv_with_options()          │
│  - read_csv_file()                      │
│  - read_csv_with_options()              │
└─────────────────┬───────────────────────┘
                  │
        ┌─────────┴─────────┐
        │                   │
┌───────▼────────┐  ┌──────▼──────────┐
│ file_format.rs │  │ file_source.rs  │
│ - Options      │  │ - Builder       │
│ - Config       │  │ - Provider      │
└───────┬────────┘  └──────┬──────────┘
        │                  │
        └─────────┬────────┘
                  │
    ┌─────────────┴─────────────┐
    │                           │
┌───▼──────────────┐  ┌────────▼─────────────┐
│ physical_exec.rs │  │ object_store_reader.rs│
│ - Execution      │  │ - Metadata           │
│ - Validation     │  │ - URL helpers        │
└──────────────────┘  └──────────────────────┘
                  │
        ┌─────────┴─────────┐
        │                   │
┌───────▼────────┐  ┌──────▼──────────┐
│ DataFusion     │  │ Object Store    │
│ CSV Format     │  │ (local/S3/etc)  │
└────────────────┘  └─────────────────┘
```

### Data Flow

1. **User API Call** → User calls one of the `SessionContextCsvExt` methods
2. **Options Creation** → `CsvFormatOptions` configured (file_format.rs)
3. **Table Provider** → `create_csv_table_provider()` builds the provider (file_source.rs)
4. **Schema Inference** → Our independent `infer_schema()` samples CSV and detects types (physical_exec.rs)
5. **Execution Plan** → `CsvExec` custom execution plan created (file_source.rs)
6. **File Opening** → `CsvOpener` reads from object store → **entire file loaded** (physical_exec.rs)
7. **CSV Parsing** → `CsvStream` parses records in batches using `csv` crate (physical_exec.rs)
8. **Batch Streaming** → `poll_next()` returns one RecordBatch at a time (8,192 rows default)
9. **Arrow Conversion** → Direct conversion to typed Arrow arrays (Int64, Float64, Boolean, Utf8)
10. **RecordBatch** → Batches streamed to DataFusion for query execution

### Streaming Architecture

**Batch-Level Streaming** (current implementation):
- File loaded once into memory via `object_store.get().bytes().await`
- `CsvStream` implements `Stream` trait
- Yields one RecordBatch at a time via `poll_next()`
- Buffer reused between batches to minimize allocations
- Memory: File size + one batch (~10MB for typical batch)

See [STREAMING_ARCHITECTURE.md](STREAMING_ARCHITECTURE.md) for detailed analysis and future enhancements.

## Development Setup

### Prerequisites

- Rust 1.70 or later
- Cargo (comes with Rust)
- Git

### Clone and Build

```bash
# Clone the repository
git clone https://github.com/geoyogesh/datafusion-csv.git
cd datafusion-csv

# Build the project
cargo build

# Run tests
cargo test

# Run tests with output
cargo test -- --nocapture
```

### IDE Setup

#### VS Code

Install the following extensions:
- `rust-analyzer` - Rust language support
- `CodeLLDB` - Debugger
- `Even Better TOML` - TOML syntax support

#### RustRover / IntelliJ IDEA

Install the Rust plugin for full IDE support.

## Building and Testing

### Build Commands

```bash
# Debug build (development)
cargo build

# Release build (optimized)
cargo build --release

# Check without building (fast)
cargo check

# Build documentation
cargo doc --open
```

### Test Commands

```bash
# Run all tests
cargo test

# Run unit tests only
cargo test --lib

# Run integration tests only
cargo test --test e2e_test

# Run specific test
cargo test test_read_users_csv

# Run tests with output
cargo test -- --nocapture

# Run tests matching pattern
cargo test join
```

### Test Organization

**Unit Tests** (in src/lib.rs and module files):
- Test individual functions and methods
- Mock external dependencies
- Fast execution

**Integration Tests** (in tests/e2e_test.rs):
- Test complete workflows
- Use real CSV files from tests/e2e_data/
- Test SQL queries and joins

**Doc Tests** (in documentation comments):
- Ensure examples in docs compile
- Validate API usage patterns

### Running Specific Test Suites

```bash
# Unit tests
cargo test --lib

# E2E tests
cargo test --test e2e_test

# Doc tests
cargo test --doc

# Tests for specific module
cargo test file_format
cargo test object_store_reader
```

## Code Organization

### Module Descriptions

#### 1. `lib.rs` - Main API

**Purpose**: Provides the public API that users interact with.

**Key Components**:
- `SessionContextCsvExt` trait - Extension methods for `SessionContext`
- Public re-exports of types from other modules
- Unit tests for core functionality

**Design Pattern**: Extension trait pattern for adding methods to existing types

```rust
pub trait SessionContextCsvExt {
    async fn register_csv_file(&self, name: &str, path: &str) -> Result<()>;
    async fn register_csv_with_delimiter(&self, name: &str, path: &str, delimiter: u8) -> Result<()>;
    async fn register_csv_with_options(&self, name: &str, path: &str, options: CsvFormatOptions) -> Result<()>;
    async fn read_csv_file(&self, path: &str) -> Result<DataFrame>;
    async fn read_csv_with_options(&self, path: &str, options: CsvFormatOptions) -> Result<DataFrame>;
}
```

#### 2. `file_format.rs` - Format Configuration

**Purpose**: Defines configuration options for CSV parsing.

**Key Components**:
- `CsvFormatOptions` - Builder pattern for CSV configuration
- Helper functions for file extension detection
- Conversion to DataFusion's format

**Design Pattern**: Builder pattern for fluent API

```rust
pub struct CsvFormatOptions {
    pub has_header: bool,
    pub delimiter: u8,
    pub schema_infer_max_rec: Option<usize>,
    pub batch_size: usize,
    pub file_extension: String,
}
```

#### 3. `file_source.rs` - Source Management

**Purpose**: Creates and configures DataFusion table providers.

**Key Components**:
- `CsvSourceBuilder` - Fluent builder for CSV sources
- `create_csv_table_provider()` - Factory function for table providers
- Integration with DataFusion's listing tables

**Design Pattern**: Builder pattern + Factory pattern

#### 4. `physical_exec.rs` - Physical Execution

**Purpose**: Core CSV reading and streaming implementation.

**Key Components**:
- `CsvOpener` - Implements `FileOpener` trait for async file opening
- `CsvStream` - Implements `Stream` trait for batch-by-batch processing
- `infer_schema()` - Independent schema inference with type detection
- RecordBatch conversion - Direct CSV → Arrow array conversion

**Design Pattern**: Stream pattern for on-demand batch processing

**Streaming Implementation**:
```rust
pub struct CsvStream {
    reader: csv::Reader<Cursor<Vec<u8>>>,  // File in memory
    opener: CsvOpener,
    record_buffer: Vec<csv::StringRecord>,  // Reused buffer
    schema: SchemaRef,
    finished: bool,
}

impl Stream for CsvStream {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn poll_next(...) -> Poll<Option<Self::Item>> {
        // Returns one batch at a time (8,192 rows default)
        // Reuses record_buffer to minimize allocations
    }
}
```

#### 5. `object_store_reader.rs` - Storage Integration

**Purpose**: Utilities for working with object stores.

**Key Components**:
- `CsvFileMetadata` - File metadata representation
- URL construction and parsing helpers
- Support for local, S3, and cloud storage

**Design Pattern**: Utility pattern with helper functions

### Adding New Features

#### Example: Adding a New Configuration Option

1. **Add to `CsvFormatOptions`** in `file_format.rs`:
```rust
pub struct CsvFormatOptions {
    // ... existing fields
    pub quote_char: u8,  // New field
}

impl CsvFormatOptions {
    pub fn with_quote_char(mut self, quote_char: u8) -> Self {
        self.quote_char = quote_char;
        self
    }
}
```

2. **Update conversion** in `to_datafusion_format()`:
```rust
pub(crate) fn to_datafusion_format(&self) -> DataFusionCsvFormat {
    DataFusionCsvFormat::default()
        .with_delimiter(self.delimiter)
        .with_has_header(self.has_header)
        .with_quote(self.quote_char)  // New
}
```

3. **Add tests**:
```rust
#[tokio::test]
async fn test_custom_quote_char() -> Result<()> {
    let options = CsvFormatOptions::new()
        .with_quote_char(b'\'');

    ctx.register_csv_with_options("data", "file.csv", options).await?;
    // ... assertions
}
```

4. **Update documentation** in `lib.rs` and `USER_GUIDE.md`

## Contributing

### Contribution Workflow

1. **Fork the repository** on GitHub

2. **Clone your fork**:
   ```bash
   git clone https://github.com/YOUR_USERNAME/datafusion-csv.git
   ```

3. **Create a feature branch**:
   ```bash
   git checkout -b feature/my-new-feature
   ```

4. **Make your changes** and test thoroughly

5. **Run all tests**:
   ```bash
   cargo test
   cargo clippy
   cargo fmt --check
   ```

6. **Commit your changes**:
   ```bash
   git add .
   git commit -m "Add feature: description"
   ```

7. **Push to your fork**:
   ```bash
   git push origin feature/my-new-feature
   ```

8. **Create a Pull Request** on GitHub

### Code Style

We follow standard Rust conventions:

```bash
# Format code
cargo fmt

# Check formatting
cargo fmt --check

# Run linter
cargo clippy

# Run with all warnings as errors
cargo clippy -- -D warnings
```

### Commit Message Guidelines

Follow conventional commit format:

```
feat: add support for custom quote characters
fix: handle empty CSV files correctly
docs: update user guide with examples
test: add tests for TSV delimiter
refactor: simplify file_source builder
```

Types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `chore`

### Testing Requirements

All contributions must include:

1. **Unit Tests** - Test individual functions
2. **Integration Tests** - Test end-to-end workflows
3. **Doc Tests** - Ensure examples work
4. **Test Coverage** - Aim for >80% coverage

### Documentation Requirements

1. **Code Comments** - Document complex logic
2. **Doc Comments** - Document public API with examples
3. **User Guide** - Update if adding user-facing features
4. **DEVELOPMENT.md** - Update if changing architecture

## Design Decisions

### Why an Independent CSV Implementation?

Rather than wrapping DataFusion's built-in CSV support, we implement an independent reader:

1. **Learning and Control** - Full control over CSV parsing and type inference
2. **Custom Type Detection** - Intelligent type inference by sampling data
3. **Architectural Patterns** - Follows DataFusion file format plugin patterns (like datafusion-orc)
4. **Direct Arrow Conversion** - Native conversion from CSV strings to typed Arrow arrays
5. **Extensibility** - Easy to extend with custom features and optimizations

### Architecture Rationale

**Modular Design**: Each module has a single responsibility
- Easier to test and maintain
- Clear separation of concerns
- Clean architectural boundaries

**Builder Pattern**: Used throughout for configuration
- Fluent API that's easy to use
- Optional parameters with sensible defaults
- Type-safe configuration

**Extension Traits**: For adding methods to SessionContext
- Non-intrusive way to extend DataFusion
- Keeps our code separate from DataFusion's internals
- Easy to opt-in to our functionality

### Future Enhancements

Potential improvements:

1. **Schema Inference Cache** - Cache inferred schemas for faster repeated access
2. **Parallel File Reading** - Read multiple CSV files in parallel
3. **Compression Support** - Handle gzipped CSV files
4. **Custom Parsers** - Support custom date/time formats
5. **Streaming Optimizations** - Better memory usage for large files

## Debugging

### Enable Logging

```rust
env_logger::init();
std::env::set_var("RUST_LOG", "datafusion_csv=debug");
```

### Common Debug Scenarios

**Schema Issues**:
```rust
// Print inferred schema
let table = create_csv_table_provider(&state, path, options).await?;
println!("Schema: {:?}", table.schema());
```

**Performance Issues**:
```rust
// Add timing
use std::time::Instant;
let start = Instant::now();
let df = ctx.read_csv_file("large.csv").await?;
println!("Read time: {:?}", start.elapsed());
```

**Memory Usage**:
```bash
# Profile memory usage
cargo build --release
valgrind --tool=massif target/release/your_app
```

## Release Process

1. Update version in `Cargo.toml`
2. Update CHANGELOG.md
3. Run all tests: `cargo test`
4. Build release: `cargo build --release`
5. Tag release: `git tag v0.1.0`
6. Push tag: `git push origin v0.1.0`
7. Create GitHub release
8. Publish to crates.io: `cargo publish`

## Resources

- [DataFusion Documentation](https://arrow.apache.org/datafusion/)
- [Arrow Documentation](https://arrow.apache.org/)
- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- [DataFusion Discord](https://discord.gg/datafusion)

## Getting Help

- **Issues**: Open a GitHub issue for bugs or features
- **Discussions**: Use GitHub Discussions for questions
- **Discord**: Join the DataFusion community

## License

This project is licensed under the MIT License - see the LICENSE file for details.

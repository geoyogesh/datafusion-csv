# Next Steps and Improvement Areas

This document outlines potential improvements and future work for the `datafusion-csv` project.

### 1. Implement True Streaming

The current implementation reads the entire CSV file into memory before processing, which is inefficient for large files.

- **Action:** Modify the `CsvStream` to read data directly from the object store's streaming reader (`impl `std::io::Read`) instead of a byte buffer. This will prevent out-of-memory errors and improve performance for large datasets.

### 2. Enhance Type Inference

The schema inference is basic and only supports a limited set of data types.

- **Action:** Expand the type inference logic in `src/physical_exec.rs` to detect a wider range of types, including:
    - Different integer and float sizes (e.g., `Int32`, `Float32`).
    - Date, time, and timestamp formats (e.g., `YYYY-MM-DD`, `ISO 8601`).

### 3. Add Compression Support

The library currently lacks support for reading compressed CSV files.

- **Action:** Add support for common compression formats like Gzip (`.csv.gz`). This will involve detecting the compression type from the file extension and decompressing the data on the fly.

### 4. Refactor for Code Reusability

There is duplicated code between `CsvOpener` and `CsvStream` in `src/physical_exec.rs`.

- **Action:** Refactor the `records_to_batch` and `build_array` functions into a shared utility or a common module to improve code maintainability and reduce redundancy.

### 5. Expose More CSV Parsing Options

The `csv` crate provides a rich set of parsing options that are not currently exposed to the user.

- **Action:** Extend `CsvFormatOptions` to include more options from the `csv` crate, such as:
    - Flexible quoting (`flexible` method in `csv::ReaderBuilder`).
    - Different comment characters.
    - Configurable line terminators.

### 6. Improve Error Handling

Error messages are often generic, making it difficult to diagnose issues.

- **Action:** Replace generic `DataFusionError::Execution` wrappers with more specific and descriptive error types. This will provide users with better feedback and simplify debugging.
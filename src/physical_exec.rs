//! Physical execution for CSV reading
//!
//! This module implements the core CSV reading and parsing logic,
//! converting CSV data directly to Arrow RecordBatches.

use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_array::{ArrayRef, StringArray};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use datafusion::datasource::physical_plan::{FileOpener, FileOpenFuture, FileMeta};
use datafusion::error::{DataFusionError, Result};
use futures::stream::Stream;
use object_store::ObjectStore;

use crate::file_format::CsvFormatOptions;

/// CSV file opener that implements the FileOpener trait
#[derive(Clone)]
pub struct CsvOpener {
    /// CSV format options
    options: CsvFormatOptions,
    /// Schema for the CSV file
    schema: SchemaRef,
    /// Column projection (indices of columns to read)
    projection: Option<Vec<usize>>,
    /// Batch size for reading
    batch_size: usize,
    /// Object store for reading files
    object_store: Arc<dyn ObjectStore>,
}

impl CsvOpener {
    pub fn new(
        options: CsvFormatOptions,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            options,
            schema,
            projection,
            batch_size: 8192,
            object_store,
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

impl FileOpener for CsvOpener {
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
        let opener = self.clone();
        let object_store = self.object_store.clone();

        Ok(Box::pin(async move {
            // Get async reader from object store
            let location = file_meta.location();
            let get_result = object_store.get(location).await.map_err(|e| {
                DataFusionError::Execution(format!("Failed to read file: {}", e))
            })?;

            // Read bytes from object store
            let bytes = get_result.bytes().await.map_err(|e| {
                DataFusionError::Execution(format!("Failed to read bytes: {}", e))
            })?;

            // Create streaming CSV reader
            let stream = CsvStream::new(bytes, opener)?;

            // Return the stream directly - CsvStream already returns ArrowError
            Ok(Box::pin(stream) as _)
        }))
    }
}

/// Streaming CSV reader that yields RecordBatches incrementally
///
/// This struct implements the `Stream` trait to provide on-demand batch processing.
/// Instead of loading the entire CSV file into memory at once, it:
///
/// 1. Reads records from the CSV reader in chunks
/// 2. Buffers up to `batch_size` records
/// 3. Converts the buffer to a RecordBatch
/// 4. Yields the batch when requested via `poll_next`
/// 5. Repeats until the file is exhausted
///
/// This approach ensures that only one batch worth of data is in memory at a time,
/// making it suitable for processing large CSV files efficiently.
struct CsvStream {
    /// CSV reader
    reader: csv::Reader<Cursor<Vec<u8>>>,
    /// CSV opener with configuration
    opener: CsvOpener,
    /// Buffer for collecting records (reused to minimize allocations)
    record_buffer: Vec<csv::StringRecord>,
    /// Schema for output batches
    schema: SchemaRef,
    /// Whether we've finished reading
    finished: bool,
}

impl CsvStream {
    fn new(bytes: Bytes, opener: CsvOpener) -> Result<Self> {
        let cursor = Cursor::new(bytes.to_vec());
        let reader = csv::ReaderBuilder::new()
            .delimiter(opener.options.delimiter)
            .has_headers(opener.options.has_header)
            .from_reader(cursor);

        // Get the output schema (projected or full)
        let schema = if let Some(ref proj) = opener.projection {
            let fields: Vec<Field> = proj
                .iter()
                .map(|i| opener.schema.field(*i).clone())
                .collect();
            Arc::new(Schema::new(fields))
        } else {
            opener.schema.clone()
        };

        Ok(Self {
            reader,
            opener,
            record_buffer: Vec::new(),
            schema,
            finished: false,
        })
    }

    /// Read next batch of records
    fn read_next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.finished {
            return Ok(None);
        }

        self.record_buffer.clear();

        // Read up to batch_size records
        for _ in 0..self.opener.batch_size {
            match self.reader.records().next() {
                Some(Ok(record)) => self.record_buffer.push(record),
                Some(Err(e)) => {
                    return Err(DataFusionError::Execution(format!("CSV parse error: {}", e)))
                }
                None => {
                    self.finished = true;
                    break;
                }
            }
        }

        if self.record_buffer.is_empty() {
            return Ok(None);
        }

        // Convert records to batch
        let batch = self.records_to_batch(&self.record_buffer)?;
        Ok(Some(batch))
    }

    /// Convert CSV records to a RecordBatch (copied from CsvOpener)
    fn records_to_batch(&self, records: &[csv::StringRecord]) -> Result<RecordBatch> {
        if records.is_empty() {
            return Err(DataFusionError::Execution("No records to convert".to_string()));
        }

        // Get the indices of columns to include
        let column_indices: Vec<usize> = if let Some(proj) = &self.opener.projection {
            proj.clone()
        } else {
            (0..self.opener.schema.fields().len()).collect()
        };

        // Handle empty projection case (e.g., COUNT(*) queries)
        if column_indices.is_empty() {
            let schema = Arc::new(Schema::empty());
            return RecordBatch::try_new_with_options(
                schema,
                vec![],
                &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(records.len())),
            )
            .map_err(|e| {
                DataFusionError::Execution(format!("Failed to create empty RecordBatch: {}", e))
            });
        }

        // Build columns
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(column_indices.len());

        for &actual_idx in &column_indices {
            let field = self.opener.schema.field(actual_idx);
            let column_data: Vec<Option<&str>> = records
                .iter()
                .map(|record| record.get(actual_idx))
                .collect();

            let array = self.build_array(field, &column_data)?;
            columns.push(array);
        }

        RecordBatch::try_new(self.schema.clone(), columns).map_err(|e| {
            DataFusionError::Execution(format!("Failed to create RecordBatch: {}", e))
        })
    }

    /// Build an Arrow array from column data (copied from CsvOpener)
    fn build_array(&self, field: &Field, data: &[Option<&str>]) -> Result<ArrayRef> {
        match field.data_type() {
            DataType::Utf8 => {
                let array: StringArray = data.iter().map(|v| *v).collect();
                Ok(Arc::new(array))
            }
            DataType::Int64 => {
                use arrow_array::Int64Array;
                let array: Int64Array = data
                    .iter()
                    .map(|v| v.and_then(|s| s.parse::<i64>().ok()))
                    .collect();
                Ok(Arc::new(array))
            }
            DataType::Float64 => {
                use arrow_array::Float64Array;
                let array: Float64Array = data
                    .iter()
                    .map(|v| v.and_then(|s| s.parse::<f64>().ok()))
                    .collect();
                Ok(Arc::new(array))
            }
            DataType::Boolean => {
                use arrow_array::BooleanArray;
                let array: BooleanArray = data
                    .iter()
                    .map(|v| v.and_then(|s| s.parse::<bool>().ok()))
                    .collect();
                Ok(Arc::new(array))
            }
            _ => {
                // Default to string for unsupported types
                let array: StringArray = data.iter().map(|v| *v).collect();
                Ok(Arc::new(array))
            }
        }
    }
}

impl Stream for CsvStream {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.read_next_batch() {
            Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
            Ok(None) => Poll::Ready(None),
            Err(e) => {
                // Convert DataFusionError to ArrowError
                let arrow_err = ArrowError::ExternalError(Box::new(e));
                Poll::Ready(Some(Err(arrow_err)))
            }
        }
    }
}


/// Infer schema from CSV file with type detection
pub async fn infer_schema(
    bytes: &[u8],
    options: &CsvFormatOptions,
) -> Result<Schema> {
    let cursor = Cursor::new(bytes);
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(options.delimiter)
        .has_headers(options.has_header)
        .from_reader(cursor);

    let headers: Vec<String> = if options.has_header {
        reader
            .headers()
            .map_err(|e| DataFusionError::Execution(format!("Failed to read headers: {}", e)))?
            .iter()
            .map(|s| s.to_string())
            .collect()
    } else {
        // Generate column names if no header
        let first_record = reader.records().next();
        if let Some(Ok(record)) = first_record {
            (0..record.len())
                .map(|i| format!("column_{}", i))
                .collect()
        } else {
            return Err(DataFusionError::Execution(
                "Cannot infer schema from empty file".to_string(),
            ));
        }
    };

    // Sample records to infer types
    let max_records = options.schema_infer_max_rec.unwrap_or(1000);
    let mut sample_records: Vec<csv::StringRecord> = Vec::new();

    for (i, result) in reader.records().enumerate() {
        if i >= max_records {
            break;
        }
        match result {
            Ok(record) => sample_records.push(record),
            Err(_) => continue,
        }
    }

    // Infer type for each column
    let num_columns = headers.len();
    let mut fields: Vec<Field> = Vec::with_capacity(num_columns);

    for (col_idx, name) in headers.into_iter().enumerate() {
        let data_type = infer_column_type(&sample_records, col_idx);
        fields.push(Field::new(name, data_type, true));
    }

    Ok(Schema::new(fields))
}

/// Infer the data type of a column by sampling values
fn infer_column_type(records: &[csv::StringRecord], col_idx: usize) -> DataType {
    let mut has_float = false;
    let mut has_int = false;
    let mut has_bool = false;
    let mut total_values = 0;

    for record in records.iter().take(100) {
        if let Some(value) = record.get(col_idx) {
            let value = value.trim();
            if value.is_empty() {
                continue;
            }

            total_values += 1;

            // Check if it's a boolean
            if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
                has_bool = true;
                continue;
            }

            // Check if it's a float
            if value.parse::<f64>().is_ok() {
                if value.contains('.') {
                    has_float = true;
                } else {
                    has_int = true;
                }
            }
        }
    }

    // Prioritize type inference: Bool > Float > Int > String
    if total_values == 0 {
        return DataType::Utf8;
    }

    if has_bool && !has_int && !has_float {
        DataType::Boolean
    } else if has_float {
        DataType::Float64
    } else if has_int {
        DataType::Int64
    } else {
        DataType::Utf8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_schema() {
        let csv_data = b"name,age,city\nAlice,30,NYC\nBob,25,LA";
        let options = CsvFormatOptions::default();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let schema = rt.block_on(infer_schema(csv_data, &options)).unwrap();

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(schema.field(1).name(), "age");
        assert_eq!(schema.field(2).name(), "city");
    }
}

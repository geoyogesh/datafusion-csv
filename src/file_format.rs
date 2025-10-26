//! CSV file format configuration and handling
//!
//! This module provides CSV format configuration options and implements
//! the DataFusion FileFormat trait for independent CSV reading.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr, Statistics};
use object_store::{ObjectMeta, ObjectStore};

use crate::file_source::CsvExec;
use crate::physical_exec;

/// CSV format configuration options
#[derive(Debug, Clone)]
pub struct CsvFormatOptions {
    /// Whether the CSV file has a header row (default: true)
    pub has_header: bool,
    /// The delimiter character (default: b',')
    pub delimiter: u8,
    /// Maximum number of rows to read for schema inference
    pub schema_infer_max_rec: Option<usize>,
    /// Batch size for reading (default: 8192)
    pub batch_size: usize,
    /// File extension to look for (default: ".csv")
    pub file_extension: String,
}

impl Default for CsvFormatOptions {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: b',',
            schema_infer_max_rec: Some(1000),
            batch_size: 8192,
            file_extension: ".csv".to_string(),
        }
    }
}

impl CsvFormatOptions {
    /// Create new CSV format options with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether the CSV has a header row
    pub fn with_has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    /// Set the delimiter character
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Set maximum records for schema inference
    pub fn with_schema_infer_max_rec(mut self, max_rec: Option<usize>) -> Self {
        self.schema_infer_max_rec = max_rec;
        self
    }

    /// Set batch size for reading
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set file extension
    pub fn with_file_extension(mut self, ext: impl Into<String>) -> Self {
        self.file_extension = ext.into();
        self
    }

    /// Get file extension with leading dot
    pub(crate) fn file_extension_with_dot(&self) -> String {
        if self.file_extension.starts_with('.') {
            self.file_extension.clone()
        } else {
            format!(".{}", self.file_extension)
        }
    }
}

/// Independent CSV file format implementation
#[derive(Debug, Clone)]
pub struct CsvFormat {
    options: CsvFormatOptions,
}

impl CsvFormat {
    pub fn new(options: CsvFormatOptions) -> Self {
        Self { options }
    }
}

impl Default for CsvFormat {
    fn default() -> Self {
        Self::new(CsvFormatOptions::default())
    }
}

impl fmt::Display for CsvFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CSV")
    }
}

#[async_trait]
impl FileFormat for CsvFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        self.options.file_extension_with_dot()
    }

    fn get_ext_with_compression(
        &self,
        _c: &datafusion::datasource::file_format::file_compression_type::FileCompressionType,
    ) -> Result<String> {
        Ok(self.get_ext())
    }

    async fn infer_schema(
        &self,
        _state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        if objects.is_empty() {
            return Ok(Arc::new(Schema::empty()));
        }

        // Read the first file to infer schema
        let obj = &objects[0];
        let bytes = store
            .get(&obj.location)
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
            .bytes()
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        // Use our independent schema inference
        let schema = physical_exec::infer_schema(&bytes, &self.options).await?;

        Ok(Arc::new(schema))
    }

    async fn infer_stats(
        &self,
        _state: &SessionState,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        // Return unknown statistics for now
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        _state: &SessionState,
        conf: FileScanConfig,
        _filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Create our custom CSV execution plan
        let exec = CsvExec::new(conf, self.options.clone());
        Ok(Arc::new(exec))
    }
}

/// Helper to detect file extension from path
pub(crate) fn detect_file_extension(path: &str) -> Option<String> {
    std::path::Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_string())
}

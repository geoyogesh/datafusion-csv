//! CSV file source configuration and execution
//!
//! This module provides the execution plan for reading CSV files,
//! using our independent CSV reader implementation.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::project_schema;
use datafusion_physical_expr::EquivalenceProperties;
use object_store::http::HttpBuilder;
use url::Url;

use crate::file_format::{detect_file_extension, CsvFormat, CsvFormatOptions};
use crate::physical_exec::CsvOpener;

/// CSV source builder for creating table providers
pub struct CsvSourceBuilder {
    path: String,
    options: CsvFormatOptions,
}

impl CsvSourceBuilder {
    /// Create a new CSV source builder
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            options: CsvFormatOptions::default(),
        }
    }

    /// Set CSV format options
    pub fn with_options(mut self, options: CsvFormatOptions) -> Self {
        self.options = options;
        self
    }

    /// Set delimiter
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.options = self.options.with_delimiter(delimiter);
        self
    }

    /// Set whether file has header
    pub fn with_has_header(mut self, has_header: bool) -> Self {
        self.options = self.options.with_has_header(has_header);
        self
    }

    /// Build the table provider
    pub async fn build(self, state: &SessionState) -> Result<Arc<dyn TableProvider>> {
        create_csv_table_provider(state, &self.path, self.options).await
    }
}

/// Create a CSV table provider from a path and options
pub async fn create_csv_table_provider(
    state: &SessionState,
    path: &str,
    options: CsvFormatOptions,
) -> Result<Arc<dyn TableProvider>> {
    // Register HTTP object store if the URL is HTTP/HTTPS
    if path.starts_with("http://") || path.starts_with("https://") {
        register_http_object_store(state, path)?;
    }

    let table_url = ListingTableUrl::parse(path)?;

    // Auto-detect file extension if not explicitly set as non-csv
    let extension = if options.file_extension == ".csv" {
        detect_file_extension(path)
            .map(|ext| if ext.starts_with('.') { ext } else { format!(".{}", ext) })
            .unwrap_or_else(|| ".csv".to_string())
    } else {
        options.file_extension_with_dot()
    };

    let format = CsvFormat::new(options);
    let listing_options = ListingOptions::new(Arc::new(format))
        .with_file_extension(&extension);

    let config = ListingTableConfig::new(table_url)
        .with_listing_options(listing_options)
        .infer_schema(state)
        .await?;

    let table = ListingTable::try_new(config)?;

    Ok(Arc::new(table))
}

/// Register HTTP object store for the given URL
fn register_http_object_store(state: &SessionState, url_str: &str) -> Result<()> {
    let url = Url::parse(url_str).map_err(|e| {
        datafusion_common::DataFusionError::Execution(format!("Failed to parse URL: {}", e))
    })?;

    // Extract the base URL (scheme + host + port)
    let base_url = format!(
        "{}://{}",
        url.scheme(),
        url.host_str().ok_or_else(|| {
            datafusion_common::DataFusionError::Execution("URL has no host".to_string())
        })?
    );

    // Build HTTP object store
    let http_store = HttpBuilder::new()
        .with_url(base_url.clone())
        .build()
        .map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Failed to create HTTP object store: {}",
                e
            ))
        })?;

    // Register the object store
    let object_store_url = Url::parse(&base_url).unwrap();
    state
        .runtime_env()
        .register_object_store(&object_store_url, Arc::new(http_store));

    Ok(())
}

/// CSV execution plan that uses our independent CSV reader
#[derive(Debug, Clone)]
pub struct CsvExec {
    /// File scan configuration
    config: FileScanConfig,
    /// CSV format options
    options: CsvFormatOptions,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Plan properties
    properties: PlanProperties,
}

impl CsvExec {
    pub fn new(config: FileScanConfig, options: CsvFormatOptions) -> Self {
        // Calculate the projected schema
        let projected_schema = if let Some(ref proj) = config.projection {
            project_schema(&config.file_schema, Some(proj)).unwrap()
        } else {
            config.file_schema.clone()
        };

        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(config.file_groups.len()),
            ExecutionMode::Bounded,
        );

        Self {
            config,
            options,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
        }
    }

    fn projected_schema(&self) -> SchemaRef {
        if let Some(ref proj) = self.config.projection {
            project_schema(&self.config.file_schema, Some(proj)).unwrap()
        } else {
            self.config.file_schema.clone()
        }
    }
}

impl DisplayAs for CsvExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let file_count: usize = self.config.file_groups.iter().map(|g| g.len()).sum();
                write!(f, "CsvExec: file_groups={{count={}}}", file_count)
            }
        }
    }
}

impl ExecutionPlan for CsvExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "CsvExec"
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema()
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let object_store_url = self.config.object_store_url.clone();
        let object_store = context.runtime_env().object_store(&object_store_url)?;

        let opener = CsvOpener::new(
            self.options.clone(),
            self.config.file_schema.clone(),
            self.config.projection.clone(),
            object_store,
        )
        .with_batch_size(self.options.batch_size);

        // Open files using our CSV opener
        let stream = datafusion::datasource::physical_plan::FileStream::new(
            &self.config,
            partition,
            opener,
            &self.metrics,
        )?;

        Ok(Box::pin(stream))
    }
}

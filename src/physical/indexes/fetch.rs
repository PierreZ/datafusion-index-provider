use crate::physical::record_fetch::{RecordFetchStream, RecordFetcher};
use arrow::array::Array;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// Physical execution plan for fetching table rows based on row identifiers
/// provided by an input plan (typically IndexScanExec).
#[derive(Debug)]
pub struct IndexedTableScanExec {
    /// The plan that provides the row identifiers (e.g., IndexScanExec)
    input: Arc<dyn ExecutionPlan>,
    /// The table provider to fetch full rows from
    table_provider: Arc<dyn TableProvider>,
    /// Name of the table (for display purposes)
    table_name: String,
    /// The schema of the final output, after projection
    schema: SchemaRef,
    /// The indices of the columns to project from the base table
    projection: Option<Vec<usize>>,
    /// Properties cache
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl IndexedTableScanExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_provider: Arc<dyn TableProvider>,
        table_name: String,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let table_schema = table_provider.schema();
        let schema = if let Some(proj) = &projection {
            Arc::new(table_schema.project(proj)?)
        } else {
            table_schema.clone()
        };

        // TODO: Accurately compute properties based on input and table provider
        let properties = Self::compute_properties(input.properties().clone(), schema.clone());

        Ok(Self {
            input,
            table_provider,
            table_name,
            schema,
            projection,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// This function creates the cache object that stores the plan properties
    fn compute_properties(input_properties: PlanProperties, schema: SchemaRef) -> PlanProperties {
        // The output partitioning and execution mode likely depend on the input plan.
        // The equivalence properties and ordering might be affected by the fetch operation.
        // For now, inherit some properties from input, but this needs refinement.
        let eq_properties = EquivalenceProperties::new(schema);

        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            input_properties.boundedness,
        )
    }
}

impl DisplayAs for IndexedTableScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "IndexedTableScanExec: table={}", self.table_name)?;
                if let Some(proj) = &self.projection {
                    write!(f, ", projection={:?}", proj)?;
                }
            }
        }
        Ok(())
    }
}

impl ExecutionPlan for IndexedTableScanExec {
    fn name(&self) -> &str {
        "IndexedTableScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "IndexedTableScanExec expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(IndexedTableScanExec::new(
            children[0].clone(),
            self.table_provider.clone(),
            self.table_name.clone(),
            self.projection.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Input stream provides batches of row IDs (__row_id__)
        let input_stream = self.input.execute(partition, context.clone())?;

        // Create the fetcher responsible for getting data from the table provider
        let fetcher = Box::new(TableRecordFetcher {
            table_provider: self.table_provider.clone(),
            projection: self.projection.clone(),
            output_schema: self.schema(),
            metrics: self.metrics.clone(),
            context: context.clone(),
        });

        // Create the stream that uses the fetcher
        let stream = RecordFetchStream::new(
            input_stream,
            self.schema(),
            partition,
            fetcher,
            self.metrics.clone(),
        );

        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Result<Statistics> {
        // TODO: Estimate statistics based on index selectivity and table stats
        Ok(Statistics::new_unknown(&self.schema()))
    }
}

// --- Helper Struct and Trait Implementation ---

#[derive(Debug)]
struct TableRecordFetcher {
    table_provider: Arc<dyn TableProvider>,
    /// Optional projection for the final output
    projection: Option<Vec<usize>>,
    /// Schema describing the data produced by the fetcher (projected)
    output_schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Task context
    context: Arc<TaskContext>,
}

#[async_trait]
impl RecordFetcher for TableRecordFetcher {
    async fn fetch_record(&mut self, index_batch: RecordBatch) -> Result<RecordBatch> {
        let timer = BaselineMetrics::new(&self.metrics, 0);

        let row_id_array = helper::get_row_ids_from_batch(&index_batch)?;
        let num_rows = row_id_array.len();
        if num_rows == 0 {
            return Ok(RecordBatch::new_empty(self.output_schema.clone()));
        }

        // TODO: Re-implement scan logic once SessionState access from TaskContext is figured out.
        //       The current TableProvider::scan requires &SessionState, which TaskContext doesn't directly provide.
        // let batch_size = self.context.session_config().batch_size();
        // let scan_stream = self
        //     .table_provider
        //     // .scan(self.context.session_state(), None, &[], Some(batch_size)) // BROKEN: needs SessionState
        //     .scan( ??? , None, &[], Some(batch_size))
        //     .await?;
        // let all_provider_batches = common::collect(scan_stream).await?;
        // if all_provider_batches.is_empty() {
        //     return Ok(RecordBatch::new_empty(self.output_schema.clone()));
        // }
        // let first_schema = all_provider_batches[0].schema();
        // let full_provider_batch = concat_batches(&first_schema, &all_provider_batches)?;
        // let row_id_u64_array = row_id_array
        //     .as_any()
        //     .downcast_ref::<arrow::array::UInt64Array>()
        //     .ok_or_else(|| {
        //         DataFusionError::Internal(format!(
        //             "Expected row ID array to be UInt64Array, found {}",
        //             row_id_array.data_type()
        //         ))
        //     })?;
        // let taken_columns: Result<Vec<Arc<dyn Array>>> = full_provider_batch
        //     .columns()
        //     .iter()
        //     .map(|column| take(column, row_id_u64_array, None).map_err(|e| DataFusionError::ArrowError(e)))
        //     .collect();
        // let taken_columns = taken_columns?;
        // let taken_rows_batch = RecordBatch::try_new(full_provider_batch.schema(), taken_columns)?;
        // let final_batch = if let Some(proj_indices) = &self.projection {
        //     taken_rows_batch.project(proj_indices)?
        // } else {
        //     taken_rows_batch
        // };
        // timer.done();
        // Ok(final_batch)

        // Temporary stub implementation
        timer.done();
        println!(
            "WARN: TableRecordFetcher::fetch_record is stubbed due to SessionState access issue."
        );
        Ok(RecordBatch::new_empty(self.output_schema.clone()))
    }
}

// Helper functions
mod helper {
    use crate::physical::indexes::index::ROW_ID_COLUMN_NAME;
    use arrow::array::{ArrayRef, RecordBatch};
    use arrow::datatypes::DataType;
    use datafusion::error::{DataFusionError, Result};

    pub(crate) fn get_row_ids_from_batch(index_batch: &RecordBatch) -> Result<ArrayRef> {
        let row_id_col_index = index_batch.schema().index_of(ROW_ID_COLUMN_NAME)?;
        let row_id_col = index_batch.column(row_id_col_index);

        if !matches!(row_id_col.data_type(), DataType::UInt64) {
            return Err(DataFusionError::Internal(format!(
                "Expected row ID column '{}' to be UInt64, but found {}",
                ROW_ID_COLUMN_NAME,
                row_id_col.data_type()
            )));
        }
        Ok(row_id_col.clone())
    }
}

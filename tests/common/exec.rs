use arrow::array::{Array, UInt64Array};
use arrow::compute::SortOptions;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use futures::{stream::StreamExt, TryFutureExt};
use std::any::Any;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub fn compute_properties(schema: SchemaRef) -> PlanProperties {
    let eq_properties = EquivalenceProperties::new(schema.clone());
    PlanProperties::new(
        eq_properties,
        Partitioning::UnknownPartitioning(1),
        EmissionType::Incremental,
        Boundedness::Bounded,
    )
}

/// Compute properties for a plan that produces output sorted by the 'index' column.
pub fn compute_sorted_properties(schema: SchemaRef) -> PlanProperties {
    // Define the sorting expression for the 'index' column (UInt64, index 0)
    let sort_expr = PhysicalSortExpr {
        expr: Arc::new(datafusion::physical_expr::expressions::Column::new(
            "index", 0,
        )) as Arc<dyn PhysicalExpr>,
        options: SortOptions {
            descending: false,  // Ascending
            nulls_first: false, // Doesn't matter for non-nullable index column
        },
    };
    let ordering = vec![sort_expr];

    // Create EquivalenceProperties using the new_with_orderings constructor
    let eq_properties =
        EquivalenceProperties::new_with_orderings(schema.clone(), &[ordering.into()]);

    // Create PlanProperties using the new constructor
    // Output ordering will be derived from eq_properties
    PlanProperties::new(
        eq_properties,
        Partitioning::UnknownPartitioning(1), // Assuming single partition for now
        EmissionType::Incremental,
        Boundedness::Bounded,
    )
}

/// Custom execution plan for index lookups
#[derive(Debug)]
pub struct IndexLookupExec {
    schema: SchemaRef,
    filtered_indices: Vec<u64>,
    cache: PlanProperties,
}

impl IndexLookupExec {
    pub fn new(schema: SchemaRef, filtered_indices: Vec<u64>, sorted: bool) -> Self {
        let cache = if sorted {
            compute_sorted_properties(schema.clone())
        } else {
            compute_properties(schema.clone())
        };
        IndexLookupExec {
            schema: schema.clone(),
            filtered_indices,
            cache,
        }
    }
}

impl DisplayAs for IndexLookupExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "IndexLookupExec")
    }
}

#[async_trait]
impl ExecutionPlan for IndexLookupExec {
    fn name(&self) -> &str {
        "IndexLookupExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
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
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Convert indices to RecordBatch
        let indices = UInt64Array::from_iter_values(self.filtered_indices.iter().copied());
        let batch = RecordBatch::try_new(self.schema.clone(), vec![Arc::new(indices)])?;

        log::debug!("index lookup Batch: {:?}", batch);

        Ok(Box::pin(MemoryStream::try_new(
            vec![batch],
            self.schema.clone(),
            None,
        )?))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }
}

/// Joins the results from an index scan (row IDs) with the base data.
#[derive(Debug)]
pub struct IndexJoinExec {
    index_exec: Arc<dyn ExecutionPlan>,
    batches: Vec<RecordBatch>,
    projection: Option<Vec<usize>>,
    schema: SchemaRef,
    cache: PlanProperties,
}

impl IndexJoinExec {
    pub fn new(
        index_exec: Arc<dyn ExecutionPlan>,
        batches: Vec<RecordBatch>,
        projection: Option<Vec<usize>>,
        schema: SchemaRef,
    ) -> Self {
        IndexJoinExec {
            index_exec,
            batches,
            projection,
            schema: schema.clone(),
            cache: compute_properties(schema),
        }
    }
}

impl DisplayAs for IndexJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "IndexJoinExec")
    }
}

#[async_trait]
impl ExecutionPlan for IndexJoinExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.index_exec]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(IndexJoinExec::new(
            children[0].clone(),
            self.batches.clone(),
            self.projection.clone(),
            self.schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let _index_schema = self.index_exec.schema();
        let base_schema = self.schema.clone();
        let index_stream_partitioned = self.index_exec.execute(partition, context.clone())?;

        let base_batches = self
            .batches
            .iter()
            .map(|batch| Ok(batch.clone()))
            .collect::<Result<Vec<_>>>()?;
        let base_stream_partitioned = MemoryStream::try_new(base_batches, base_schema, None)?;

        let stream = Box::pin(
            async move {
                // 1. Collect all row IDs from the index stream
                let mut row_ids = HashSet::new();
                let mut index_stream_concrete = index_stream_partitioned;
                while let Some(batch_result) = index_stream_concrete.next().await {
                    let batch = batch_result?;
                    // Assuming the index stream returns a single UInt64 column named "_row_id"
                    let row_id_col = batch
                        .column_by_name("_row_id")
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                "Index stream missing _row_id column".to_string(),
                            )
                        })?
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                "Index stream _row_id column is not UInt64Array".to_string(),
                            )
                        })?;

                    for row_id in row_id_col.values().iter() {
                        row_ids.insert(*row_id);
                    }
                }

                // 2. Filter the base stream using the collected row IDs
                let base_stream_filtered = base_stream_partitioned.map(move |batch_result| {
                    let batch = batch_result?;
                    // Find the _row_id column in the base batch
                    let base_row_id_col = batch
                        .column_by_name("_row_id")
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                "Base stream missing _row_id column".to_string(),
                            )
                        })?
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                "Base stream _row_id column is not UInt64Array".to_string(),
                            )
                        })?;

                    // Create a boolean filter array
                    let filter_array = base_row_id_col
                        .iter()
                        .map(|row_id_opt| {
                            row_id_opt.map_or(None, |row_id| Some(row_ids.contains(&row_id)))
                        })
                        .collect::<arrow::array::BooleanArray>();

                    // Apply the filter
                    let filtered_batch =
                        arrow::compute::filter_record_batch(&batch, &filter_array)?;
                    Ok(filtered_batch)
                });

                Ok(base_stream_filtered)
            }
            .try_flatten_stream(),
        );

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn name(&self) -> &str {
        "IndexJoinExec"
    }
}

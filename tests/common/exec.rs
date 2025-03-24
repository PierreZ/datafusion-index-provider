use arrow::array::{Array, Int32Array, UInt64Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use datafusion_index_provider::physical::record_fetch::{RecordFetchStream, RecordFetcher};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub fn compute_properties(schema: SchemaRef) -> PlanProperties {
    let eq_properties = EquivalenceProperties::new(schema);
    PlanProperties::new(
        eq_properties,
        Partitioning::UnknownPartitioning(1),
        EmissionType::Incremental,
        Boundedness::Bounded,
    )
}

/// Custom execution plan for index lookups
#[derive(Debug)]
pub struct IndexLookupExec {
    schema: SchemaRef,
    filtered_indices: Vec<usize>,
    cache: PlanProperties,
}

impl IndexLookupExec {
    pub fn new(schema: SchemaRef, filtered_indices: Vec<usize>) -> Self {
        IndexLookupExec {
            schema: schema.clone(),
            filtered_indices,
            cache: compute_properties(schema),
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
        let indices =
            UInt64Array::from_iter_values(self.filtered_indices.iter().map(|&i| i as u64));
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

/// Mapper that filters batches using index results
struct BatchMapper {
    batches: Vec<RecordBatch>,
}

impl BatchMapper {
    fn new(batches: Vec<RecordBatch>) -> Self {
        Self { batches }
    }
}

#[async_trait]
impl RecordFetcher for BatchMapper {
    async fn fetch_record(&mut self, index_batch: RecordBatch) -> Result<RecordBatch> {
        log::debug!("Index batch: {:?}", index_batch);
        // Get row indices from the index batch
        let indices = index_batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let row_ids: Vec<usize> = indices.iter().flatten().map(|i| i as usize).collect();

        log::debug!("Row ids: {:?}", row_ids);

        // Apply the row filter to get filtered batch
        apply_row_filter(&self.batches[0], &row_ids)
    }
}

/// Execution plan that joins index results with actual data
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
        // Execute index lookup to get the row indices
        let index_stream = self.index_exec.execute(partition, context.clone())?;

        // Create a mapper that will use the index results to filter batches
        let mapper = Box::new(BatchMapper::new(self.batches.clone()));

        // Create and return a RecordFetchStream that combines the index results with the actual data
        Ok(Box::pin(RecordFetchStream::new(
            index_stream,
            partition,
            mapper,
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

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }
}

fn apply_row_filter(batch: &RecordBatch, row_ids: &[usize]) -> Result<RecordBatch> {
    log::debug!("Row ids: {:?}", row_ids);
    let new_columns: Result<Vec<Arc<dyn Array>>> = batch
        .columns()
        .iter()
        .map(|col| {
            Ok(Arc::new(arrow::compute::take(
                col.as_ref(),
                &Int32Array::from_iter_values(row_ids.iter().map(|&i| i as i32)),
                None,
            )?) as Arc<dyn Array>)
        })
        .collect();

    Ok(RecordBatch::try_new(batch.schema(), new_columns?)?)
}

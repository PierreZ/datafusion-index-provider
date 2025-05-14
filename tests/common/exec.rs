use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use datafusion_index_provider::record_fetch::RecordFetchStream;

use std::any::Any;

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use crate::common::record_fetcher::BatchMapper;

/// Compute properties for a plan that produces output sorted by the 'index' column.
pub fn compute_properties(schema: SchemaRef) -> PlanProperties {
    let eq_properties = EquivalenceProperties::new(schema.clone());
    PlanProperties::new(
        eq_properties,
        Partitioning::UnknownPartitioning(1),
        EmissionType::Incremental,
        Boundedness::Bounded,
    )
}

/// Joins the results from an index scan (row IDs) with the base data.
#[derive(Debug)]
pub struct IndexJoinExec {
    index_exec: Arc<dyn ExecutionPlan>,
    batches: Vec<RecordBatch>,
    projection: Option<Vec<usize>>,
    schema: SchemaRef,
    cache: PlanProperties,
    metrics: Arc<ExecutionPlanMetricsSet>, // Added metrics field
}

impl IndexJoinExec {
    pub fn new(
        index_exec: Arc<dyn ExecutionPlan>,
        batches: Vec<RecordBatch>,
        projection: Option<Vec<usize>>,
        schema: SchemaRef,
    ) -> Self {
        let metrics = Arc::new(ExecutionPlanMetricsSet::new()); // Create metrics
        IndexJoinExec {
            index_exec,
            batches,
            projection,
            schema: schema.clone(),
            cache: compute_properties(schema),
            metrics, // Store metrics
        }
    }
}

impl DisplayAs for IndexJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "IndexJoinExec: projection={}, batches={}",
                    self.projection.is_some(), // Show if projection is applied
                    self.batches.len()         // Show number of base batches
                )?;
                // Optionally, in verbose mode, display the child plan
                // Use pattern matching for enum comparison
                if matches!(
                    t,
                    DisplayFormatType::Verbose | DisplayFormatType::TreeRender
                ) {
                    write!(f, ", child=")?;
                    self.index_exec.fmt_as(t, f)?; // Display child plan details
                }
            }
        }
        Ok(())
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
        let index_stream_partitioned = self.index_exec.execute(partition, context.clone())?;

        // Create BatchMapper instance
        let mapper = BatchMapper::new(self.batches.clone());

        // Create RecordFetchStream
        let stream = RecordFetchStream::new(
            index_stream_partitioned,
            self.metrics.clone(), // Pass metrics to RecordFetchStream
            partition,
            Box::new(mapper),
        );

        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner()) // Expose metrics
    }

    fn name(&self) -> &str {
        "IndexJoinExec"
    }
}

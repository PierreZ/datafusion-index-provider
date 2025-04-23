use arrow::array::UInt64Array;
use arrow::compute::SortOptions;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::MetricsSet;
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
    sorted: bool, // Add sorted field
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
            sorted, // Store sorted flag
        }
    }
}

impl DisplayAs for IndexLookupExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        // Include index count and sorted status
        write!(
            f,
            "IndexLookupExec: indices_count={}, sorted={}",
            self.filtered_indices.len(),
            self.sorted
        )
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
            partition, // Pass the partition number
            Box::new(mapper),
        );

        Ok(Box::pin(stream))
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

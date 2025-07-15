use crate::physical_plan::Index;
use arrow::datatypes::SchemaRef;
use datafusion::{
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    },
    prelude::Expr,
};
use datafusion_common::DataFusionError;
use std::sync::Arc;

#[derive(Debug)]
pub struct IndexScanExec {
    /// The index to scan.
    index: Arc<dyn Index>,
    /// The filters to apply to the index.
    filters: Vec<Expr>,
    /// The limit to apply to the index.
    limit: Option<usize>,
    /// Properties of the plan.
    plan_properties: PlanProperties,
}

impl DisplayAs for IndexScanExec {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "IndexScanExec: index={}", self.index.name()),
            DisplayFormatType::Verbose => write!(
                f,
                "IndexScanExec: index={}, filters={:?}, limit={:?}",
                self.index.name(),
                self.filters,
                self.limit
            ),
            DisplayFormatType::TreeRender => write!(f, "IndexScanExec"),
        }
    }
}

impl ExecutionPlan for IndexScanExec {
    fn name(&self) -> &str {
        "IndexScanExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    // Return an empty list since this plan does not have any children.
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Internal(
                "IndexScanExec only supports a single partition".to_string(),
            ));
        }

        self.index.scan(&self.filters, self.limit)
    }
}

impl IndexScanExec {
    pub fn try_new(
        index: Arc<dyn Index>,
        filters: Vec<Expr>,
        limit: Option<usize>,
        schema: SchemaRef,
    ) -> Result<Self, DataFusionError> {
        let plan_properties = Self::compute_properties(schema);
        Ok(Self {
            index,
            filters,
            limit,
            plan_properties,
        })
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);

        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::ROW_ID_COLUMN_NAME;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::Statistics;
    use datafusion::physical_plan::memory::MemoryStream;
    use std::any::Any;
    use std::sync::Mutex;

    #[derive(Debug)]
    struct MockIndex {
        schema: SchemaRef,
        scan_called: Mutex<bool>,
    }

    impl MockIndex {
        fn new() -> Self {
            Self {
                schema: Arc::new(Schema::new(vec![Field::new(
                    ROW_ID_COLUMN_NAME,
                    DataType::UInt64,
                    false,
                )])),
                scan_called: Mutex::new(false),
            }
        }
    }

    impl Index for MockIndex {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn name(&self) -> &str {
            "mock_index"
        }

        fn index_schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn table_name(&self) -> &str {
            "mock_table"
        }

        fn column_name(&self) -> &str {
            "mock_column"
        }

        fn scan(
            &self,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<SendableRecordBatchStream, DataFusionError> {
            *self.scan_called.lock().unwrap() = true;
            let batch = RecordBatch::new_empty(self.schema.clone());
            let stream = MemoryStream::try_new(vec![batch], self.schema.clone(), None)?;
            Ok(Box::pin(stream))
        }

        fn statistics(&self) -> Statistics {
            Statistics::new_unknown(&self.schema)
        }
    }

    #[tokio::test]
    async fn test_index_scan_exec() -> datafusion::common::Result<()> {
        let index = Arc::new(MockIndex::new());
        let schema = index.index_schema();
        let exec = IndexScanExec::try_new(index.clone(), vec![], None, schema)?;

        // execute
        let task_ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, task_ctx)?;
        let batches = datafusion::physical_plan::common::collect(stream).await?;

        assert!(*index.scan_called.lock().unwrap());
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_index_scan_exec_invalid_partition() {
        let index = Arc::new(MockIndex::new());
        let schema = index.index_schema();
        let exec = IndexScanExec::try_new(index.clone(), vec![], None, schema).unwrap();

        let task_ctx = Arc::new(TaskContext::default());
        let res = exec.execute(1, task_ctx);
        match res {
            Err(e) => {
                assert!(
                    e.to_string()
                        .contains("IndexScanExec only supports a single partition"),
                    "unexpected error message: {}",
                    e
                );
            }
            Ok(_) => panic!("expected an error"),
        }
    }
}

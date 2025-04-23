use crate::physical::indexes::index::Index;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use datafusion::prelude::Expr;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

pub const ROW_ID_COLUMN_NAME: &str = "__row_id__";

#[derive(Debug)]
pub struct IndexScanExec {
    index: Arc<dyn Index>,
    predicate: Expr,
    properties: PlanProperties,
    schema: SchemaRef,
}

impl IndexScanExec {
    pub fn new(index: Arc<dyn Index>, predicate: Expr) -> Result<Self> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            ROW_ID_COLUMN_NAME,
            DataType::UInt64,
            false, // Row IDs are typically not nullable
        )]));

        let eq_properties = EquivalenceProperties::new(schema.clone());
        let partitioning = Partitioning::UnknownPartitioning(1); // TODO: Determine partitioning based on index
        let emission_type = EmissionType::Incremental; // Index scan produces batches incrementally
        let boundedness = Boundedness::Bounded; // Use Bounded until calculation is implemented

        let properties =
            PlanProperties::new(eq_properties, partitioning, emission_type, boundedness);

        Ok(Self {
            index,
            predicate,
            properties,
            schema,
        })
    }

    pub fn index(&self) -> &Arc<dyn Index> {
        &self.index
    }

    pub fn predicate(&self) -> &Expr {
        &self.predicate
    }
}

impl DisplayAs for IndexScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "IndexScanExec: index={}", self.index.name())?;
                write!(f, ", table={}", self.index.table_name())?;
                write!(f, ", predicate={:?}", self.predicate)?;
            }
        }
        Ok(())
    }
}

impl ExecutionPlan for IndexScanExec {
    fn name(&self) -> &str {
        "IndexScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
        _context: Arc<TaskContext>, // Context not used by Index::scan directly
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "IndexScanExec does not support partitioning, tried to execute partition {}",
                partition
            )));
        }

        self.index.scan(&self.predicate, None)
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.index.statistics())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::indexes::index::Index;
    use arrow::array::UInt64Array;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef}; // For MemoryStream
    use arrow::record_batch::RecordBatch;
    use datafusion::error::Result;
    use datafusion::execution::context::TaskContext;
    use datafusion::logical_expr::{col, Expr}; // Import col function and Expr
    use datafusion::physical_plan::memory::MemoryStream;
    use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream, Statistics};
    use datafusion_common::stats::Precision; // Import Precision
    use datafusion_common::ScalarValue;
    use futures::stream::TryStreamExt; // For try_collect

    use std::any::Any;
    use std::sync::Arc;

    // A mock index for testing purposes
    #[derive(Debug)]
    struct MockIndex {
        name: String,
        table_name: String,
        schema: SchemaRef,
        stats: Statistics,
        scan_result: Option<Result<Vec<RecordBatch>>>, // Store data to return from scan
    }

    impl MockIndex {
        fn new() -> Self {
            Self {
                name: "mock_index".to_string(),
                table_name: "mock_table".to_string(),
                schema: Self::default_schema(),
                scan_result: None, // Default to no result
                stats: Statistics::new_unknown(&Schema::empty()), // Default stats
            }
        }

        fn with_stats(mut self, stats: Statistics) -> Self {
            self.stats = stats;
            self
        }

        fn with_scan_result(mut self, result: Result<Vec<RecordBatch>>) -> Self {
            self.scan_result = Some(result);
            self
        }

        fn default_schema() -> SchemaRef {
            Arc::new(Schema::new(vec![Field::new(
                ROW_ID_COLUMN_NAME,
                DataType::UInt64,
                false,
            )]))
        }
    }

    impl Index for MockIndex {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn name(&self) -> &str {
            &self.name
        }

        fn index_schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn table_name(&self) -> &str {
            &self.table_name
        }

        // For basic tests, assume any relevant predicate is supported
        fn supports_predicate(&self, _predicate: &Expr) -> Result<bool> {
            Ok(true)
        }

        // Return an empty stream for now, specific tests can override this
        fn scan(
            &self,
            _predicate: &Expr,
            _projection: Option<&Vec<usize>>,
        ) -> Result<SendableRecordBatchStream> {
            let data = match &self.scan_result {
                Some(Ok(batches)) => batches.clone(),
                Some(Err(e)) => {
                    return Err(DataFusionError::Execution(format!(
                        "Mock scan error: {}",
                        e
                    )))
                } // Propagate error
                None => vec![], // Return empty stream if no result set
            };
            let stream = MemoryStream::try_new(data, self.schema.clone(), None)?;
            Ok(Box::pin(stream))
        }

        fn statistics(&self) -> Statistics {
            self.stats.clone()
        }
    }

    #[test]
    fn test_index_scan_new() -> Result<()> {
        let mock_index = Arc::new(MockIndex::new());
        let predicate = col("a").eq(Expr::Literal(ScalarValue::Int32(Some(1)))); // Example predicate

        let index_scan_exec = IndexScanExec::new(mock_index.clone(), predicate)?;

        // Verify schema
        let expected_schema = Arc::new(Schema::new(vec![Field::new(
            ROW_ID_COLUMN_NAME,
            DataType::UInt64,
            false,
        )]));
        assert_eq!(index_scan_exec.schema(), expected_schema);

        // Verify properties (basic check)
        assert_eq!(
            index_scan_exec.properties().emission_type, // Access field directly
            EmissionType::Incremental
        );
        // TODO: Add more checks for partitioning, boundedness, equivalence properties if needed

        Ok(())
    }

    #[test]
    fn test_index_scan_statistics() -> Result<()> {
        let expected_stats = Statistics {
            num_rows: Precision::Exact(100),        // Use Precision::Exact
            total_byte_size: Precision::Exact(800), // Use Precision::Exact
            column_statistics: vec![],              // Use empty vec instead of None
        };
        let mock_index = Arc::new(MockIndex::new().with_stats(expected_stats.clone()));
        let predicate = col("a").eq(Expr::Literal(ScalarValue::Int32(Some(1))));
        let index_scan_exec = IndexScanExec::new(mock_index, predicate)?;

        assert_eq!(index_scan_exec.statistics()?, expected_stats);
        Ok(())
    }

    #[tokio::test]
    async fn test_index_scan_execute() -> Result<()> {
        // 1. Prepare expected data
        let expected_row_ids = vec![10_u64, 20, 30];
        let schema = MockIndex::default_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt64Array::from(expected_row_ids.clone()))],
        )?;

        // 2. Setup MockIndex and IndexScanExec
        let mock_index = Arc::new(MockIndex::new().with_scan_result(Ok(vec![batch.clone()])));
        let predicate = col("a").eq(Expr::Literal(ScalarValue::Int32(Some(1))));
        let index_scan_exec = Arc::new(IndexScanExec::new(mock_index, predicate)?);

        // 3. Execute and collect
        let task_context = Arc::new(TaskContext::default());
        let stream = index_scan_exec.execute(0, task_context)?; // No need to clone context anymore
        let collected_batches: Vec<RecordBatch> = stream.try_collect().await?; // Use try_collect on the stream

        // 4. Assert results
        assert_eq!(collected_batches.len(), 1);
        assert_eq!(collected_batches[0], batch);
        Ok(())
    }
}

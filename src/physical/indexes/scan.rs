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

// Make the constant private to ensure the name is controlled within this module
const ROW_ID_COLUMN_NAME: &str = "__row_id__";

/// Creates the standard schema for index scan results (a single __row_id column)
/// with the specified data type.
pub fn index_scan_schema(data_type: DataType) -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        ROW_ID_COLUMN_NAME,
        data_type,
        false, // Typically, row IDs are not nullable
    )]))
}

/// Physical plan node for scanning a single index based on a filter.
#[derive(Debug)]
pub struct IndexScanExec {
    index: Arc<dyn Index>,
    predicate: Expr,
    properties: PlanProperties,
    schema: SchemaRef,
}

impl IndexScanExec {
    pub fn new(index: Arc<dyn Index>, predicate: Expr) -> Result<Self> {
        // Use the helper function with the standard UInt64 type
        let schema = index_scan_schema(DataType::UInt64);

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
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "IndexScanExec: index={}", self.index.name())?;
                write!(f, ", table={}", self.index.table_name())?;
                // Use Display format for the predicate for better readability
                write!(f, ", predicate={}", self.predicate)?;
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
    use arrow::datatypes::{DataType, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use datafusion::error::Result;
    use datafusion::execution::context::TaskContext;
    use datafusion::logical_expr::{col, Expr};
    use datafusion::physical_plan::memory::MemoryStream;
    use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream, Statistics};
    use datafusion_common::stats::Precision;
    use datafusion_common::ScalarValue;
    use futures::stream::TryStreamExt;

    use std::any::Any;
    use std::sync::Arc;

    // A mock index for testing purposes
    #[derive(Debug)]
    struct MockIndex {
        name: String,
        table_name: String,
        schema: SchemaRef,
        stats: Statistics,
        scan_result: Option<Result<Vec<RecordBatch>>>,
    }

    impl MockIndex {
        fn new() -> Self {
            Self {
                name: "mock_index".to_string(),
                table_name: "mock_table".to_string(),
                // Use the helper function with the standard UInt64 type
                schema: index_scan_schema(DataType::UInt64),
                scan_result: None,
                stats: Statistics::new_unknown(&Schema::empty()),
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

        fn supports_predicate(&self, _predicate: &Expr) -> Result<bool> {
            Ok(true)
        }

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
                }
                None => vec![],
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
        let predicate = col("a").eq(Expr::Literal(ScalarValue::Int32(Some(1))));

        let index_scan_exec = IndexScanExec::new(mock_index.clone(), predicate)?;

        // Verify schema
        // Use the helper function with the standard UInt64 type
        let expected_schema = index_scan_schema(DataType::UInt64);
        assert_eq!(index_scan_exec.schema(), expected_schema);

        // Verify properties (basic check)
        assert_eq!(
            index_scan_exec.properties().emission_type,
            EmissionType::Incremental
        );

        Ok(())
    }

    #[test]
    fn test_index_scan_statistics() -> Result<()> {
        let expected_stats = Statistics {
            num_rows: Precision::Exact(100),
            total_byte_size: Precision::Exact(800),
            column_statistics: vec![],
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
        // Use the helper function with the standard UInt64 type
        let schema = index_scan_schema(DataType::UInt64);
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
        let stream = index_scan_exec.execute(0, task_context)?;
        let collected_batches: Vec<RecordBatch> = stream.try_collect().await?;

        // 4. Assert results
        assert_eq!(collected_batches.len(), 1);
        assert_eq!(collected_batches[0], batch);
        Ok(())
    }
}

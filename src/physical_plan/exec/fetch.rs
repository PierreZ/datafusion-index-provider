use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::common::Statistics;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::execution::SendableRecordBatchStream;
use futures::FutureExt;
use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
};
use futures::stream::{Stream, StreamExt};

use crate::physical_plan::exec::index::IndexScanExec;
use crate::physical_plan::fetcher::RecordFetcher;
use crate::physical_plan::joins::try_create_index_lookup_join;
use crate::types::{IndexFilter, IndexFilters};

/// Physical plan node for fetching records from a [`RecordFetcher`] using
/// row IDs produced by one or more index scans.
///
/// This operator takes one or more [`IndexFilter`]s, builds an input plan
/// to produce row IDs (by scanning and joining index results), and then uses
/// a [`RecordFetcher`] to retrieve the actual data for those row IDs.
#[derive(Debug)]
pub struct RecordFetchExec {
    indexes: Arc<IndexFilters>,
    limit: Option<usize>,
    plan_properties: PlanProperties,
    record_fetcher: Arc<dyn RecordFetcher>,
    /// The input plan that produces the row IDs.
    input: Arc<dyn ExecutionPlan>,
    metrics: ExecutionPlanMetricsSet,
    schema: SchemaRef,
}

impl RecordFetchExec {
    /// Create a new `RecordFetchExec` plan.
    pub fn try_new(
        indexes: Vec<IndexFilter>,
        limit: Option<usize>,
        record_fetcher: Arc<dyn RecordFetcher>,
        schema: SchemaRef,
    ) -> Result<Self> {
        if indexes.is_empty() {
            return Err(DataFusionError::Plan(
                "RecordFetchExec requires at least one index".to_string(),
            ));
        }

        let input = Self::build_input_plan(indexes.clone(), limit)?;
        let eq_properties = EquivalenceProperties::new(schema.clone());
        let plan_properties = PlanProperties::new(
            eq_properties,
            input.properties().output_partitioning().clone(),
            input.properties().emission_type,
            input.properties().boundedness,
        );

        Ok(Self {
            indexes: indexes.into(),
            limit,
            plan_properties,
            record_fetcher,
            input,
            metrics: ExecutionPlanMetricsSet::new(),
            schema,
        })
    }

    /// Build the input plan that produces the row IDs.
    ///
    /// If there is a single index, the input plan is an `IndexScanExec`.
    /// If there are multiple indexes, the input plans are `IndexScanExec`s joined
    /// together using `IndexLookupJoin`s.
    fn build_input_plan(
        indexes: Vec<IndexFilter>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut plans = indexes
            .iter()
            .map(|(index, filters)| -> Result<Arc<dyn ExecutionPlan>> {
                Ok(Arc::new(IndexScanExec::try_new(
                    index.clone(),
                    filters.clone(),
                    limit,
                    index.index_schema(),
                )?))
            })
            .collect::<Result<Vec<Arc<dyn ExecutionPlan>>>>()?;

        if plans.len() == 1 {
            return Ok(plans.remove(0));
        }

        let mut left = plans.remove(0);
        while !plans.is_empty() {
            let right = plans.remove(0);
            left = try_create_index_lookup_join(left, right)?;
        }

        Ok(left)
    }
}

impl DisplayAs for RecordFetchExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let index_names: Vec<_> = self.indexes.iter().map(|(i, _)| i.name()).collect();
                write!(
                    f,
                    "RecordFetchExec: indexes=[{}], limit={:?}",
                    index_names.join(", "),
                    self.limit
                )
            }
            DisplayFormatType::TreeRender => write!(f, "RecordFetchExec"),
        }
    }
}

impl ExecutionPlan for RecordFetchExec {
    /// Return a reference to the name of this execution plan.
    fn name(&self) -> &str {
        "RecordFetchExec"
    }

    /// Return a reference to the logical plan as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema of this execution plan
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Get the properties for this execution plan
    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    /// Returns the children of this [`ExecutionPlan`].
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    /// Create a new [`ExecutionPlan`] with new children.
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "RecordFetchExec should have exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(RecordFetchExec {
            indexes: self.indexes.clone(),
            limit: self.limit,
            plan_properties: self.plan_properties.clone(),
            record_fetcher: self.record_fetcher.clone(),
            input: children[0].clone(),
            metrics: self.metrics.clone(),
            schema: self.schema.clone(),
        }))
    }

    /// Executes this plan and returns a stream of `RecordBatch`es.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        Ok(Box::pin(RecordFetchStream::new(
            input_stream,
            self.record_fetcher.clone(),
            baseline_metrics,
        )))
    }

    /// Get the statistics for this execution plan.
    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }
}

/// A stream that fetches records using row IDs from an input stream.
pub struct RecordFetchStream {
    /// The schema of the output data.
    schema: SchemaRef,
    /// The stream of row IDs to fetch.
    input: Option<SendableRecordBatchStream>,
    /// The fetcher implementation.
    fetcher: Arc<dyn RecordFetcher>,
    /// Execution metrics.
    baseline_metrics: BaselineMetrics,
}

impl RecordFetchStream {
    /// Create a new `RecordFetchStream`.
    pub fn new(
        input: SendableRecordBatchStream,
        fetcher: Arc<dyn RecordFetcher>,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        let schema = fetcher.schema();
        Self {
            schema,
            input: Some(input),
            fetcher,
            baseline_metrics,
        }
    }
}

impl Stream for RecordFetchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = match &mut self.input {
            // input has been cleared
            None => Poll::Ready(None),
            Some(input) => match input.poll_next_unpin(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
                Poll::Ready(Some(Ok(record_batch))) => {
                    match self.fetcher.fetch(record_batch).poll_unpin(cx) {
                        Poll::Ready(record) => Poll::Ready(Some(record)),
                        Poll::Pending => Poll::Pending,
                    }
                }
            },
        };
        self.baseline_metrics.record_poll(poll)
    }
}

impl fmt::Debug for RecordFetchStream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("RecordFetchStream")
            .field("schema", &self.schema)
            .field("baseline_metrics", &self.baseline_metrics)
            .finish_non_exhaustive()
    }
}

impl RecordBatchStream for RecordFetchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::create_index_schema;
    use crate::physical_plan::Index;
    use crate::physical_plan::ROW_ID_COLUMN_NAME;
    use arrow::array::UInt64Array;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use datafusion::common::Statistics;
    use datafusion::logical_expr::Expr;
    use datafusion::physical_plan::joins::HashJoinExec;
    use datafusion::physical_plan::memory::MemoryStream;
    use std::any::Any;
    use std::sync::Mutex;

    // --- Mock Index ---
    #[derive(Debug)]
    struct MockIndex {
        schema: SchemaRef,
        scan_called: Mutex<bool>,
        batches: Vec<RecordBatch>,
    }

    impl MockIndex {
        fn new(batches: Vec<RecordBatch>) -> Self {
            Self {
                schema: create_index_schema(DataType::UInt64),
                scan_called: Mutex::new(false),
                batches,
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
        ) -> Result<SendableRecordBatchStream> {
            *self.scan_called.lock().unwrap() = true;
            let stream = MemoryStream::try_new(self.batches.clone(), self.schema.clone(), None)?;
            Ok(Box::pin(stream))
        }

        fn statistics(&self) -> Statistics {
            Statistics::new_unknown(&self.schema)
        }
    }

    // --- Mock Record Fetcher ---
    #[derive(Debug, Clone)]
    struct MockRecordFetcher {
        schema: SchemaRef,
    }

    impl MockRecordFetcher {
        fn new() -> Self {
            Self {
                schema: Arc::new(Schema::new(vec![
                    Field::new(ROW_ID_COLUMN_NAME, DataType::UInt64, false),
                    Field::new("name", DataType::Utf8, false),
                ])),
            }
        }

        fn with_data(self) -> impl RecordFetcher {
            #[derive(Debug)]
            struct MockFetcherWithData {
                schema: SchemaRef,
            }

            #[async_trait]
            impl RecordFetcher for MockFetcherWithData {
                fn schema(&self) -> SchemaRef {
                    self.schema.clone()
                }

                async fn fetch(&self, index_batch: RecordBatch) -> Result<RecordBatch> {
                    let row_ids = index_batch
                        .column_by_name(ROW_ID_COLUMN_NAME)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();

                    let names: Vec<_> = row_ids
                        .values()
                        .iter()
                        .map(|id| format!("name_{id}"))
                        .collect();

                    Ok(RecordBatch::try_new(
                        self.schema.clone(),
                        vec![
                            Arc::new(row_ids.clone()),
                            Arc::new(arrow::array::StringArray::from(names)),
                        ],
                    )?)
                }
            }

            MockFetcherWithData {
                schema: self.schema,
            }
        }
    }

    #[async_trait]
    impl RecordFetcher for MockRecordFetcher {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        async fn fetch(&self, _index_batch: RecordBatch) -> Result<RecordBatch> {
            unimplemented!("MockRecordFetcher::fetch should not be called in these tests")
        }
    }

    // --- Tests ---

    #[tokio::test]
    async fn test_record_fetch_exec_no_indexes() {
        let fetcher = Arc::new(MockRecordFetcher::new());
        let err =
            RecordFetchExec::try_new(vec![], None, fetcher, Arc::new(Schema::empty())).unwrap_err();
        assert!(
            matches!(err, DataFusionError::Plan(ref msg) if msg == "RecordFetchExec requires at least one index"),
            "Unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn test_record_fetch_exec_single_index() -> Result<()> {
        let index_batch = RecordBatch::try_from_iter(vec![(
            ROW_ID_COLUMN_NAME,
            Arc::new(UInt64Array::from(vec![1, 3])) as _,
        )])?;
        let index = Arc::new(MockIndex::new(vec![index_batch]));
        let indexes: Vec<IndexFilter> = vec![(index.clone() as Arc<dyn Index>, vec![])];

        let fetcher = Arc::new(MockRecordFetcher::new());
        let exec = RecordFetchExec::try_new(indexes, None, fetcher, Arc::new(Schema::empty()))?;

        // The input plan should be just the IndexScanExec
        assert_eq!(exec.input.name(), "IndexScanExec");
        Ok(())
    }

    #[tokio::test]
    async fn test_record_fetch_exec_multiple_indexes() -> Result<()> {
        // Create two indexes that return different row IDs
        let index1_batch = RecordBatch::try_from_iter(vec![(
            ROW_ID_COLUMN_NAME,
            Arc::new(UInt64Array::from(vec![1, 3])) as _,
        )])?;
        let index1 = Arc::new(MockIndex::new(vec![index1_batch]));

        let index2_batch = RecordBatch::try_from_iter(vec![(
            ROW_ID_COLUMN_NAME,
            Arc::new(UInt64Array::from(vec![3, 5])) as _,
        )])?;
        let index2 = Arc::new(MockIndex::new(vec![index2_batch]));

        let indexes: Vec<IndexFilter> = vec![
            (index1 as Arc<dyn Index>, vec![]),
            (index2 as Arc<dyn Index>, vec![]),
        ];

        let fetcher = Arc::new(MockRecordFetcher::new());
        let exec = RecordFetchExec::try_new(indexes, None, fetcher, Arc::new(Schema::empty()))?;

        // The input plan should be a HashJoinExec
        assert_eq!(exec.input.name(), "HashJoinExec");
        Ok(())
    }

    #[tokio::test]
    async fn test_record_fetch_exec_five_indexes() -> Result<()> {
        let mut indexes_vec = Vec::new();
        for i in 0..5 {
            let batch = RecordBatch::try_from_iter(vec![(
                ROW_ID_COLUMN_NAME,
                Arc::new(UInt64Array::from(vec![i, i + 1, i + 2])) as _,
            )])?;
            indexes_vec.push((
                Arc::new(MockIndex::new(vec![batch])) as Arc<dyn Index>,
                vec![],
            ));
        }

        let indexes = indexes_vec;
        let fetcher = Arc::new(MockRecordFetcher::new());
        let exec = RecordFetchExec::try_new(indexes, None, fetcher, Arc::new(Schema::empty()))?;

        // The input plan should be a tree of HashJoinExecs
        assert_eq!(exec.input.name(), "HashJoinExec");

        fn count_joins(plan: &Arc<dyn ExecutionPlan>) -> usize {
            if let Some(join_exec) = plan.as_any().downcast_ref::<HashJoinExec>() {
                1 + count_joins(join_exec.children()[0]) + count_joins(join_exec.children()[1])
            } else {
                0
            }
        }

        let join_count = count_joins(&exec.input);
        assert_eq!(join_count, 4, "Expected 4 joins for 5 indexes");

        Ok(())
    }

    #[tokio::test]
    async fn test_record_fetch_exec_execute() -> Result<()> {
        // 1. Setup mocks
        let index_batch = RecordBatch::try_from_iter(vec![(
            ROW_ID_COLUMN_NAME,
            Arc::new(UInt64Array::from(vec![1, 3, 5])) as _,
        )])?;
        let index = Arc::new(MockIndex::new(vec![index_batch]));
        let indexes = vec![(index as Arc<dyn Index>, vec![])];

        let fetcher = Arc::new(MockRecordFetcher::new().with_data());
        let schema = fetcher.schema();

        // 2. Create exec plan
        let exec = RecordFetchExec::try_new(indexes, None, fetcher, schema.clone())?;

        // 3. Execute and collect results
        let task_ctx = Arc::new(TaskContext::default());
        let mut stream = exec.execute(0, task_ctx)?;
        let mut results = Vec::new();
        while let Some(batch) = stream.next().await {
            results.push(batch?);
        }

        // 4. Assert results
        let expected_names = vec!["name_1", "name_3", "name_5"];
        let expected_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![1, 3, 5])),
                Arc::new(arrow::array::StringArray::from(expected_names)),
            ],
        )?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0], expected_batch);

        Ok(())
    }

    #[tokio::test]
    async fn test_record_fetch_exec_execute_empty_input() -> Result<()> {
        // 1. Setup mocks with no batches
        let index = Arc::new(MockIndex::new(vec![]));
        let indexes = vec![(index as Arc<dyn Index>, vec![])];
        let fetcher = Arc::new(MockRecordFetcher::new().with_data());

        // 2. Create exec plan
        let exec = RecordFetchExec::try_new(indexes, None, fetcher, Arc::new(Schema::empty()))?;

        // 3. Execute and collect results
        let task_ctx = Arc::new(TaskContext::default());
        let mut stream = exec.execute(0, task_ctx)?;
        let mut results = Vec::new();
        while let Some(batch) = stream.next().await {
            results.push(batch?);
        }

        // 4. Assert results are empty
        assert!(results.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_record_fetch_exec_execute_multiple_batches() -> Result<()> {
        // 1. Setup mocks with multiple batches
        let batch1 = RecordBatch::try_from_iter(vec![(
            ROW_ID_COLUMN_NAME,
            Arc::new(UInt64Array::from(vec![1, 3])) as _,
        )])?;
        let batch2 = RecordBatch::try_from_iter(vec![(
            ROW_ID_COLUMN_NAME,
            Arc::new(UInt64Array::from(vec![5, 7])) as _,
        )])?;
        let index = Arc::new(MockIndex::new(vec![batch1, batch2]));
        let indexes = vec![(index as Arc<dyn Index>, vec![])];
        let fetcher = Arc::new(MockRecordFetcher::new().with_data());
        let schema = fetcher.schema();

        // 2. Create exec plan
        let exec = RecordFetchExec::try_new(indexes, None, fetcher, schema.clone())?;

        // 3. Execute and collect results
        let task_ctx = Arc::new(TaskContext::default());
        let results =
            datafusion::physical_plan::common::collect(exec.execute(0, task_ctx)?).await?;

        // 4. Assert results
        let expected_batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![1, 3])),
                Arc::new(arrow::array::StringArray::from(vec!["name_1", "name_3"])),
            ],
        )?;
        let expected_batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![5, 7])),
                Arc::new(arrow::array::StringArray::from(vec!["name_5", "name_7"])),
            ],
        )?;

        assert_eq!(results.len(), 2);
        assert_eq!(results[0], expected_batch1);
        assert_eq!(results[1], expected_batch2);

        Ok(())
    }

    #[tokio::test]
    async fn test_record_fetch_exec_fetcher_error() -> Result<()> {
        // 1. Setup mocks
        #[derive(Debug)]
        struct ErrorFetcher;
        #[async_trait]
        impl RecordFetcher for ErrorFetcher {
            fn schema(&self) -> SchemaRef {
                Arc::new(Schema::empty())
            }
            async fn fetch(&self, _index_batch: RecordBatch) -> Result<RecordBatch> {
                Err(DataFusionError::Execution("fetcher error".to_string()))
            }
        }

        let index_batch = RecordBatch::try_from_iter(vec![(
            ROW_ID_COLUMN_NAME,
            Arc::new(UInt64Array::from(vec![1])) as _,
        )])?;
        let index = Arc::new(MockIndex::new(vec![index_batch]));
        let indexes = vec![(index as Arc<dyn Index>, vec![])];
        let fetcher = Arc::new(ErrorFetcher);

        // 2. Create exec plan
        let exec = RecordFetchExec::try_new(indexes, None, fetcher, Arc::new(Schema::empty()))?;

        // 3. Execute and expect an error
        let task_ctx = Arc::new(TaskContext::default());
        let result = datafusion::physical_plan::common::collect(exec.execute(0, task_ctx)?).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DataFusionError::Execution(_)));

        Ok(())
    }
}

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::common::Statistics;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::execution::SendableRecordBatchStream;
use futures::future::BoxFuture;
use futures::FutureExt;

use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PlanProperties,
    RecordBatchStream,
};
use futures::stream::{Stream, StreamExt};

use crate::physical_plan::exec::index::IndexScanExec;
use crate::physical_plan::fetcher::RecordFetcher;
use crate::physical_plan::joins::try_create_index_lookup_join;
use crate::physical_plan::{create_index_schema, ROW_ID_COLUMN_NAME};
use crate::types::{IndexFilter, IndexFilters};
use datafusion::arrow::datatypes::Schema;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::PhysicalExpr;

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

        if indexes.len() > 1 {
            return Err(DataFusionError::Internal(
                "RecordFetchExec expects a single root IndexFilter".to_string(),
            ));
        }

        let input = match indexes.first() {
            Some(index_filter) => Self::build_scan_exec(index_filter, limit)?,
            None => {
                return Err(DataFusionError::Plan(
                    "RecordFetchExec requires at least one index".to_string(),
                ));
            }
        };
        let eq_properties = EquivalenceProperties::new(schema.clone());
        let plan_properties = PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
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

    /// Builds the input execution plan that produces row IDs based on the IndexFilter structure.
    ///
    /// This method is the core of the index-based execution plan generation. It recursively
    /// processes the [`IndexFilter`] tree to create an optimized physical plan that efficiently
    /// produces row IDs matching the query filters.
    ///
    /// # Plan Generation Strategy
    ///
    /// The method generates different execution plans based on the [`IndexFilter`] variant:
    ///
    /// ## [`IndexFilter::Single`] - Direct Index Scan
    /// Creates a single [`IndexScanExec`] that directly scans the specified index with the given filter.
    /// This is the most efficient case with minimal overhead.
    ///
    /// ```text
    /// IndexScanExec(index, filter)
    /// ```
    ///
    /// ## [`IndexFilter::And`] - Index Intersection via Joins
    /// Builds a left-deep tree of joins to intersect row IDs from multiple indexes.
    /// The joins are performed on the `row_id` column to find rows that satisfy ALL conditions.
    /// Uses [`crate::physical_plan::joins::try_create_index_lookup_join`] which selects between
    /// HashJoin and SortMergeJoin based on input ordering.
    ///
    /// ```text
    /// HashJoin/SortMergeJoin(
    ///   HashJoin/SortMergeJoin(
    ///     IndexScanExec(index1, filter1),
    ///     IndexScanExec(index2, filter2)
    ///   ),
    ///   IndexScanExec(index3, filter3)
    /// )
    /// ```
    ///
    /// This approach:
    /// - Processes filters left-to-right, building joins incrementally
    /// - Each join reduces the result set, making subsequent joins more efficient
    /// - Preserves only row IDs that appear in ALL index scans
    ///
    /// ## [`IndexFilter::Or`] - Union with Deduplication
    /// Creates a [`UnionExec`] of all index scans followed by an [`AggregateExec`] that groups by
    /// `row_id` to automatically deduplicate overlapping results.
    ///
    /// ```text
    /// AggregateExec(GROUP BY row_id,
    ///   UnionExec(
    ///     IndexScanExec(index1, filter1),
    ///     IndexScanExec(index2, filter2),
    ///     IndexScanExec(index3, filter3)
    ///   )
    /// )
    /// ```
    ///
    /// This approach:
    /// - Combines results from multiple indexes that satisfy ANY of the conditions
    /// - Automatically deduplicates row IDs that appear in multiple index results
    /// - Ensures each row is fetched only once in the subsequent fetch phase
    /// - Handles empty filter lists by returning an empty execution plan
    ///
    /// # Arguments
    /// * `index_filter` - The [`IndexFilter`] tree specifying which indexes to scan and how to combine them
    /// * `limit` - Optional limit on the number of rows to return, passed through to individual index scans
    ///
    /// # Returns
    /// An [`Arc<dyn ExecutionPlan>`] that produces a stream of row IDs matching the filter criteria.
    /// The output schema always contains a single column named [`ROW_ID_COLUMN_NAME`].
    ///
    /// # Errors
    /// Returns [`DataFusionError::Plan`] if:
    /// - An [`IndexFilter::And`] contains no sub-filters
    /// - Any recursive call to build sub-plans fails
    /// - Index scan creation fails due to invalid filter expressions
    fn build_scan_exec(
        index_filter: &IndexFilter,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match index_filter {
            IndexFilter::Single { index, filter } => {
                let index_schema = index
                    .index_schema()
                    .column_with_name(ROW_ID_COLUMN_NAME)
                    .ok_or(DataFusionError::Plan(
                        "IndexScanExec requires a column named __row_id__".to_string(),
                    ))?
                    .1
                    .data_type()
                    .clone();
                // Use consistent schema for all index scans to avoid union mismatches
                let consistent_schema = create_index_schema(index_schema);
                let exec = IndexScanExec::try_new(
                    index.clone(),
                    vec![filter.clone()],
                    limit,
                    consistent_schema,
                )?;
                Ok(Arc::new(exec))
            }
            IndexFilter::And(filters) => {
                let mut plans = filters
                    .iter()
                    .map(|f| Self::build_scan_exec(f, limit))
                    .collect::<Result<Vec<_>>>()?;

                if plans.is_empty() {
                    return Err(DataFusionError::Plan(
                        "IndexFilter::And requires at least one sub-filter".to_string(),
                    ));
                }

                let mut left = plans.remove(0);
                while !plans.is_empty() {
                    let right = plans.remove(0);
                    left = try_create_index_lookup_join(left, right)?;
                }
                Ok(left)
            }
            IndexFilter::Or(filters) => {
                let original_plans = filters
                    .iter()
                    .map(|f| Self::build_scan_exec(f, limit))
                    .collect::<Result<Vec<_>>>()?;

                if original_plans.is_empty() {
                    return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
                }

                // Create canonical schema for all plans - single __row_id__ column
                let canonical_schema = create_index_schema(DataType::UInt64);

                // Normalize all plans to have the same canonical schema
                let mut normalized_plans = Vec::new();
                for plan in original_plans {
                    let plan_schema = plan.schema();

                    // Check if this plan needs schema normalization
                    if plan_schema == canonical_schema {
                        // Plan already has correct schema, use as-is
                        normalized_plans.push(plan);
                    } else {
                        // Plan has different schema (e.g., HashJoinExec with multiple __row_id__ columns)
                        // Add projection to normalize to single __row_id__ column

                        // Find the first __row_id__ column index
                        let row_id_index = plan_schema
                            .fields()
                            .iter()
                            .position(|f| f.name() == ROW_ID_COLUMN_NAME)
                            .ok_or_else(|| {
                                DataFusionError::Plan(format!(
                                    "Plan schema missing required {ROW_ID_COLUMN_NAME} column: {plan_schema:?}"
                                ))
                            })?;

                        // Create projection expression to select only the first __row_id__ column
                        let projection_expr = vec![(
                            Arc::new(Column::new(ROW_ID_COLUMN_NAME, row_id_index))
                                as Arc<dyn PhysicalExpr>,
                            ROW_ID_COLUMN_NAME.to_string(),
                        )];

                        // Wrap plan with projection to normalize schema
                        let projection_plan = ProjectionExec::try_new(projection_expr, plan)?;

                        normalized_plans.push(Arc::new(projection_plan) as Arc<dyn ExecutionPlan>);
                    }
                }

                // Now all plans have identical schemas, UnionExec will work
                let union_input = Arc::new(UnionExec::new(normalized_plans));

                // Create aggregate to deduplicate row IDs
                let group_expr = PhysicalGroupBy::new_single(vec![(
                    Arc::new(Column::new(ROW_ID_COLUMN_NAME, 0)) as Arc<dyn PhysicalExpr>,
                    ROW_ID_COLUMN_NAME.to_string(),
                )]);

                let agg_exec = AggregateExec::try_new(
                    AggregateMode::Single,
                    group_expr,
                    vec![],
                    vec![],
                    union_input,
                    canonical_schema,
                )?;

                Ok(Arc::new(agg_exec))
            }
        }
    }
}

impl DisplayAs for RecordFetchExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                let index_names: Vec<_> = self.indexes.iter().map(|i| i.to_string()).collect();
                write!(
                    f,
                    "RecordFetchExec: indexes=[{}], limit={:?}",
                    index_names.join(", "),
                    self.limit
                )
            }
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

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // RecordFetchExec requires a single partition input because it merges
        // results from multiple index scans.
        vec![Distribution::SinglePartition]
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
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "RecordFetchExec executed with partition {partition} but expected 0"
            )));
        }

        let input_stream = self.input.execute(0, context)?;
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
    /// Execution metrics.
    baseline_metrics: BaselineMetrics,
    /// The state of the stream.
    state: FetchState,
}

/// A future that resolves to a fetched `RecordBatch` and the reclaimed
/// input stream and fetcher.
type FetchFuture = BoxFuture<
    'static,
    Result<(
        SendableRecordBatchStream,
        Arc<dyn RecordFetcher>,
        RecordBatch,
    )>,
>;

/// The state of the `RecordFetchStream`.
enum FetchState {
    /// Reading from the input stream.
    ReadingInput {
        input: SendableRecordBatchStream,
        fetcher: Arc<dyn RecordFetcher>,
    },
    /// Fetching a batch of records. The future returns the input stream and
    /// fetcher so they can be reclaimed.
    Fetching(FetchFuture),
    /// An error occurred.
    Error,
}

impl RecordFetchStream {
    /// Create a new `RecordFetchStream`.
    pub fn new(
        input: SendableRecordBatchStream,
        fetcher: Arc<dyn RecordFetcher>,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        let schema = fetcher.schema();
        let state = FetchState::ReadingInput { input, fetcher };
        Self {
            schema,
            baseline_metrics,
            state,
        }
    }
}

impl Stream for RecordFetchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match std::mem::replace(&mut self.state, FetchState::Error) {
                FetchState::ReadingInput { mut input, fetcher } => {
                    match input.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) if batch.num_rows() > 0 => {
                            // Start async fetch for non-empty batch
                            let fut = {
                                let fetcher = fetcher.clone();
                                async move {
                                    fetcher
                                        .fetch(batch)
                                        .await
                                        .map(|batch| (input, fetcher, batch))
                                }
                                .boxed()
                            };
                            self.state = FetchState::Fetching(fut);
                        }
                        Poll::Ready(Some(Ok(_))) => {
                            // Empty batch - continue reading
                            self.state = FetchState::ReadingInput { input, fetcher };
                        }
                        Poll::Ready(Some(Err(e))) => {
                            return self.baseline_metrics.record_poll(Poll::Ready(Some(Err(e))));
                        }
                        Poll::Ready(None) => {
                            return self.baseline_metrics.record_poll(Poll::Ready(None));
                        }
                        Poll::Pending => {
                            self.state = FetchState::ReadingInput { input, fetcher };
                            return self.baseline_metrics.record_poll(Poll::Pending);
                        }
                    }
                }
                FetchState::Fetching(mut fut) => {
                    match fut.as_mut().poll(cx) {
                        Poll::Ready(Ok((input, fetcher, batch))) if batch.num_rows() > 0 => {
                            // Yield non-empty batch and prepare for next input
                            self.state = FetchState::ReadingInput { input, fetcher };
                            return self
                                .baseline_metrics
                                .record_poll(Poll::Ready(Some(Ok(batch))));
                        }
                        Poll::Ready(Ok((input, fetcher, _))) => {
                            // Empty batch - continue reading
                            self.state = FetchState::ReadingInput { input, fetcher };
                        }
                        Poll::Ready(Err(e)) => {
                            return self.baseline_metrics.record_poll(Poll::Ready(Some(Err(e))));
                        }
                        Poll::Pending => {
                            self.state = FetchState::Fetching(fut);
                            return self.baseline_metrics.record_poll(Poll::Pending);
                        }
                    }
                }
                FetchState::Error => {
                    return self.baseline_metrics.record_poll(Poll::Ready(None));
                }
            }
        }
    }
}

impl fmt::Debug for RecordFetchStream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("RecordFetchStream")
            .field("schema", &self.schema)
            .field("baseline_metrics", &self.baseline_metrics)
            .finish()
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
    use async_trait::async_trait;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::array::UInt64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::Statistics;
    use datafusion::logical_expr::Expr;
    use datafusion::logical_expr::{col, lit};
    use datafusion::physical_plan::joins::HashJoinExec;
    use datafusion::physical_plan::memory::MemoryStream;
    use datafusion::prelude::SessionContext;
    use std::any::Any;
    use std::sync::Mutex;
    use std::time::Duration;

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
                            Arc::new(StringArray::from(names)),
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

    // --- Slow Record Fetcher ---
    #[derive(Debug)]
    struct SlowRecordFetcher {
        schema: SchemaRef,
        names: Vec<String>,
    }

    impl SlowRecordFetcher {
        fn new(names: Vec<String>) -> Self {
            Self {
                schema: Arc::new(Schema::new(vec![
                    Field::new(ROW_ID_COLUMN_NAME, DataType::UInt64, false),
                    Field::new("name", DataType::Utf8, false),
                ])),
                names,
            }
        }
    }

    #[async_trait]
    impl RecordFetcher for SlowRecordFetcher {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        async fn fetch(&self, index_batch: RecordBatch) -> Result<RecordBatch> {
            // Simulate a delay
            tokio::time::sleep(Duration::from_millis(20)).await;

            let row_ids = index_batch
                .column_by_name(ROW_ID_COLUMN_NAME)
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();

            // add a delay between each row
            let mut names = Vec::with_capacity(row_ids.len());
            for id in row_ids.values().iter() {
                // simulate an await point
                tokio::time::sleep(Duration::from_millis(20)).await;
                names.push(self.names[*id as usize].clone());
            }

            Ok(RecordBatch::try_new(
                self.schema.clone(),
                vec![
                    Arc::new(row_ids.clone()),
                    Arc::new(StringArray::from(names)),
                ],
            )?)
        }
    }

    #[tokio::test]
    async fn test_record_fetch_exec_slow_input() {
        let session_ctx = SessionContext::new();
        let _task_ctx = session_ctx.task_ctx();
        let schema = Arc::new(Schema::new(vec![Field::new(
            ROW_ID_COLUMN_NAME,
            DataType::UInt64,
            false,
        )]));

        // create a memoryStream of 5 rows
        let input_stream = MemoryStream::try_new(
            vec![RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(UInt64Array::from(vec![0, 1, 2, 3, 4]))],
            )
            .expect("Failed to create RecordBatch")],
            schema.clone(),
            None,
        )
        .expect("Failed to create MemoryStream");

        let fetcher = Arc::new(SlowRecordFetcher::new(vec![
            "name_0".to_string(),
            "name_1".to_string(),
            "name_2".to_string(),
            "name_3".to_string(),
            "name_4".to_string(),
        ]));
        let metrics = ExecutionPlanMetricsSet::new();
        let baseline_metrics = BaselineMetrics::new(&metrics, 0);

        let mut stream = RecordFetchStream::new(Box::pin(input_stream), fetcher, baseline_metrics);

        let mut total_rows = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();
        }

        assert_eq!(total_rows, 5, "Should have fetched all 5 rows");
    }

    #[tokio::test]
    async fn test_record_fetch_exec_slow_and_multiple() {
        let session_ctx = SessionContext::new();
        let _task_ctx = session_ctx.task_ctx();
        let schema = Arc::new(Schema::new(vec![Field::new(
            ROW_ID_COLUMN_NAME,
            DataType::UInt64,
            false,
        )]));

        // create a memoryStream of 5 rows
        let input_stream = MemoryStream::try_new(
            vec![
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(UInt64Array::from(vec![0, 1, 2]))],
                )
                .expect("Failed to create RecordBatch"),
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(UInt64Array::from(vec![3, 4]))],
                )
                .expect("Failed to create RecordBatch"),
            ],
            schema.clone(),
            None,
        )
        .expect("Failed to create MemoryStream");

        let fetcher = Arc::new(SlowRecordFetcher::new(vec![
            "name_0".to_string(),
            "name_1".to_string(),
            "name_2".to_string(),
            "name_3".to_string(),
            "name_4".to_string(),
        ]));
        let metrics = ExecutionPlanMetricsSet::new();
        let baseline_metrics = BaselineMetrics::new(&metrics, 0);

        let mut stream = RecordFetchStream::new(Box::pin(input_stream), fetcher, baseline_metrics);

        let mut total_rows = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();
        }

        assert_eq!(total_rows, 5, "Should have fetched all 5 rows");
    }

    #[tokio::test]
    async fn test_record_fetch_exec_multiple_recordbatch() {
        let session_ctx = SessionContext::new();
        let _task_ctx = session_ctx.task_ctx();
        let schema = Arc::new(Schema::new(vec![Field::new(
            ROW_ID_COLUMN_NAME,
            DataType::UInt64,
            false,
        )]));

        // create a memoryStream of 5 recordBatch
        let input_stream = MemoryStream::try_new(
            vec![
                RecordBatch::try_new(schema.clone(), vec![Arc::new(UInt64Array::from(vec![0]))])
                    .expect("Failed to create RecordBatch"),
                RecordBatch::try_new(schema.clone(), vec![Arc::new(UInt64Array::from(vec![1]))])
                    .expect("Failed to create RecordBatch"),
                RecordBatch::try_new(schema.clone(), vec![Arc::new(UInt64Array::from(vec![2]))])
                    .expect("Failed to create RecordBatch"),
                RecordBatch::try_new(schema.clone(), vec![Arc::new(UInt64Array::from(vec![3]))])
                    .expect("Failed to create RecordBatch"),
                RecordBatch::try_new(schema.clone(), vec![Arc::new(UInt64Array::from(vec![4]))])
                    .expect("Failed to create RecordBatch"),
            ],
            schema.clone(),
            None,
        )
        .expect("Failed to create MemoryStream");

        let fetcher = Arc::new(SlowRecordFetcher::new(vec![
            "name_0".to_string(),
            "name_1".to_string(),
            "name_2".to_string(),
            "name_3".to_string(),
            "name_4".to_string(),
        ]));
        let metrics = ExecutionPlanMetricsSet::new();
        let baseline_metrics = BaselineMetrics::new(&metrics, 0);

        let mut stream = RecordFetchStream::new(Box::pin(input_stream), fetcher, baseline_metrics);

        let mut total_rows = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();
        }

        assert_eq!(total_rows, 5, "Should have fetched all 5 rows");
    }

    // --- Tests ---

    #[tokio::test]
    async fn test_record_fetch_stream_eager_with_empty_batches() -> Result<()> {
        // This test ensures that the stream is "eager" and will skip over empty
        // input batches to find the next valid one within a single poll cycle.

        // 1. Setup input stream with an empty batch in the middle
        let schema = Arc::new(Schema::new(vec![Field::new(
            ROW_ID_COLUMN_NAME,
            DataType::UInt64,
            false,
        )]));
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt64Array::from(vec![1, 2]))],
        )?;
        let empty_batch = RecordBatch::new_empty(schema.clone());
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt64Array::from(vec![3, 4]))],
        )?;
        let input_stream = MemoryStream::try_new(vec![batch1, empty_batch, batch2], schema, None)?;

        // 2. Setup fetcher and stream
        let names = (0..5).map(|i| format!("name_{i}")).collect();
        let fetcher = Arc::new(SlowRecordFetcher::new(names));
        let metrics = ExecutionPlanMetricsSet::new();
        let baseline_metrics = BaselineMetrics::new(&metrics, 0);
        let stream =
            RecordFetchStream::new(Box::pin(input_stream), fetcher.clone(), baseline_metrics);

        // 3. Collect results
        let results = datafusion::physical_plan::common::collect(Box::pin(stream)).await?;

        // 4. Assert results
        let expected_batch1 = RecordBatch::try_new(
            fetcher.schema(),
            vec![
                Arc::new(UInt64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["name_1", "name_2"])),
            ],
        )?;
        let expected_batch2 = RecordBatch::try_new(
            fetcher.schema(),
            vec![
                Arc::new(UInt64Array::from(vec![3, 4])),
                Arc::new(StringArray::from(vec!["name_3", "name_4"])),
            ],
        )?;

        assert_eq!(
            results.len(),
            2,
            "Should have produced two non-empty batches"
        );
        assert_eq!(results[0], expected_batch1);
        assert_eq!(results[1], expected_batch2);

        Ok(())
    }

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
        let indexes: Vec<IndexFilter> = vec![IndexFilter::Single {
            index: index.clone() as Arc<dyn Index>,
            filter: col("a").eq(lit(1)),
        }];

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

        let indexes = vec![IndexFilter::And(vec![
            IndexFilter::Single {
                index: index1,
                filter: col("a").eq(lit(1)),
            },
            IndexFilter::Single {
                index: index2,
                filter: col("a").eq(lit(1)),
            },
        ])];

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
            indexes_vec.push(IndexFilter::Single {
                index: Arc::new(MockIndex::new(vec![batch])) as Arc<dyn Index>,
                filter: col("a").eq(lit(1)),
            });
        }

        let indexes = vec![IndexFilter::And(indexes_vec)];
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
        let indexes = vec![IndexFilter::Single {
            index: index.clone() as Arc<dyn Index>,
            filter: col("a").eq(lit(1)),
        }];

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
                Arc::new(StringArray::from(expected_names)),
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
        let indexes = vec![IndexFilter::Single {
            index: index.clone() as Arc<dyn Index>,
            filter: col("a").eq(lit(1)),
        }];
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
        let indexes = vec![IndexFilter::Single {
            index: index.clone() as Arc<dyn Index>,
            filter: col("a").eq(lit(1)),
        }];
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
                Arc::new(StringArray::from(vec!["name_1", "name_3"])),
            ],
        )?;
        let expected_batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![5, 7])),
                Arc::new(StringArray::from(vec!["name_5", "name_7"])),
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
        let indexes = vec![IndexFilter::Single {
            index: index.clone() as Arc<dyn Index>,
            filter: col("a").eq(lit(1)),
        }];
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

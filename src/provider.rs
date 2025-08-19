use crate::physical_plan::exec::fetch::RecordFetchExec;
use crate::physical_plan::fetcher::RecordFetcher;
use crate::physical_plan::Index;
use crate::types::{IndexFilter, IndexFilters};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use std::collections::HashMap;
use std::sync::Arc;

/// A table provider that can be scanned using indexes.
///
/// This trait extends the [`TableProvider`] trait with additional methods
/// for scanning the table using indexes.
/// The [`IndexedTableProvider::scan_with_indexes_or_fallback`] method is the main entry point for scanning the table
/// that can be used in `TableProvider::scan`.
#[async_trait]
pub trait IndexedTableProvider: TableProvider + Sync + Send {
    /// Returns a list of all indexes available for this table.
    /// This is the only method that must be implemented by the user.
    fn indexes(&self) -> Result<Vec<Arc<dyn Index>>>;

    /// Analyzes filters, optimizes them, and groups them by the index that can handle them.
    ///
    /// # Default implementation
    /// The default implementation groups filters by the index that supports them, based on the
    /// column they refer to. It does not perform any optimization.
    ///
    /// # Returns
    /// A tuple containing:
    /// - A list of `IndexFilter`s, where each element is a struct containing an index and a list of
    ///   expressions that can be handled by that index.
    /// - A list of expressions that cannot be handled by any index.
    fn analyze_and_optimize_filters(&self, filters: &[Expr]) -> Result<(IndexFilters, Vec<Expr>)> {
        let indexes = self.indexes()?;
        let mut indexed_filters: HashMap<String, IndexFilter> = HashMap::new();
        let mut remaining_filters = Vec::new();

        for filter in filters {
            let mut found_index_for_filter = false;
            for index in &indexes {
                if index.supports_predicate(filter)? {
                    let entry = indexed_filters
                        .entry(index.name().to_string())
                        .or_insert_with(|| IndexFilter {
                            index: index.clone(),
                            filters: Vec::new(),
                        });
                    entry.filters.push(filter.clone());
                    found_index_for_filter = true;
                    break;
                }
            }

            if !found_index_for_filter {
                remaining_filters.push(filter.clone());
            }
        }

        let optimized_filters = indexed_filters.into_values().collect();

        Ok((optimized_filters, remaining_filters))
    }

    /// Returns whether the filters can be pushed down to the index.
    /// This method can be used in `TableProvider::supports_filters_pushdown`.
    ///
    /// # Default implementation
    /// The default implementation returns `TableProviderFilterPushDown::Exact` for any filter
    /// that is supported by at least one index, and `TableProviderFilterPushDown::Unsupported`
    /// otherwise.
    fn supports_filters_index_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let indexes = self.indexes()?;
        let mut pushdown = Vec::new();
        for filter in filters {
            let mut found_index = false;
            for index in indexes.iter() {
                if index.supports_predicate(filter)? {
                    pushdown.push(TableProviderFilterPushDown::Exact);
                    found_index = true;
                    break;
                }
            }

            if !found_index {
                pushdown.push(TableProviderFilterPushDown::Unsupported);
            }
        }
        Ok(pushdown)
    }

    /// Creates an execution plan that scans the table using the provided indexes.
    ///
    /// # Arguments
    /// * `indexes` - A slice of `IndexFilter` structs, each containing an index and a list of
    ///   expressions that can be handled by that index.
    /// * `_projection` - A slice of column indices to be projected. This is currently ignored.
    /// * `_remaining_filters` - A slice of expressions that cannot be handled by any index.
    /// * `limit` - An optional limit on the number of rows to return.
    /// * `schema` - The schema of the table.
    /// * `mapper` - A reference to a `RecordFetcher` that will be used to fetch records.
    ///
    /// # Returns
    /// A `Result` containing an `Arc<dyn ExecutionPlan>` that can be used to scan the table.
    async fn create_execution_plan_with_indexes(
        &self,
        indexes: &[IndexFilter],
        // TODO: Use projection
        _projection: Option<&Vec<usize>>,
        _remaining_filters: &[Expr],
        limit: Option<usize>,
        schema: SchemaRef,
        mapper: Arc<dyn RecordFetcher>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(RecordFetchExec::try_new(
            indexes.to_vec(),
            limit,
            Arc::clone(&mapper),
            schema.clone(),
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::Index;
    use crate::physical_plan::ROW_ID_COLUMN_NAME;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::catalog::Session;
    use datafusion::common::Statistics;
    use datafusion::datasource::TableType;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    };
    use datafusion::prelude::{col, lit};
    use datafusion_common::{DataFusionError, Result};
    use std::any::Any;

    // Mock Index
    #[derive(Debug)]
    struct MockIndex {
        column_name: String,
        table_name: String,
    }

    impl Index for MockIndex {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn name(&self) -> &str {
            "mock_index"
        }

        fn index_schema(&self) -> SchemaRef {
            Arc::new(Schema::new(vec![Field::new(
                ROW_ID_COLUMN_NAME,
                DataType::UInt32,
                false,
            )]))
        }

        fn table_name(&self) -> &str {
            &self.table_name
        }

        fn column_name(&self) -> &str {
            &self.column_name
        }

        fn scan(
            &self,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<SendableRecordBatchStream, DataFusionError> {
            unimplemented!("MockIndex::scan")
        }

        fn statistics(&self) -> Statistics {
            Statistics::new_unknown(&self.index_schema())
        }
    }

    // Mock ExecutionPlan
    #[derive(Debug)]
    struct MockExec;

    impl DisplayAs for MockExec {
        fn fmt_as(&self, _t: DisplayFormatType, _f: &mut std::fmt::Formatter) -> std::fmt::Result {
            unimplemented!()
        }
    }

    impl ExecutionPlan for MockExec {
        fn name(&self) -> &str {
            "MockExec"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &PlanProperties {
            unimplemented!()
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
            unimplemented!()
        }
    }

    // Mock IndexedTableProvider
    #[derive(Debug)]
    struct MockTableProvider {
        indexes: Vec<Arc<dyn Index>>,
    }

    impl MockTableProvider {
        fn new(indexes: Vec<Arc<dyn Index>>) -> Self {
            Self { indexes }
        }
    }

    #[async_trait]
    impl TableProvider for MockTableProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
            ]))
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }
    }

    #[async_trait]
    impl IndexedTableProvider for MockTableProvider {
        fn indexes(&self) -> Result<Vec<Arc<dyn Index>>> {
            Ok(self.indexes.clone())
        }
    }

    #[test]
    fn test_analyze_and_optimize_filters() -> Result<()> {
        let provider = MockTableProvider::new(vec![Arc::new(MockIndex {
            column_name: "a".into(),
            table_name: "t".into(),
        })]);
        let filters = vec![col("a").eq(lit(1))];
        let (indexed, remaining) = provider.analyze_and_optimize_filters(&filters)?;

        assert_eq!(
            indexed.len(),
            1,
            "Expected 1 indexed filter, got {}",
            indexed.len()
        );
        assert_eq!(
            indexed[0].filters.len(),
            1,
            "Expected 1 indexed filter, got {}",
            indexed[0].filters.len()
        );
        assert_eq!(
            remaining.len(),
            0,
            "Expected 0 remaining filters, got {}",
            remaining.len()
        );

        Ok(())
    }

    #[test]
    fn test_analyze_and_optimize_filters_with_multiple_filters() -> Result<()> {
        let provider = MockTableProvider::new(vec![Arc::new(MockIndex {
            column_name: "a".into(),
            table_name: "t".into(),
        })]);
        let filters = vec![col("a").eq(lit(1)), col("b").eq(lit(2))];
        let (indexed, remaining) = provider.analyze_and_optimize_filters(&filters)?;

        assert_eq!(
            indexed.len(),
            1,
            "Expected 1 indexed filter group, got {}",
            indexed.len()
        );
        assert_eq!(
            indexed[0].filters.len(),
            1,
            "Expected 1 filter in group, got {}",
            indexed[0].filters.len()
        );
        assert_eq!(
            remaining.len(),
            1,
            "Expected 1 remaining filter, got {}",
            remaining.len()
        );

        Ok(())
    }

    #[test]
    fn test_analyze_and_optimize_filters_groups_by_index() -> Result<()> {
        let provider = MockTableProvider::new(vec![Arc::new(MockIndex {
            column_name: "a".into(),
            table_name: "t".into(),
        })]);
        let filters = vec![col("a").eq(lit(1)), col("a").gt(lit(0))];
        let (indexed, remaining) = provider.analyze_and_optimize_filters(&filters)?;

        assert_eq!(indexed.len(), 1, "Expected 1 indexed filter group");
        assert_eq!(indexed[0].filters.len(), 2, "Expected 2 filters in group");
        assert_eq!(remaining.len(), 0, "Expected 0 remaining filters");

        Ok(())
    }
}

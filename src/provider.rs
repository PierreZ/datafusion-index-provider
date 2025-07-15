use crate::physical_plan::Index;
use crate::types::{IndexFilter, IndexFilters};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
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
    /// - A list of `IndexFilter`s, where each element is a tuple of an index and a list of
    ///   expressions that can be handled by that index.
    /// - A list of expressions that cannot be handled by any index.
    fn analyze_and_optimize_filters(&self, filters: &[Expr]) -> Result<(IndexFilters, Vec<Expr>)> {
        let indexes = self.indexes()?;
        let mut indexed_filters: HashMap<String, (Arc<dyn Index>, Vec<Expr>)> = HashMap::new();
        let mut remaining_filters = Vec::new();

        for filter in filters {
            let mut found_index_for_filter = false;
            for index in &indexes {
                if index.supports_predicate(filter)? {
                    let entry = indexed_filters
                        .entry(index.name().to_string())
                        .or_insert_with(|| (index.clone(), Vec::new()));
                    entry.1.push(filter.clone());
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

    /// Builds an `ExecutionPlan` to merge the results of multiple index scans.
    ///
    /// This method is designed to be called by [`Self::scan_with_indexes_or_fallback`].
    /// It is responsible for creating a physical plan that can execute scans on the given
    /// indexes and merge the results.
    async fn scan_with_indexes(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        remaining_filters: &[Expr],
        limit: Option<usize>,
        indexes: &[IndexFilter],
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Builds an `ExecutionPlan` to scan the table.
    ///
    /// This method is designed to be called by [`Self::scan_with_indexes_or_fallback`].
    /// It is responsible for creating a physical plan that can execute a full table scan.
    async fn scan_with_table(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Returns an `ExecutionPlan` to scan the table, using indexes if possible.
    ///
    /// This is the main entry point for scanning the table. It can be used in
    /// `TableProvider::scan` to route between a table scan and an index scan.
    ///
    /// # Default implementation
    /// The default implementation analyzes the filters to determine if any of them can be
    /// handled by an index. If so, it calls [`Self::scan_with_indexes`]. Otherwise, it calls
    /// [`Self::scan_with_table`].
    ///
    /// ## Execution Flow
    ///
    /// ```text
    /// +--------------------------------------+
    /// | `scan_with_indexes_or_fallback`      |
    /// +--------------------------------------+
    ///                  |
    /// +--------------------------------------+
    /// | `analyze_and_optimize_filters`       |
    /// +--------------------------------------+
    ///                  |                
    ///    (index_filters, remaining_filters)
    ///                  |                
    /// +--------------------------------------+
    /// | IF !index_filters.is_empty()         |
    /// +--------------------------------------+
    ///                  |                
    ///     YES          |           NO
    /// +----------------+---------------------+
    /// |                                      |
    /// v                                      v
    /// +----------------+----------------+    +-----------------------------+
    /// | `scan_with_indexes`             |    | `scan_with_table`           |
    /// | (index_filters, remaining_filters) |    | (all_filters)               |
    /// +---------------------------------+    +-----------------------------+
    /// ```
    async fn scan_with_indexes_or_fallback(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (indexed_filters, remaining_filters) = self.analyze_and_optimize_filters(filters)?;

        if indexed_filters.is_empty() {
            return self
                .scan_with_table(state, projection, &remaining_filters, limit)
                .await;
        }

        self.scan_with_indexes(
            state,
            projection,
            &remaining_filters,
            limit,
            &indexed_filters,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::Index;
    use crate::physical_plan::ROW_ID_COLUMN_NAME;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::common::Statistics;
    use datafusion::datasource::TableType;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    };
    use datafusion::prelude::{col, lit, SessionContext};
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
        scanned_with_indexes: std::sync::Mutex<bool>,
        scanned_with_table: std::sync::Mutex<bool>,
    }

    impl MockTableProvider {
        fn new(indexes: Vec<Arc<dyn Index>>) -> Self {
            Self {
                indexes,
                scanned_with_indexes: std::sync::Mutex::new(false),
                scanned_with_table: std::sync::Mutex::new(false),
            }
        }

        fn was_scanned_with_indexes(&self) -> bool {
            *self.scanned_with_indexes.lock().unwrap()
        }

        fn was_scanned_with_table(&self) -> bool {
            *self.scanned_with_table.lock().unwrap()
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

        async fn scan_with_indexes(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
            _indexes: &[IndexFilter],
        ) -> Result<Arc<dyn ExecutionPlan>> {
            *self.scanned_with_indexes.lock().unwrap() = true;
            Ok(Arc::new(MockExec))
        }

        async fn scan_with_table(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            *self.scanned_with_table.lock().unwrap() = true;
            Ok(Arc::new(MockExec))
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
            indexed[0].1.len(),
            1,
            "Expected 1 indexed filter, got {}",
            indexed[0].1.len()
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
            indexed[0].1.len(),
            1,
            "Expected 1 filter in group, got {}",
            indexed[0].1.len()
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
        assert_eq!(indexed[0].1.len(), 2, "Expected 2 filters in group");
        assert_eq!(remaining.len(), 0, "Expected 0 remaining filters");

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_with_indexes_or_fallback_uses_index() -> Result<()> {
        let provider = MockTableProvider::new(vec![Arc::new(MockIndex {
            column_name: "a".into(),
            table_name: "t".into(),
        })]);
        let filters = vec![col("a").eq(lit(1))];
        let session = SessionContext::new();

        let _ = provider
            .scan_with_indexes_or_fallback(&session.state(), None, &filters, None)
            .await?;

        assert!(provider.was_scanned_with_indexes());
        assert!(!provider.was_scanned_with_table());

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_with_indexes_or_fallback_uses_table_scan() -> Result<()> {
        let provider = MockTableProvider::new(vec![Arc::new(MockIndex {
            column_name: "a".into(),
            table_name: "t".into(),
        })]);
        let filters = vec![col("b").eq(lit(2))];
        let session = SessionContext::new();

        let _ = provider
            .scan_with_indexes_or_fallback(&session.state(), None, &filters, None)
            .await?;

        assert!(!provider.was_scanned_with_indexes());
        assert!(provider.was_scanned_with_table());

        Ok(())
    }
}

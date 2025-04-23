use crate::physical::indexes::index::Index;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::{utils::expr_to_columns, Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use std::collections::HashSet;
use std::sync::Arc;

/// Extends the DataFusion `TableProvider` trait to add support for indexes.
///
/// Table providers implementing this trait can advertise the indexes they manage,
/// allowing the query planner to potentially use them for optimization.
#[async_trait]
pub trait IndexedTableProvider: TableProvider + Sync + Send {
    /// Returns a list of indexes available for this table.
    ///
    /// The planner might call this to discover available indexes.
    fn indexes(&self) -> Result<Vec<Arc<dyn Index>>>;

    /// Finds indexes that could potentially satisfy the given predicate expression.
    ///
    /// This method can be used by the optimizer to quickly filter relevant indexes
    /// based on the query predicates. The default implementation checks all available
    /// indexes returned by `indexes()` using `Index::supports_predicate`.
    fn find_suitable_indexes(&self, predicate: &Expr) -> Result<Vec<Arc<dyn Index>>> {
        let mut suitable_indexes = vec![];
        for index in self.indexes()? {
            // Check if the index itself thinks it can support the predicate
            if index.supports_predicate(predicate)? {
                suitable_indexes.push(index);
            }
        }
        Ok(suitable_indexes)
    }

    // --- Methods moved from old IndexProvider trait ---

    /// Returns a set of column names that are indexed by any of the indexes
    /// returned by `indexes()`.
    /// The default implementation iterates through all indexes and collects
    /// the columns they cover.
    fn get_indexed_columns_names(&self) -> Result<HashSet<String>> {
        let mut all_indexed_columns = HashSet::new();
        for index in self.indexes()? {
            for field in index.index_schema().fields() {
                all_indexed_columns.insert(field.name().clone());
            }
        }
        Ok(all_indexed_columns)
    }

    /// Optimizes a list of expressions by combining them when possible
    /// For example, multiple expressions on the same column could be combined into a single expression
    /// Returns a new list of optimized expressions
    /// TODO: Implement actual optimization logic if needed.
    fn optimize_exprs(&self, exprs: &[Expr]) -> Result<Vec<Expr>> {
        // Default implementation returns expressions as-is
        Ok(exprs.to_vec())
    }

    /// Creates an IndexLookupExec for the given filter expression.
    /// This likely involves finding a suitable index via `find_suitable_indexes`
    /// and then calling the `scan` method on that index.
    /// The exact implementation might vary.
    /// Placeholder: Needs implementation details based on planner interaction.
    fn create_index_lookup(&self, expr: &Expr) -> Result<Arc<dyn ExecutionPlan>>;

    /// Creates an execution plan that combines multiple index lookups (if necessary)
    /// and joins the results with the base table data.
    /// Default implementation might use HashJoinExec or a custom IndexJoinExec.
    /// Placeholder: Needs implementation details.
    fn create_index_join(
        &self,
        lookups: Vec<Arc<dyn ExecutionPlan>>, // Results of create_index_lookup
        projection: Option<&Vec<usize>>,      // Projection for the final table data
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Overloads the `supports_filters_pushdown` method from `TableProvider` to take advantage of indexes.
    /// It checks if filters can be handled by available indexes.
    fn supports_index_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let mut pushdowns = Vec::with_capacity(filters.len());
        let indexed_columns = self.get_indexed_columns_names()?;
        for filter in filters {
            let mut columns = HashSet::new();
            expr_to_columns(filter, &mut columns)?;
            // Check if *all* columns used by the filter are indexed
            let all_columns_indexed = columns
                .iter()
                .all(|col| indexed_columns.contains(col.name()));

            // Basic logic: If all columns are indexed, mark as Inexact (index can evaluate).
            // More sophisticated logic could check if a *specific* index can handle the *entire* filter.
            if all_columns_indexed && !columns.is_empty() {
                // Check if any single suitable index supports the *entire* predicate
                let can_support = self
                    .find_suitable_indexes(filter)?
                    .iter()
                    .any(|index| index.supports_predicate(filter).unwrap_or(false));
                if can_support {
                    pushdowns.push(TableProviderFilterPushDown::Inexact);
                } else {
                    // Columns are indexed, but no single index supports the filter structure
                    pushdowns.push(TableProviderFilterPushDown::Unsupported);
                }
            } else {
                pushdowns.push(TableProviderFilterPushDown::Unsupported);
            }
        }
        Ok(pushdowns)
    }

    // Note: We might need more methods here in the future, for example,
    // to provide more detailed cost information about using a specific index
    // or to handle index-specific DDL/DML operations.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::indexes::index::Index;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use async_trait::async_trait;
    use datafusion::catalog::{Session, TableProvider}; // Keep Session for trait bounds/scan signature
    use datafusion::datasource::TableType;
    use datafusion::error::Result;
    use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
    use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream, Statistics};
    use datafusion_common::Column;
    use lazy_static::lazy_static;
    use std::any::Any;
    use std::sync::Arc;

    // --- Mock Index ---
    #[derive(Debug)]
    struct MockSimpleIndex {
        name: String,
        supports: bool,    // Controls supports_predicate result
        schema: SchemaRef, // Add schema field
    }

    lazy_static! {
        static ref DUMMY_SCHEMA: SchemaRef = Arc::new(Schema::empty());
        // Schema for mock index tests, including col_a
        static ref MOCK_INDEX_SCHEMA: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Int32, false),
        ]));
    }

    impl Index for MockSimpleIndex {
        fn name(&self) -> &str {
            &self.name
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
        fn table_name(&self) -> &str {
            "mock_table"
        }

        // Return the index's specific schema
        fn index_schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn supports_predicate(&self, _predicate: &Expr) -> Result<bool> {
            Ok(self.supports)
        }

        fn scan(
            &self,
            _predicate: &Expr,
            _projection: Option<&Vec<usize>>,
        ) -> Result<SendableRecordBatchStream> {
            unimplemented!("MockSimpleIndex::scan not needed for provider tests")
        }

        // Use index schema for statistics
        fn statistics(&self) -> Statistics {
            Statistics::new_unknown(&self.index_schema())
        }
    }

    // --- Mock Base Table Provider ---
    #[derive(Debug)]
    struct MockBaseTableProvider {
        schema: SchemaRef,
    }

    impl MockBaseTableProvider {
        fn new() -> Self {
            Self {
                schema: Arc::new(Schema::new(vec![Field::new(
                    "col_a",
                    DataType::Int32,
                    false,
                )])),
            }
        }
    }

    #[async_trait]
    impl TableProvider for MockBaseTableProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
        fn table_type(&self) -> TableType {
            TableType::Base
        }

        // Update scan signature: state type to &dyn Session
        async fn scan(
            &self,
            _state: &(dyn Session + '_), // Correct state type
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!("MockBaseTableProvider::scan not needed for provider tests")
        }

        fn supports_filters_pushdown(
            &self,
            _filters: &[&Expr],
        ) -> Result<Vec<TableProviderFilterPushDown>> {
            Ok(vec![
                TableProviderFilterPushDown::Unsupported;
                _filters.len()
            ])
        }
    }

    // --- Mock Indexed Table Provider ---
    #[derive(Debug)]
    struct MockIndexedTable {
        base_provider: MockBaseTableProvider,
        indexes: Vec<Arc<dyn Index>>,
        supports_pushdown: bool, // Add flag to control pushdown behavior
    }

    impl MockIndexedTable {
        fn new(indexes: Vec<Arc<dyn Index>>) -> Self {
            Self {
                base_provider: MockBaseTableProvider::new(),
                indexes,
                supports_pushdown: true,
            } // Default to true
        }
        fn new_with_pushdown(indexes: Vec<Arc<dyn Index>>, supports_pushdown: bool) -> Self {
            Self {
                base_provider: MockBaseTableProvider::new(),
                indexes,
                supports_pushdown,
            }
        }
    }

    #[async_trait]
    impl IndexedTableProvider for MockIndexedTable {
        fn indexes(&self) -> Result<Vec<Arc<dyn Index>>> {
            Ok(self.indexes.clone())
        }

        // Correct signature for create_index_lookup
        fn create_index_lookup(&self, _expr: &Expr) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!("Mock does not implement create_index_lookup")
        }
        // Correct signature for create_index_join
        fn create_index_join(
            &self,
            _lookups: Vec<Arc<dyn ExecutionPlan>>,
            _projection: Option<&Vec<usize>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!("Mock does not implement create_index_join")
        }

        fn supports_index_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> Result<Vec<TableProviderFilterPushDown>> {
            if self.supports_pushdown {
                let mut pushdowns = Vec::with_capacity(filters.len());
                let indexed_columns = self.get_indexed_columns_names()?;
                for filter in filters {
                    let mut columns = HashSet::new();
                    expr_to_columns(filter, &mut columns)?;
                    // Check if *all* columns used by the filter are indexed
                    let all_columns_indexed = columns
                        .iter()
                        .all(|col| indexed_columns.contains(col.name()));

                    // Basic logic: If all columns are indexed, mark as Inexact (index can evaluate).
                    // More sophisticated logic could check if a *specific* index can handle the *entire* filter.
                    if all_columns_indexed && !columns.is_empty() {
                        // Check if any single suitable index supports the *entire* predicate
                        let can_support = self
                            .find_suitable_indexes(filter)?
                            .iter()
                            .any(|index| index.supports_predicate(filter).unwrap_or(false));
                        if can_support {
                            pushdowns.push(TableProviderFilterPushDown::Inexact);
                        } else {
                            // Columns are indexed, but no single index supports the filter structure
                            pushdowns.push(TableProviderFilterPushDown::Unsupported);
                        }
                    } else {
                        pushdowns.push(TableProviderFilterPushDown::Unsupported);
                    }
                }
                Ok(pushdowns)
            } else {
                Ok(vec![
                    TableProviderFilterPushDown::Unsupported;
                    filters.len()
                ])
            }
        }
    }

    // Delegate TableProvider methods to the base provider
    #[async_trait]
    impl TableProvider for MockIndexedTable {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn schema(&self) -> SchemaRef {
            self.base_provider.schema()
        }
        fn table_type(&self) -> TableType {
            self.base_provider.table_type()
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!("MockIndexedTable::scan delegation requires mock Session")
        }
    }

    // --- Tests ---
    #[test]
    fn test_find_suitable_indexes_default() -> Result<()> {
        // Provide schema to mock index
        let index_supports = Arc::new(MockSimpleIndex {
            name: "idx_supports".to_string(),
            supports: true,
            schema: MOCK_INDEX_SCHEMA.clone(),
        });
        let index_no_support = Arc::new(MockSimpleIndex {
            name: "idx_no_support".to_string(),
            supports: false,
            schema: MOCK_INDEX_SCHEMA.clone(),
        });
        let provider = MockIndexedTable::new(vec![index_supports.clone(), index_no_support]);

        // Use imported Column type
        let dummy_expr = Expr::Column(Column::new_unqualified("col_a")); // Content doesn't matter for mock

        let suitable = provider.find_suitable_indexes(&dummy_expr)?;

        assert_eq!(suitable.len(), 1);
        assert_eq!(suitable[0].name(), "idx_supports");
        Ok(())
    }

    #[test]
    fn test_supports_index_filters_pushdown_default() -> Result<()> {
        // Provide schema to mock index
        let index_supports = Arc::new(MockSimpleIndex {
            name: "idx_supports".to_string(),
            supports: true,
            schema: MOCK_INDEX_SCHEMA.clone(),
        });
        let index_no_support = Arc::new(MockSimpleIndex {
            name: "idx_no_support".to_string(),
            supports: false,
            schema: MOCK_INDEX_SCHEMA.clone(),
        });
        let provider_supports = MockIndexedTable::new_with_pushdown(
            vec![index_supports.clone(), index_no_support.clone()],
            true,
        );
        let provider_no_support =
            MockIndexedTable::new_with_pushdown(vec![index_no_support.clone()], false);

        // Use imported Column type
        let dummy_expr1 = Expr::Column(Column::new_unqualified("col_a")); // Indexed
        let dummy_expr2 = Expr::Column(Column::new_unqualified("col_b")); // Not indexed by mock schema
        let filters = [dummy_expr1, dummy_expr2];
        let filter_refs: Vec<&Expr> = filters.iter().collect(); // Collect references

        // Case 1: One index supports the filter -> Inexact for col_a, Unsupported for col_b
        // Pass slice of references (&filter_refs)
        let pushdown_supports = provider_supports.supports_index_filters_pushdown(&filter_refs)?;

        assert_eq!(pushdown_supports.len(), 2);
        assert_eq!(pushdown_supports[0], TableProviderFilterPushDown::Inexact); // col_a is indexed and supported
        assert_eq!(
            pushdown_supports[1],
            TableProviderFilterPushDown::Unsupported
        ); // col_b is not indexed

        // Case 2: No index supports the filter, or columns not indexed -> Unsupported
        // Pass slice of references (&filter_refs)
        let pushdown_no_support =
            provider_no_support.supports_index_filters_pushdown(&filter_refs)?;

        assert_eq!(pushdown_no_support.len(), 2);
        assert_eq!(
            pushdown_no_support[0],
            TableProviderFilterPushDown::Unsupported
        ); // col_a is indexed but index doesn't support
        assert_eq!(
            pushdown_no_support[1],
            TableProviderFilterPushDown::Unsupported
        ); // col_b is not indexed

        Ok(())
    }
}

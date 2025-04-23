use crate::physical::indexes::index::Index;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::utils::expr_to_columns;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use std::collections::HashMap;
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

    /// Analyzes filters, optimizes those suitable for index pushdown, and separates remaining filters.
    ///
    /// Takes a slice of all filters applied to the scan and returns a tuple:
    ///  - `Vec<Expr>`: Optimized filters potentially suitable for index lookup.
    ///  - `Vec<Expr>`: Filters that could not be pushed down to an index.
    ///
    /// The default implementation identifies filters on indexed columns and attempts
    /// simple optimizations like combining range comparisons into BETWEEN clauses.
    fn analyze_and_optimize_filters(&self, filters: &[Expr]) -> Result<(Vec<Expr>, Vec<Expr>)> {
        let mut potential_index_filters = Vec::new();
        let mut remaining_filters = Vec::new();

        // 1. Initial pass: Identify potential candidates using the helper method
        for filter in filters {
            if self.identify_index_pushdown_candidate(filter)? {
                potential_index_filters.push(filter.clone());
            } else {
                remaining_filters.push(filter.clone());
            }
        }

        // 2. Group potential candidates by column name
        let (column_grouped_filters, ungrouped_potential_filters) =
            group_potential_index_filters_by_column(potential_index_filters);

        // 3. Optimize each group using the helper method
        let mut optimized_index_filters = Vec::new();
        for (col_name, col_exprs) in column_grouped_filters {
            let optimized_group = self.optimize_column_filters(&col_name, &col_exprs)?;
            optimized_index_filters.extend(optimized_group);
        }

        // 4. Add ungrouped potential filters to optimized_index_filters directly
        //    (They were identified as candidates but couldn't be optimized further by column)
        optimized_index_filters.extend(ungrouped_potential_filters);

        Ok((optimized_index_filters, remaining_filters))
    }

    /// Determines if a single filter expression is potentially suitable for index pushdown.
    ///
    /// This is called *before* grouping or optimization. Implementers can override this
    /// to define which basic expression shapes their indexes might support.
    ///
    /// Default implementation checks for BinaryExpr `Column Op Literal` where the column is indexed.
    fn identify_index_pushdown_candidate(&self, filter: &Expr) -> Result<bool> {
        if let Expr::BinaryExpr(binary) = filter {
            if let (Expr::Column(col), Expr::Literal(_)) = (&*binary.left, &*binary.right) {
                // Check if the column itself is indexed
                let indexed_columns = self.get_indexed_columns_names()?;
                if indexed_columns.contains(&col.name) {
                    // TODO: Could add more checks here based on the operator (binary.op)
                    //       or the literal type if needed by default.
                    return Ok(true);
                }
            }
            // Could potentially add checks for other Expr types like InList, Between here.
        }
        Ok(false)
    }

    /// Optimizes a set of filter expressions that apply to the *same* indexed column.
    ///
    /// This is called *after* initial candidates are identified and grouped by column.
    /// Implementers can override this to provide custom combination logic (e.g., range -> BETWEEN).
    ///
    /// Default implementation currently returns the filters as-is, but includes a placeholder
    /// for combination logic like `try_combine_exprs_to_between`.
    fn optimize_column_filters(&self, _col_name: &str, filters: &[Expr]) -> Result<Vec<Expr>> {
        if filters.len() > 1 {
            // Placeholder for combination logic
            // let filters_refs: Vec<&Expr> = filters.iter().collect();
            // if let Some(combined) = try_combine_exprs_to_between(filters_refs.as_slice(), _col_name) {
            //     return Ok(combined);
            // }
            // Fallback: return original filters if no combination possible
            Ok(filters.to_vec())
            // TODO: Re-enable or implement try_combine_exprs_to_between or other optimizations
        } else {
            Ok(filters.to_vec())
        }
    }

    /// Creates an IndexLookupExec for the given filter expression.
    /// This likely involves finding a suitable index via `find_suitable_indexes`
    /// and then calling the `scan` method on that index.
    /// The exact implementation might vary.
    /// Placeholder: Needs implementation details based on planner interaction.
    fn create_index_scan_exec_for_expr(&self, _expr: &Expr) -> Result<Arc<dyn ExecutionPlan>>;

    /// Creates an execution plan that combines multiple index lookups (if necessary)
    /// and joins the results with the base table data.
    /// Default implementation might use HashJoinExec or a custom IndexJoinExec.
    /// Placeholder: Needs implementation details.
    fn merge_indexes_streams(
        &self,
        _lookups: Vec<Arc<dyn ExecutionPlan>>, // Results of create_index_lookup
        _projection: Option<&Vec<usize>>,      // Projection for the final table data
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

/// Helper function to group potential index filters by the column they reference.
/// Filters that cannot be easily associated with a single column are returned separately.
fn group_potential_index_filters_by_column(
    potential_filters: Vec<Expr>,
) -> (HashMap<String, Vec<Expr>>, Vec<Expr>) {
    let mut column_grouped_filters: HashMap<String, Vec<Expr>> = HashMap::new();
    let mut ungrouped_potential_filters = Vec::new();

    for expr in potential_filters {
        let mut columns = HashSet::new();
        match expr_to_columns(&expr, &mut columns) {
            Ok(_) if columns.len() == 1 => {
                // If exactly one column is referenced, use its name for grouping
                if let Some(col) = columns.iter().next() {
                    column_grouped_filters
                        .entry(col.name.clone())
                        .or_default()
                        .push(expr); // Move expr here
                } else {
                    // Should not happen if len is 1, but handle defensively
                    ungrouped_potential_filters.push(expr);
                }
            }
            _ => {
                // Error converting or zero/multiple columns found
                ungrouped_potential_filters.push(expr);
            }
        }
    }

    (column_grouped_filters, ungrouped_potential_filters)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::indexes::index::Index;
    use arrow::datatypes::{DataType, Field, Schema};
    use async_trait::async_trait;
    use datafusion::catalog::{Session, TableProvider}; // Keep Session for trait bounds/scan signature
    use datafusion::datasource::TableType;
    use datafusion::error::Result;
    use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
    use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream, Statistics};
    use datafusion_common::Column;
    use std::any::Any;
    use std::sync::Arc;

    // --- Mock Index ---
    #[derive(Debug)]
    struct MockSimpleIndex {
        name: String,
        supports: bool,      // Controls supports_predicate result
        schema: Arc<Schema>, // Use SchemaRef (Arc<Schema>)
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
        fn index_schema(&self) -> Arc<Schema> {
            self.schema.clone() // Clone the Arc
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
            Statistics::new_unknown(&self.index_schema()) // Use the method returning Arc
        }
    }

    // --- Mock Base Table Provider ---
    #[derive(Debug)]
    struct MockBaseTableProvider {
        schema: Arc<Schema>, // Use SchemaRef (Arc<Schema>)
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
        fn schema(&self) -> Arc<Schema> {
            self.schema.clone() // Clone the Arc
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

        // Correct signature for create_index_scan_exec_for_expr
        fn create_index_scan_exec_for_expr(&self, _expr: &Expr) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!("Mock create_index_scan_exec_for_expr")
        }
        // Correct signature for create_index_join
        fn merge_indexes_streams(
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
        fn schema(&self) -> Arc<Schema> {
            self.base_provider.schema() // Call the method returning Arc
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
            schema: Arc::new(Schema::new(vec![Field::new(
                "col_a",
                DataType::Int32,
                false,
            )])),
        });
        let index_no_support = Arc::new(MockSimpleIndex {
            name: "idx_no_support".to_string(),
            supports: false,
            schema: Arc::new(Schema::new(vec![Field::new(
                "col_a",
                DataType::Int32,
                false,
            )])),
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
            schema: Arc::new(Schema::new(vec![Field::new(
                "col_a",
                DataType::Int32,
                false,
            )])),
        });
        let index_no_support = Arc::new(MockSimpleIndex {
            name: "idx_no_support".to_string(),
            supports: false,
            schema: Arc::new(Schema::new(vec![Field::new(
                "col_a",
                DataType::Int32,
                false,
            )])),
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

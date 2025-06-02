use crate::physical::indexes::index::Index;
use crate::physical::joins::try_create_index_lookup_join;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::TableProvider;
use datafusion::common::Result;
use datafusion::logical_expr::utils::expr_to_columns;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::DataFusionError;
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
    fn analyze_and_optimize_filters(
        &self,
        filters: &[Expr],
    ) -> Result<(Vec<(Arc<dyn Index>, Vec<Expr>)>, Vec<Expr>)> {
        let mut potential_index_filters = Vec::new();
        let mut remaining_filters = Vec::new();
        let mut result: HashMap<String, (Arc<dyn Index>, Vec<Expr>)> = HashMap::new();

        // Group expr by index
        for filter in filters {
            match self.find_index_from_expr(&filter)? {
                None => remaining_filters.push(filter.clone()),
                Some(index) => match result.get_mut(&index.name().to_string()) {
                    Some((_, exprs)) => exprs.push(filter.clone()),
                    None => {
                        result.insert(
                            index.name().to_string(),
                            (index.clone(), vec![filter.clone()]),
                        );
                    }
                },
            }
        }

        // ask the index if it can optimize
        for (_index_name, (index, exprs)) in result.iter() {
            let optimized = index.optimize(&exprs)?;
            potential_index_filters.push((index.clone(), optimized));
        }

        Ok((potential_index_filters, remaining_filters))
    }

    /// Finds the first index that can support the given predicate.
    fn find_index_from_expr(&self, expr: &Expr) -> Result<Option<Arc<dyn Index>>> {
        let indexes = self.indexes()?;
        for index in indexes {
            if index.supports_predicate(expr)? {
                return Ok(Some(index));
            }
        }
        Ok(None)
    }

    /// Creates an execution plan that combines multiple index lookups (if necessary)
    /// and joins the results with the base table data.
    /// Default implementation might use HashJoinExec or a custom IndexJoinExec.
    fn merge_indexes_streams(
        &self,
        indexes: &Vec<(Arc<dyn Index>, Vec<Expr>)>,
        _projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if indexes.len() == 0 {
            return Err(DataFusionError::Internal("No indexes provided".to_string()));
        }

        if indexes.len() == 1 {
            let (index, exprs) = &indexes[0];
            return index.scan_index(exprs, limit);
        }

        if indexes.len() == 2 {
            let (index1, exprs1) = &indexes[0];
            let (index2, exprs2) = &indexes[1];

            let left = index1.scan_index(exprs1, limit)?;
            let left_ordered = index1.is_ordered();
            let right = index2.scan_index(exprs2, limit)?;
            let right_ordered = index2.is_ordered();

            return try_create_index_lookup_join(left, left_ordered, right, right_ordered);
        }

        unimplemented!()
    }

    /// Overloads the `supports_filters_pushdown` method from `TableProvider` to take advantage of indexes.
    /// It checks if filters can be handled by available indexes.
    fn supports_index_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // Get all unique column names that have at least one index.
        // This is done once to avoid redundant computations within the loop.
        let indexed_column_names = self.get_indexed_columns_names()?;

        filters
            .iter()
            .map(|filter_expr| {
                // Renamed 'filter' to 'filter_expr' for clarity with Expr type
                let mut used_columns = HashSet::new();
                // Extract all columns involved in the current filter expression.
                expr_to_columns(filter_expr, &mut used_columns)?;

                // Determine if this filter can be (at least partially) handled by an index.
                let is_inexact_pushdown = if !used_columns.is_empty() {
                    // Check if all columns used in the filter expression are indexed.
                    let all_filter_columns_are_indexed = used_columns
                        .iter()
                        .all(|col| indexed_column_names.contains(&col.name));

                    if all_filter_columns_are_indexed {
                        // If all columns are indexed, check if any single suitable index
                        // can fully support this filter predicate.
                        // The `unwrap_or(false)` handles cases where `supports_predicate` itself
                        // might return an error, treating such cases as 'not supported'.
                        self.find_suitable_indexes(filter_expr)?
                            .iter()
                            .any(|index| index.supports_predicate(filter_expr).unwrap_or(false))
                    } else {
                        // Not all columns used by the filter are indexed.
                        false
                    }
                } else {
                    // The filter expression does not involve any columns.
                    false
                };

                if is_inexact_pushdown {
                    Ok(TableProviderFilterPushDown::Inexact)
                } else {
                    Ok(TableProviderFilterPushDown::Unsupported)
                }
            })
            .collect() // Collect the results for each filter into a Vec.
    }

    async fn create_execution_plan_with_indexes(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (indexes, remaining) = self.analyze_and_optimize_filters(filters)?;

        if !remaining.is_empty() {
            return Err(DataFusionError::NotImplemented(
                "Remaining filters not supported".to_string(),
            ));
        }

        // Merge the index scan plans into a single execution plan
        self.merge_indexes_streams(&indexes, projection, limit)
    }

    // Add support for updating indexes
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
    use datafusion::physical_plan::{ExecutionPlan, Statistics};
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

        fn scan_index(
            &self,
            _predicate: &Vec<Expr>,
            _limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
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
                        .all(|col| indexed_columns.contains(&col.name));

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

use crate::physical::indexes::index::Index;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::logical_expr::{utils::expr_to_columns, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion_common::Result;
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
            // Assuming Index trait has a method like `indexed_columns()` -> Vec<String>
            // If not, this needs adjustment based on how Index reveals its columns.
            // For now, let's placeholder this - we'll need to add a method to the Index trait.
            // Example placeholder:
            // all_indexed_columns.extend(index.indexed_columns());
            // If index only works on one column via name():
            all_indexed_columns.insert(index.name().to_string()); // Adjust if index name != column name
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

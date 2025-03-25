//! DataFusion Index Provider implementation.
//!
//! This crate provides an extension to DataFusion that adds support for indexed column lookups,
//! allowing for more efficient query execution when indexes are available.
//!
//! The main components are:
//! * [`IndexProvider`] - A trait that extends TableProvider with index capabilities
//! * [`IndexedColumn`] - Represents information about an indexed column
//! * [`physical`] - Module containing physical execution components for index operations
//! * [`optimizer`] - Module containing query optimization logic for index operations

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::utils::expr_to_columns;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion_common::Result;
use std::collections::HashSet;
use std::sync::Arc;

pub mod optimizer;
pub mod physical;

/// A provider that supports indexed column lookups
#[async_trait]
pub trait IndexProvider: TableProvider {
    /// Returns a set of column names that are indexed
    fn get_indexed_columns_names(&self) -> HashSet<String>;

    /// Optimizes a list of expressions by combining them when possible
    /// For example, multiple expressions on the same column could be combined into a single expression
    /// Returns a new list of optimized expressions
    fn optimize_exprs(&self, exprs: &[Expr]) -> Result<Vec<Expr>> {
        // Default implementation returns expressions as-is
        Ok(exprs.to_vec())
    }

    /// Creates an IndexLookupExec for the given filter expression
    fn create_index_lookup(&self, expr: &Expr) -> Result<Arc<dyn ExecutionPlan>>;

    /// Creates an execution plan that combines multiple index lookups
    /// Default implementation uses HashJoinExec when multiple indexes are used
    fn create_index_join(
        &self,
        lookups: Vec<Arc<dyn ExecutionPlan>>,
        projection: Option<&Vec<usize>>,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Overloads the `supports_filters_pushdown` method from `TableProvider` to take advantage of indexes
    fn supports_index_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let mut pushdowns = Vec::with_capacity(filters.len());
        let indexed_columns = self.get_indexed_columns_names();
        for filter in filters {
            let mut columns = HashSet::new();
            expr_to_columns(filter, &mut columns)?;

            for column in columns {
                let pushdown = if indexed_columns.contains(&column.name) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                };
                log::debug!("Pushdown: {:?} for column: {}", pushdown, column.name);
                pushdowns.push(pushdown);
            }
        }

        Ok(pushdowns)
    }
}

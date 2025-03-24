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
use datafusion::logical_expr::Operator;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion_common::Result;
use std::collections::HashMap;
use std::sync::Arc;

pub mod optimizer;
pub mod physical;

/// Represents information about an indexed column
#[derive(Debug, Clone)]
pub struct IndexedColumn {
    /// Name of the column that is indexed
    pub name: String,
    /// List of supported operators for this index
    pub supported_operators: Vec<Operator>,
}

/// A provider that supports indexed column lookups
#[async_trait]
pub trait IndexProvider: TableProvider {
    /// Returns a map of column names to their index information
    fn get_indexed_columns(&self) -> HashMap<String, IndexedColumn>;

    /// Returns whether a specific column and operator combination is supported
    fn supports_operator(&self, column: &str, op: &Operator) -> bool {
        self.get_indexed_columns()
            .get(column)
            .map(|idx| idx.supported_operators.contains(op))
            .unwrap_or(false)
    }

    /// Optimizes a list of expressions by combining them when possible
    /// For example, multiple expressions on the same column could be combined into a single expression
    /// Returns a new list of optimized expressions
    fn optimize_exprs(&self, exprs: &[Expr]) -> Result<Vec<Expr>> {
        // Default implementation returns expressions as-is
        Ok(exprs.to_vec())
    }

    /// Creates an IndexLookupExec for the given filter expression
    /// Returns None if the filter cannot use an index
    fn create_index_lookup(&self, expr: &Expr) -> Result<Option<Arc<dyn ExecutionPlan>>>;

    /// Creates an execution plan that combines multiple index lookups
    /// Default implementation uses HashJoinExec when multiple indexes are used
    fn create_index_join(
        &self,
        lookups: Vec<Arc<dyn ExecutionPlan>>,
        projection: Option<&Vec<usize>>,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

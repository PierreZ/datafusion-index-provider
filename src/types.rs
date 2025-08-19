//! Core type definitions for representing index-based filter operations.
//!
//! This module defines the [`IndexFilter`] enum which represents the structure of
//! filter operations that can be pushed down to indexes. These types form the
//! foundation for the query optimization and execution plan generation process.

use std::sync::Arc;

use datafusion::prelude::Expr;

use crate::physical_plan::Index;
use std::fmt;

/// Represents the structure of filter expressions that can be executed using indexes.
///
/// This enum captures the hierarchical structure of filter conditions and their
/// mapping to physical indexes. It serves as an intermediate representation between
/// SQL filter expressions and physical execution plans.
///
/// ## Execution Plan Mapping
/// Each variant maps to a specific execution strategy:
/// - `Single`: Direct index scan via `IndexScanExec`
/// - `And`: Index intersection via cascaded joins (`IndexLookupJoin`)  
/// - `Or`: Index union via `UnionExec` + `AggregateExec` for deduplication
#[derive(Debug, Clone)]
pub enum IndexFilter {
    /// A filter condition that can be handled by a single index.
    ///
    /// This represents the base case where one index can directly answer a filter predicate.
    /// The filter expression should be compatible with the index's `supports_predicate()` method.
    Single {
        /// The index that will handle this filter
        index: Arc<dyn Index>,
        /// The filter expression to be evaluated by the index
        filter: Expr,
    },

    /// A conjunction (AND) of multiple filter conditions across different indexes.
    ///
    /// This represents scenarios where multiple indexes must be consulted and their
    /// results intersected to find rows that satisfy ALL conditions. The execution
    /// strategy builds a left-deep tree of joins to progressively narrow the result set.
    And(Vec<IndexFilter>),

    /// A disjunction (OR) of multiple filter conditions across different indexes.
    ///
    /// This represents scenarios where any of several conditions can be satisfied.
    /// The execution strategy unions results from all indexes and deduplicates to
    /// ensure each row appears only once in the final result.
    Or(Vec<IndexFilter>),
}

/// A collection of [`IndexFilter`]s representing a complete index-based query plan.
///
/// This type represents the complete set of index operations needed to satisfy a query's
/// filter conditions. It is typically the output of `analyze_and_optimize_filters()` and
/// serves as input to `create_execution_plan_with_indexes()` for physical plan generation.
pub type IndexFilters = Vec<IndexFilter>;

impl fmt::Display for IndexFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexFilter::Single { index, .. } => write!(f, "{}", index.name()),
            IndexFilter::And(filters) => {
                let parts: Vec<String> = filters.iter().map(|f| f.to_string()).collect();
                write!(f, "({})", parts.join(" AND "))
            }
            IndexFilter::Or(filters) => {
                let parts: Vec<String> = filters.iter().map(|f| f.to_string()).collect();
                write!(f, "({})", parts.join(" OR "))
            }
        }
    }
}

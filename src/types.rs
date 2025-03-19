//! Common type definitions used throughout the crate.

use std::sync::Arc;

use datafusion::prelude::Expr;

use crate::physical_plan::Index;
use std::fmt;

/// Represents a filter expression that can be pushed down to an index.
#[derive(Debug, Clone)]
pub enum IndexFilter {
    /// Represents one or more filter expressions handled by a single index.
    Single { index: Arc<dyn Index>, filter: Expr },
    /// Represents a conjunction (AND) of filters handled by different indexes.
    And(Vec<IndexFilter>),
    /// Represents a disjunction (OR) of filters handled by different indexes.
    Or(Vec<IndexFilter>),
}

/// A list of [`IndexFilter`]s.
///
/// This represents the collection of all indexes that will be used to satisfy a query,
/// along with the specific filters each index will be responsible for.
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

//! Common type definitions used throughout the crate.

use std::sync::Arc;

use datafusion::prelude::Expr;

use crate::physical_plan::Index;

/// A tuple representing an [`Index`] and the filter expressions that it can evaluate.
///
/// This is a key data structure used to represent the outcome of filter analysis,
/// where a set of `Expr`s is associated with a specific index that can handle them.
pub type IndexFilter = (Arc<dyn Index>, Vec<Expr>);

/// A list of [`IndexFilter`]s.
///
/// This represents the collection of all indexes that will be used to satisfy a query,
/// along with the specific filters each index will be responsible for.
pub type IndexFilters = Vec<IndexFilter>;

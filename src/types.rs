use std::sync::Arc;

use datafusion::prelude::Expr;

use crate::physical_plan::Index;

/// A tuple of an index and the filters that can be applied to it
pub type IndexFilter = (Arc<dyn Index>, Vec<Expr>);

/// A list of index filters
pub type IndexFilters = Vec<IndexFilter>;

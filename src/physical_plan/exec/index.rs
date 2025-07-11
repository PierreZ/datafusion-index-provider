use crate::physical_plan::Index;
use arrow::datatypes::SchemaRef;
use datafusion::prelude::Expr;
use std::sync::Arc;

#[derive(Debug)]
pub struct IndexScanExec {
    /// The index to scan.
    index: Arc<dyn Index>,
    /// The filters to apply to the index.
    filters: Vec<Expr>,
    /// The schema of the output of this exec node.
    schema: SchemaRef,
    // ... plan properties
}

// TODO Implement ExecutionPlan

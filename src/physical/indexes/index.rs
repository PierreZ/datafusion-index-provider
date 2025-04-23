use arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::physical_plan::{SendableRecordBatchStream, Statistics};
use datafusion::prelude::Expr;
use std::any::Any;
use std::fmt;

/// Default column name for row IDs produced by index scans.
pub const ROW_ID_COLUMN_NAME: &str = "__row_id__";

/// Represents a physical index structure that can be scanned.
pub trait Index: fmt::Debug + Send + Sync + 'static {
    /// Returns the index as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Get the name of this index
    fn name(&self) -> &str;

    /// Get the schema of the index *output* (usually the row identifiers)
    fn index_schema(&self) -> SchemaRef;

    /// Get the name of the table this index applies to
    fn table_name(&self) -> &str;

    /// Check if this index can potentially satisfy (part of) the given predicate.
    /// This is a fast check that should avoid accessing index metadata if possible.
    fn supports_predicate(&self, predicate: &Expr) -> Result<bool>;

    /// Create a physical plan that scans the index.
    /// The plan should yield row identifiers (e.g., UInt64) satisfying the predicate.
    /// The `projection` specifies which *indexed* columns are needed.
    /// If `projection` is None or empty, only the row identifier is required.
    /// If the query requires only columns available within the index itself
    /// (an "index-only scan"), this scan node might be able to return that data directly.
    fn scan(
        &self,
        predicate: &Expr,
        projection: Option<&Vec<usize>>,
    ) -> Result<SendableRecordBatchStream>;

    /// Provides statistics for the index (e.g., cardinality)
    /// Used by the optimizer for cost-based decisions.
    fn statistics(&self) -> Statistics;

    // Optional: Add methods for index maintenance (insert, delete, update) later
    // fn insert(&self, batch: &RecordBatch) -> Result<()>;
    // fn delete(&self, predicate: &Expr) -> Result<()>;
}

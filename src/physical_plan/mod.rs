pub mod exec;
pub mod fetcher;
pub mod joins;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{Result, Statistics};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{utils::expr_to_columns, Expr};
use datafusion_common::DataFusionError;
use std::any::Any;
use std::collections::HashSet;
use std::fmt;

pub const ROW_ID_COLUMN_NAME: &str = "__row_id__";

use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::expressions::Column as PhysicalColumn;
use datafusion::physical_plan::{Partitioning, PlanProperties};
use std::sync::Arc;

/// Creates a `PlanProperties` for a scan that is potentially ordered by the row ID.
pub fn create_plan_properties_for_row_id_scan(schema: SchemaRef, ordered: bool) -> PlanProperties {
    let mut eq_properties = EquivalenceProperties::new(schema);
    if ordered {
        eq_properties.add_new_ordering(LexOrdering::new(vec![PhysicalSortExpr::new_default(
            Arc::new(PhysicalColumn::new(ROW_ID_COLUMN_NAME, 0)),
        )
        .asc()]));
    }
    PlanProperties::new(
        eq_properties,
        Partitioning::UnknownPartitioning(1),
        EmissionType::Incremental,
        Boundedness::Bounded,
    )
}

/// Creates a schema for an index with a single column of the specified data type.
pub fn create_index_schema(data_type: DataType) -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        ROW_ID_COLUMN_NAME,
        data_type,
        false,
    )]))
}

/// Represents a physical index that can be scanned to find row IDs.
pub trait Index: fmt::Debug + Send + Sync + 'static {
    /// Returns the index as `Any` for downcasting.
    fn as_any(&self) -> &dyn Any;

    /// The name of this index.
    fn name(&self) -> &str;

    /// The schema of the index output (a single column of row IDs).
    fn index_schema(&self) -> SchemaRef;

    /// The name of the table this index belongs to.
    fn table_name(&self) -> &str;

    /// The name of the column this index primarily covers.
    /// Used for simple, single-column indexes to quickly check for predicate support.
    fn column_name(&self) -> &str;

    /// A fast check to see if this index can potentially satisfy the predicate.
    /// The default implementation checks if any column in the predicate matches `column_name`.
    fn supports_predicate(&self, predicate: &Expr) -> Result<bool> {
        let mut columns = HashSet::new();
        expr_to_columns(predicate, &mut columns)?;
        Ok(columns.iter().any(|col| col.name == self.column_name()))
    }

    /// Creates a physical plan that scans the index and returns row IDs.
    /// The output of this plan MUST have a schema matching `index_schema()`.
    fn scan(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<SendableRecordBatchStream, DataFusionError>;

    /// Provides statistics for the index.
    fn statistics(&self) -> Statistics;
}

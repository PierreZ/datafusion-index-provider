//! Physical plan traits and operators for index-based scanning.
//!
//! This module provides the core [`Index`] trait, which defines the interface
//! for a physical index, as well as various `ExecutionPlan` implementations
//! that use indexes to scan and fetch data.
pub mod exec;
pub mod fetcher;
pub mod joins;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{Result, Statistics};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{utils::expr_to_columns, Expr};
use datafusion_common::DataFusionError;
use std::any::Any;
use std::collections::HashSet;
use std::fmt;

/// The name of the column that contains the row ID.
/// This column is used to join the results of an index scan with the base table.
pub const ROW_ID_COLUMN_NAME: &str = "__row_id__";

use datafusion::physical_expr::{EquivalenceProperties, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::expressions::Column as PhysicalColumn;
use datafusion::physical_plan::{Partitioning, PlanProperties};
use std::sync::Arc;

/// Creates a `PlanProperties` for a scan that is potentially ordered by the row ID.
///
/// # Arguments
/// * `schema` - The schema of the output plan.
/// * `ordered` - Whether the output is ordered by the row ID.
pub fn create_plan_properties_for_row_id_scan(schema: SchemaRef, ordered: bool) -> PlanProperties {
    let mut eq_properties = EquivalenceProperties::new(schema);
    if ordered {
        let sort_expr = PhysicalSortExpr::new_default(Arc::new(
            PhysicalColumn::new(ROW_ID_COLUMN_NAME, 0),
        )).asc();
        eq_properties.add_ordering(vec![sort_expr]);
    }
    PlanProperties::new(
        eq_properties,
        Partitioning::RoundRobinBatch(1),
        EmissionType::Incremental,
        Boundedness::Bounded,
    )
}

/// Creates a schema for an index with a single column of the specified data type.
/// The column will be named [`ROW_ID_COLUMN_NAME`].
///
/// # Arguments
/// * `data_type` - The data type of the row ID column.
pub fn create_index_schema(data_type: DataType) -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        ROW_ID_COLUMN_NAME,
        data_type,
        false,
    )]))
}

/// Represents a physical index that can be scanned to find row IDs.
///
/// An `Index` is a physical operator that, given a set of predicates, can
/// efficiently produce a stream of row identifiers (e.g., row numbers, or
/// primary keys) that satisfy those predicates.
pub trait Index: fmt::Debug + Send + Sync + 'static {
    /// Returns the index as [`Any`] for downcasting.
    fn as_any(&self) -> &dyn Any;

    /// Returns the name of this index.
    fn name(&self) -> &str;

    /// Returns the schema of the index output, which must consist of a single
    /// column named [`ROW_ID_COLUMN_NAME`].
    fn index_schema(&self) -> SchemaRef;

    /// Returns the name of the table this index belongs to.
    fn table_name(&self) -> &str;

    /// Returns the name of the column this index primarily covers.
    ///
    /// This is used by the default implementation of [`Self::supports_predicate`] to
    /// perform a simple check for predicate support.
    fn column_name(&self) -> &str;

    /// A fast check to see if this index can potentially satisfy the predicate.
    ///
    /// # Default implementation
    /// The default implementation checks if any column referenced in the predicate
    /// matches the value of [`Self::column_name`].
    fn supports_predicate(&self, predicate: &Expr) -> Result<bool> {
        let mut columns = HashSet::new();
        expr_to_columns(predicate, &mut columns)?;
        Ok(columns.iter().any(|col| col.name == self.column_name()))
    }

    /// Returns indication if the index is ordered by the row ID.
    ///
    /// # Default implementation
    /// The default implementation returns `false`.
    fn is_ordered(&self) -> bool {
        false
    }

    /// Creates a stream of row IDs that satisfy the given filters.
    ///
    /// The output of this stream MUST have a schema matching [`Self::index_schema()`].
    fn scan(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<SendableRecordBatchStream, DataFusionError>;

    /// Provides statistics for the index data.
    fn statistics(&self) -> Statistics;
}

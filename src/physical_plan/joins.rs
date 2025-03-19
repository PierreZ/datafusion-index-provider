//! Physical execution plans for joining index scan results in AND operations.
//!
//! This module provides functionality to create optimized join execution plans that
//! intersect row IDs from multiple index scans. The joins are used to implement
//! AND conditions across multiple indexed columns by finding the intersection of
//! row IDs that satisfy all individual predicates.

use super::ROW_ID_COLUMN_NAME;
use datafusion::common::Result;
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::expressions::Column as PhysicalColumn;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion_common::NullEquality;
use std::sync::Arc;

/// Creates an optimized join execution plan to intersect row IDs from two index scans.
///
/// This function automatically selects the most efficient join algorithm based on the
/// ordering properties of the input execution plans:
///
/// ## Join Algorithm Selection
/// - **SortMergeJoin**: Used when both inputs are ordered by row ID, providing O(n+m) complexity
/// - **HashJoin**: Used when inputs are unordered, providing O(n+m) average complexity with O(n) space
///
/// ## Performance Characteristics
/// - **SortMergeJoin**: Memory-efficient streaming join, ideal for large ordered datasets
/// - **HashJoin**: Builds hash table from left input, efficient for smaller left side
///
/// The join is always an INNER join since the goal is to find row IDs that satisfy
/// ALL conditions (intersection semantics for AND operations).
///
/// # Arguments
/// * `left` - Left execution plan producing row IDs (typically becomes hash table in HashJoin)
/// * `right` - Right execution plan producing row IDs
///
/// # Returns
/// An execution plan that produces row IDs present in both inputs, maintaining the
/// row ID schema with a single `__row_id__` column.
///
/// # Performance Tips
/// For optimal performance:
/// - Place the more selective index scan on the left side for HashJoin
/// - Ensure row ID columns are properly typed and indexed
/// - Consider the memory vs. CPU trade-offs between join algorithms
pub fn try_create_index_lookup_join(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Create join columns - both inputs have the row ID column at index 0
    let join_on = vec![(
        Arc::new(PhysicalColumn::new(ROW_ID_COLUMN_NAME, 0)) as Arc<dyn PhysicalExpr>,
        Arc::new(PhysicalColumn::new(ROW_ID_COLUMN_NAME, 0)) as Arc<dyn PhysicalExpr>,
    )];

    // Check if both inputs are sorted the same way for SortMergeJoin optimization
    let both_sorted = match (
        left.properties().output_ordering(),
        right.properties().output_ordering(),
    ) {
        (Some(left_ord), Some(right_ord)) if left_ord.eq(right_ord) => {
            Some(left_ord.iter().map(|e| e.options).collect())
        }
        _ => None,
    };

    match both_sorted {
        Some(sort_options) => Ok(Arc::new(SortMergeJoinExec::try_new(
            left,
            right,
            join_on,
            None,
            JoinType::Inner,
            sort_options,
            NullEquality::NullEqualsNull,
        )?)),
        None => Ok(Arc::new(HashJoinExec::try_new(
            left,
            right,
            join_on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNull,
        )?)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::create_plan_properties_for_row_id_scan;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::common::Statistics;
    use datafusion::execution::context::TaskContext;
    use datafusion::execution::SendableRecordBatchStream;

    use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
    use std::any::Any;
    use std::fmt;

    /// A mock execution plan that can be configured to be ordered or not.
    #[derive(Debug)]
    struct MockExec {
        plan_properties: PlanProperties,
        schema: SchemaRef,
    }

    impl MockExec {
        fn new(ordered: bool) -> Self {
            let schema = Arc::new(Schema::new(vec![Field::new(
                ROW_ID_COLUMN_NAME,
                DataType::UInt64,
                false,
            )]));

            let plan_properties = create_plan_properties_for_row_id_scan(schema.clone(), ordered);

            Self {
                plan_properties,
                schema,
            }
        }
    }

    impl DisplayAs for MockExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "MockExec")
        }
    }

    impl ExecutionPlan for MockExec {
        fn name(&self) -> &str {
            "MockExec"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn properties(&self) -> &PlanProperties {
            &self.plan_properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            unimplemented!()
        }

        fn statistics(&self) -> Result<Statistics> {
            unimplemented!()
        }
    }

    #[test]
    fn test_uses_sort_merge_join_for_ordered_inputs() -> Result<()> {
        let left = Arc::new(MockExec::new(true));
        let right = Arc::new(MockExec::new(true));

        let join_plan = try_create_index_lookup_join(left, right)?;

        assert_eq!(join_plan.name(), "SortMergeJoinExec");
        Ok(())
    }

    #[test]
    fn test_uses_hash_join_for_unordered_inputs() -> Result<()> {
        // One side unordered
        let left = Arc::new(MockExec::new(true));
        let right = Arc::new(MockExec::new(false));
        let join_plan = try_create_index_lookup_join(left, right)?;
        assert_eq!(join_plan.name(), "HashJoinExec");

        // Both sides unordered
        let left = Arc::new(MockExec::new(false));
        let right = Arc::new(MockExec::new(false));
        let join_plan = try_create_index_lookup_join(left, right)?;
        assert_eq!(join_plan.name(), "HashJoinExec");

        Ok(())
    }
}

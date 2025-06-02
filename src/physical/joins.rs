//! Physical execution components for index joins.

use std::sync::Arc;

use crate::ROW_ID_COLUMN_NAME;
use datafusion::error::Result;
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::expressions::Column as PhysicalColumn;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use datafusion::physical_plan::ExecutionPlan;

/// Attempts to join two index lookup execution plans.
/// Chooses SortMergeJoinExec if both inputs report sorted output on the 'index' column,
/// otherwise uses HashJoinExec.
pub fn try_create_index_lookup_join(
    left: Arc<dyn ExecutionPlan>,
    left_ordered: bool,
    right: Arc<dyn ExecutionPlan>,
    right_ordered: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Assuming both inputs have the row ID column at index 0
    let left_col = PhysicalColumn::new(ROW_ID_COLUMN_NAME, 0);
    let right_col = PhysicalColumn::new(ROW_ID_COLUMN_NAME, 0);

    // Wrap columns in Arc<dyn PhysicalExpr> for the join functions
    let left_col_expr = Arc::new(left_col) as Arc<dyn PhysicalExpr>;
    let right_col_expr = Arc::new(right_col) as Arc<dyn PhysicalExpr>;
    let join_on = vec![(left_col_expr, right_col_expr)];

    let use_sort_merge = left_ordered && right_ordered;

    if use_sort_merge {
        // Both inputs are sorted on the join key, use SortMergeJoinExec
        Ok(Arc::new(SortMergeJoinExec::try_new(
            left.clone(),
            right.clone(),
            join_on,         // on: JoinOn
            None,            // filter: Option<JoinFilter>
            JoinType::Inner, // Use Inner join for intersection
            vec![],
            false,
        )?))
    } else {
        // Use HashJoinExec
        Ok(Arc::new(HashJoinExec::try_new(
            left.clone(),
            right.clone(),
            join_on,
            None, // No filter
            &JoinType::Inner,
            None, // projection: Option<Vec<usize>>
            PartitionMode::CollectLeft,
            false, // null_equals_null: bool
        )?))
    }
}

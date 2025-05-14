//! Physical execution components for index joins.

use std::sync::Arc;

use crate::ROW_ID_COLUMN_NAME;
use datafusion::error::Result;
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::expressions::Column as PhysicalColumn;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

/// Attempts to join two index lookup execution plans.
/// Chooses SortMergeJoinExec if both inputs report sorted output on the 'index' column,
/// otherwise uses HashJoinExec.
pub fn try_create_lookup_join(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Assuming both inputs have the row ID column at index 0
    let left_col = PhysicalColumn::new(ROW_ID_COLUMN_NAME, 0);
    let right_col = PhysicalColumn::new(ROW_ID_COLUMN_NAME, 0);

    // Wrap columns in Arc<dyn PhysicalExpr> for the join functions
    let left_col_expr = Arc::new(left_col) as Arc<dyn PhysicalExpr>;
    let right_col_expr = Arc::new(right_col) as Arc<dyn PhysicalExpr>;
    let join_on = vec![(left_col_expr, right_col_expr)];

    // Check if both inputs are sorted on the join key ('__row_id__' column)
    let left_ordering = left.output_ordering();
    let right_ordering = right.output_ordering();

    let use_sort_merge = match (left_ordering, right_ordering) {
        (Some(lo), Some(ro)) => {
            // Check if both are sorted on the first column (index)
            // and the column names match 'index'
            lo.len() == 1
                && ro.len() == 1
                && lo[0]
                    .expr
                    .as_any()
                    .downcast_ref::<PhysicalColumn>()
                    .map(|c| c.name() == ROW_ID_COLUMN_NAME)
                    .unwrap_or(false)
                && ro[0]
                    .expr
                    .as_any()
                    .downcast_ref::<PhysicalColumn>()
                    .map(|c| c.name() == ROW_ID_COLUMN_NAME)
                    .unwrap_or(false)
        }
        _ => false,
    };

    if use_sort_merge {
        // Both inputs are sorted on the join key, use SortMergeJoinExec
        // Re-fetch left_ordering as it was moved in the match
        let left_ordering = left.output_ordering();
        Ok(Arc::new(SortMergeJoinExec::try_new(
            left.clone(),
            right.clone(),
            join_on,                                 // on: JoinOn
            None,                                    // filter: Option<JoinFilter>
            JoinType::Inner,                         // Use Inner join for intersection
            vec![left_ordering.unwrap()[0].options], // Extract SortOptions
            false,                                   // null_equals_null is false for standard joins
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

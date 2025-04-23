//! Physical execution components for index joins.

use std::sync::Arc;

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
    // Define join columns ("index")
    let left_col = Arc::new(PhysicalColumn::new("index", 0)) as Arc<dyn PhysicalExpr>;
    let right_col = Arc::new(PhysicalColumn::new("index", 0)) as Arc<dyn PhysicalExpr>;
    let join_on = vec![(left_col, right_col)];

    // Check if both inputs are sorted on the join key ('index' column)
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
                    .map(|c| c.name() == "index")
                    .unwrap_or(false)
                && ro[0]
                    .expr
                    .as_any()
                    .downcast_ref::<PhysicalColumn>()
                    .map(|c| c.name() == "index")
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
            join_on, // on: JoinOn
            None,    // filter: Option<JoinFilter>
            JoinType::Inner,
            vec![left_ordering.unwrap()[0].options], // sort_options: Vec<SortOptions>
            false,                                   // null_equals_null: bool
        )?))
    } else {
        // Use HashJoinExec
        Ok(Arc::new(HashJoinExec::try_new(
            left.clone(),
            right.clone(),
            join_on, // on: JoinOn
            None,    // filter: Option<JoinFilter>
            &JoinType::Inner,
            None, // projection: Option<Vec<usize>>
            PartitionMode::CollectLeft,
            false, // null_equals_null: bool
        )?))
    }
}

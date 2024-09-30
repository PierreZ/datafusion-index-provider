use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::DataFusionError;

#[derive(Default, Debug)]
pub struct IndexAwareOptimizer {}

impl IndexAwareOptimizer {
    pub(crate) fn rewrite_expr(&self, expression: Expr) -> Result<Transformed<Expr>, DataFusionError> {
        dbg!(&expression);

        match &expression {
            Expr::BinaryExpr(BinaryExpr { op, left, right}) => {
                let can_be_transformed = match op {
                    Operator::Eq => true,
                //  Operator::Lt => true,
                //  Operator::LtEq => true,
                //  Operator::Gt => true,
                //  Operator::GtEq => true,
                    _ => false,
                };
                dbg!(&op, &left, &right);
                unimplemented!()
            },
            _ => Ok(Transformed::no(expression)),
        }
    }
}

impl OptimizerRule for IndexAwareOptimizer {
    fn name(&self) -> &str {
        "IndexAwareOptimizer"
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        plan.map_expressions(|expr| {
            self.rewrite_expr(expr)
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::optimizer::IndexAwareOptimizer;
    use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    // https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/optimizer_rule.rs
    #[tokio::test]
    async fn optimizer() {
        // Note you can change the order of optimizer rules using the lower level
        // `SessionState` API
        let ctx = SessionContext::new();
        ctx.add_optimizer_rule(Arc::new(IndexAwareOptimizer {}));

        ctx.register_batch("person", person_batch())
            .expect("could not register batch");
        let sql = "SELECT * FROM person WHERE age = 22";
        let plan = ctx
            .sql(sql)
            .await
            .expect("could not inject sql")
            .into_optimized_plan()
            .expect("could not create optimized plan");

        // We can see the effect of our rewrite on the output plan that the filter
        // has been rewritten to `my_eq`
        assert_eq!(
            plan.display_indent().to_string(),
            "Filter: my_eq(person.age, Int32(22))\
        \n  TableScan: person projection=[name, age]"
        );
    }
    /// Return a RecordBatch with made up data
    fn person_batch() -> RecordBatch {
        let name: ArrayRef = Arc::new(StringArray::from_iter_values(["Andy", "Andrew", "Oleks"]));
        let age: ArrayRef = Arc::new(Int32Array::from(vec![11, 22, 33]));
        RecordBatch::try_from_iter(vec![("name", name), ("age", age)]).unwrap()
    }
}

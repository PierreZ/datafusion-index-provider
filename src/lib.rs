use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use std::collections::HashMap;

pub mod index_scan;

pub trait IndexProvider: TableProvider {
    fn get_indexes(&self) -> HashMap<String, Vec<Operator>>;

    fn supports_index_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion_common::Result<Vec<TableProviderFilterPushDown>> {
        dbg!(filters);
        let indexes = self.get_indexes();
        let supports = filters
            .iter()
            .map(|expr| {
                dbg!(expr);
                if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr {
                    if let Some(column) = left.try_as_col() {
                        let name = &column.name;
                        if let Some(operators) = indexes.get(name) {
                            if operators.contains(op) {
                                dbg!(name, op);
                                return TableProviderFilterPushDown::Exact;
                            }
                        }
                    }
                }

                TableProviderFilterPushDown::Unsupported
            })
            .collect();
        Ok(supports)
    }
}

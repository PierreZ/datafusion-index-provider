use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DataFusionError;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use datafusion_common::config::ConfigOptions;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

struct IndexAwareOptimizerRule;

impl Debug for IndexAwareOptimizerRule {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexAwareOptimizerRule").finish()
    }
}

impl PhysicalOptimizerRule for IndexAwareOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        dbg!(&plan);

        Ok(plan)
    }

    fn name(&self) -> &str {
        "IndexAwareOptimizerRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn physical_plan() -> Result<(), DataFusionError> {
    // create a default table source
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    // To create an ExecutionPlan we must provide an actual
    // TableProvider. For this example, we don't provide any data
    // but in production code, this would have `RecordBatch`es with
    // in memory data
    let table_provider = Arc::new(MemTable::try_new(Arc::new(schema), vec![])?);
    // Use the provider_as_source function to convert the TableProvider to a table source
    let table_source = provider_as_source(table_provider);

    // create a LogicalPlanBuilder for a table scan
    let builder = LogicalPlanBuilder::scan("person", table_source, None)?;

    // perform a filter operation and build the plan
    let logical_plan = builder
        .filter(col("id").eq(lit(500)))? // WHERE id = 500
        .build()?;

    println!("{}", logical_plan.display_indent_schema());

    println!("----------");
    println!("----------");

    // Now create the physical plan by calling `create_physical_plan`
    let ctx = SessionStateBuilder::new()
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(IndexAwareOptimizerRule))
        .build();
    let physical_plan = ctx.create_physical_plan(&logical_plan).await?;

    // print the plan
    println!(
        "{}",
        DisplayableExecutionPlan::new(physical_plan.as_ref()).indent(true)
    );
    Ok(())
}

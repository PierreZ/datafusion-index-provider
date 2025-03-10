use crate::common::{get_users, scan_age_index, User};
use arrow::array::{RecordBatch, UInt64Array};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::{provider_as_source, TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{
    col, lit, BinaryExpr, Expr, LogicalPlanBuilder, Operator, TableProviderFilterPushDown,
    UserDefinedLogicalNode,
};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::prelude::SessionContext;
use datafusion_common::{project_schema, DataFusionError, ScalarValue};
use datafusion_index_provider::index_scan::{MapIndexWithRecord, ScanWithIndexStream};
use datafusion_index_provider::IndexProvider;
use std::any::Any;
use std::collections::Bound::Included;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::ops::Bound::Excluded;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

mod common;

// Mapping between id and age from populated users:
// id: 1 -> age: 15
// id: 2 -> age: 20
// id: 3 -> age: 50

#[tokio::test]
async fn physical_plan_no_result() {
    let custom_db = Arc::new(CustomDataSource::default());

    search_accounts(custom_db.clone(), Some(col("age").eq(lit(999))), vec![])
        .await
        .unwrap();
}

#[tokio::test]
async fn physical_plan_eq_15() {
    let custom_db = Arc::new(CustomDataSource::default());

    search_accounts(custom_db.clone(), Some(col("age").eq(lit(15))), vec![1])
        .await
        .unwrap();
}

#[tokio::test]
async fn physical_plan_greater_than_15() {
    let custom_db = Arc::new(CustomDataSource::default());

    search_accounts(custom_db.clone(), Some(col("age").gt(lit(15))), vec![2, 3])
        .await
        .unwrap();
}

#[tokio::test]
async fn physical_plan_greater_eq_than_15() {
    let custom_db = Arc::new(CustomDataSource::default());

    search_accounts(
        custom_db.clone(),
        Some(col("age").gt_eq(lit(15))),
        vec![1, 2, 3],
    )
    .await
    .unwrap();
}

async fn search_accounts(
    db: Arc<CustomDataSource>,
    filter: Option<Expr>,
    expected_ids: Vec<u64>,
) -> Result<(), DataFusionError> {
    // create local execution context
    let ctx = SessionContext::new();

    // create logical plan composed of a single TableScan
    let logical_plan =
        LogicalPlanBuilder::scan_with_filters("accounts", provider_as_source(db), None, vec![])?
            .build()?;

    let mut dataframe = DataFrame::new(ctx.state(), logical_plan);

    if let Some(f) = filter {
        dataframe = dataframe.filter(f)?;
    }

    timeout(Duration::from_secs(10), async move {
        let result = dataframe.collect().await.unwrap();
        let record_batch = result.first().unwrap();

        let ids = record_batch.column_by_name("id").unwrap();
        let ids: Vec<u64> = ids
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .iter()
            .flatten()
            .collect();

        assert_eq!(
            expected_ids, ids,
            "received ids: {:?}, was expecting {:?}",
            ids, expected_ids
        );

        dbg!(record_batch.columns());
    })
    .await
    .unwrap();

    Ok(())
}

#[derive(Default, Clone)]
pub struct CustomDataSource {}

impl CustomDataSource {
    pub(crate) async fn create_index_aware_physical_plan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        dbg!(projection, filters, limit);
        Ok(Arc::new(RecordExec::try_new(
            projection,
            filters,
            self.schema(),
            limit,
        )?))
    }
}

impl Debug for CustomDataSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("CustomDataSource")
    }
}

#[async_trait]
impl TableProvider for CustomDataSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        User::get_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        self.create_index_aware_physical_plan(state, projection, filters, limit)
            .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion_common::Result<Vec<TableProviderFilterPushDown>> {
        self.supports_index_pushdown(filters)
    }
}

impl IndexProvider for CustomDataSource {
    fn get_indexes(&self) -> HashMap<String, Vec<Operator>> {
        HashMap::from([(
            "age".to_string(),
            vec![
                Operator::Eq,
                Operator::Gt,
                Operator::GtEq,
                Operator::Lt,
                Operator::LtEq,
            ],
        )])
    }
}

struct RecordExec {
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
    index_provider: Arc<dyn IndexProvider>,
    cache: PlanProperties,
}

impl RecordExec {
    pub fn try_new(
        projections: Option<&Vec<usize>>,
        filters: &[Expr],
        schema: SchemaRef,
        limit: Option<usize>,
    ) -> Result<Self, DataFusionError> {
        let projected_schema = project_schema(&schema, projections).unwrap();
        let cache: PlanProperties = Self::compute_properties(projected_schema.clone());
        Ok(Self {
            projection: projections.cloned(),
            filters: filters.to_vec(),
            limit,
            index_provider: Arc::new(CustomDataSource::default()),
            cache,
        })
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl Debug for RecordExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("RecordExec")
    }
}

impl DisplayAs for RecordExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        f.write_str("RecordExec")
    }
}

impl ExecutionPlan for RecordExec {
    fn name(&self) -> &str {
        "RecordExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if self.filters.is_empty() {
            return Ok(self);
        }
        let indexes = self.index_provider.get_indexes();

        match self.filters.first() {
            None => return Ok(self),
            Some(filter) => {
                match dbg!(filter) {
                    Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                        match (left.as_ref(), op, right.as_ref()) {
                            // Eq
                            (Expr::Column(col_name), Operator::Eq, Expr::Literal(scalar_value))
                            | (Expr::Literal(scalar_value), Operator::Eq, Expr::Column(col_name)) => {
                                if let ScalarValue::UInt64(Some(v)) = scalar_value {
                                    let range = (Included(v.to_owned()), Included(v.to_owned()));
                                    let scan_index = scan_age_index(range);

                                    Ok(Arc::new(MapRecordExec {
                                        input: scan_index,
                                        schema: User::get_schema(),
                                        cache: RecordExec::compute_properties(User::get_schema()),
                                    }))
                                } else {
                                    unimplemented!()
                                }
                            }
                            // Gt and GtEq
                            (Expr::Column(col_name), Operator::Gt, Expr::Literal(scalar_value))
                            | (Expr::Literal(scalar_value), Operator::Gt, Expr::Column(col_name))
                            | (
                                Expr::Column(col_name),
                                Operator::GtEq,
                                Expr::Literal(scalar_value),
                            )
                            | (
                                Expr::Literal(scalar_value),
                                Operator::GtEq,
                                Expr::Column(col_name),
                            ) => {
                                if let ScalarValue::UInt64(Some(v)) = scalar_value {
                                    let range = if op.eq(&Operator::Gt) {
                                        (Excluded(v.to_owned()), Excluded(u64::MAX))
                                    } else {
                                        (Included(v.to_owned()), Included(u64::MAX))
                                    };
                                    let scan_index = scan_age_index(range);

                                    Ok(Arc::new(MapRecordExec {
                                        input: scan_index,
                                        schema: User::get_schema(),
                                        cache: RecordExec::compute_properties(User::get_schema()),
                                    }))
                                } else {
                                    unimplemented!()
                                }
                            }
                            _ => unimplemented!(),
                        }
                    }
                    _ => unimplemented!(),
                }
            }
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        unimplemented!()
    }
}

struct MapRecordExec {
    pub input: Arc<dyn ExecutionPlan>,
    pub schema: SchemaRef,
    cache: PlanProperties,
}

impl Debug for MapRecordExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("MapRecordExec")
    }
}

impl DisplayAs for MapRecordExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        f.write_str("MapRecordExec")
    }
}

impl ExecutionPlan for MapRecordExec {
    fn name(&self) -> &str {
        "MapRecordExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        dbg!(children);
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        dbg!(partition);

        let input = self.input.execute(partition, context.clone())?;

        Ok(Box::pin(ScanWithIndexStream::new(
            input,
            partition,
            Box::new(Mapper::new()),
        )))
    }
}

struct Mapper {}

impl Mapper {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl MapIndexWithRecord for Mapper {
    async fn map_index_with_record(
        &mut self,
        index_batch: RecordBatch,
    ) -> datafusion_common::Result<RecordBatch> {
        dbg!(&index_batch);
        let column = index_batch.column_by_name("id").unwrap();
        let index_entry = column.as_any().downcast_ref::<UInt64Array>().unwrap();
        let index_entry: Vec<u64> = index_entry.iter().flatten().collect();

        Ok(get_users(&index_entry).unwrap())
    }
}

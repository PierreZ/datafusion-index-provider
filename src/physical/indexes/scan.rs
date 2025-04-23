use crate::physical::indexes::index::Index;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use datafusion::prelude::Expr;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

pub const ROW_ID_COLUMN_NAME: &str = "__row_id__";

#[derive(Debug)]
pub struct IndexScanExec {
    index: Arc<dyn Index>,
    predicate: Expr,
    properties: PlanProperties,
    schema: SchemaRef,
}

impl IndexScanExec {
    pub fn new(index: Arc<dyn Index>, predicate: Expr) -> Result<Self> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            ROW_ID_COLUMN_NAME,
            DataType::UInt64,
            false, // Row IDs are typically not nullable
        )]));

        let eq_properties = EquivalenceProperties::new(schema.clone());
        let partitioning = Partitioning::UnknownPartitioning(1);
        let boundedness = Boundedness::Bounded;

        let properties = PlanProperties::new(
            eq_properties,
            partitioning,
            EmissionType::Incremental, // Corrected: Use Incremental
            boundedness,
        );

        Ok(Self {
            index,
            predicate,
            properties,
            schema,
        })
    }

    pub fn index(&self) -> &Arc<dyn Index> {
        &self.index
    }

    pub fn predicate(&self) -> &Expr {
        &self.predicate
    }
}

impl DisplayAs for IndexScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "IndexScanExec: index={}", self.index.name())?;
                write!(f, ", table={}", self.index.table_name())?;
                write!(f, ", predicate={:?}", self.predicate)?;
            }
        }
        Ok(())
    }
}

impl ExecutionPlan for IndexScanExec {
    fn name(&self) -> &str {
        "IndexScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>, // Context not used by Index::scan directly
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "IndexScanExec does not support partitioning, tried to execute partition {}",
                partition
            )));
        }

        self.index.scan(&self.predicate, None)
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.index.statistics())
    }
}

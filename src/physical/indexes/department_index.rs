use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::Statistics as DFStatistics;

use super::index::{Index, ROW_ID_COLUMN_NAME};

/// A simple index for string-based department lookups.
#[derive(Debug)]
pub struct DepartmentIndex {
    index: HashMap<String, HashSet<u64>>,
    schema: SchemaRef,
}

impl DepartmentIndex {
    /// Creates a new DepartmentIndex.
    pub fn new(data: &[(String, u64)]) -> Self {
        let mut index: HashMap<String, HashSet<u64>> = HashMap::new();
        for (department, row_id) in data {
            index.entry(department.clone()).or_default().insert(*row_id);
        }

        let schema = Arc::new(Schema::new(vec![Field::new(
            ROW_ID_COLUMN_NAME,
            DataType::UInt64,
            false,
        )]));

        Self { index, schema }
    }
}

impl Index for DepartmentIndex {
    fn name(&self) -> &str {
        "DepartmentIndex"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn index_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_name(&self) -> &str {
        "" // This index isn't tied to a specific table name
    }

    /// Supports simple equality filters on the 'department' column.
    fn supports_predicate(&self, expr: &Expr) -> Result<bool> {
        if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr {
            if *op == Operator::Eq {
                if let Expr::Column(col) = left.as_ref() {
                    if col.name == "department" {
                        if let Expr::Literal(ScalarValue::Utf8(Some(_))) = right.as_ref() {
                            return Ok(true);
                        }
                    }
                }
                // Also check if the sides are swapped
                if let Expr::Column(col) = right.as_ref() {
                    if col.name == "department" {
                        if let Expr::Literal(ScalarValue::Utf8(Some(_))) = left.as_ref() {
                            return Ok(true);
                        }
                    }
                }
            }
        }
        Ok(false)
    }

    /// Scan the index based on the already validated predicate.
    fn scan(
        &self,
        filter: &Expr,
        _projection: Option<&Vec<usize>>,
    ) -> Result<SendableRecordBatchStream> {
        let value = match filter {
            Expr::BinaryExpr(BinaryExpr { op, right, .. }) if *op == Operator::Eq => {
                match right.as_ref() {
                    Expr::Literal(ScalarValue::Utf8(Some(val))) => Ok(val),
                    _ => Err(DataFusionError::Internal(
                        "DepartmentIndex scan expected Utf8 literal".to_string(),
                    )),
                }
            }
            // Handle case where literal is on the left
            Expr::BinaryExpr(BinaryExpr { op, left, .. }) if *op == Operator::Eq => {
                match left.as_ref() {
                    Expr::Literal(ScalarValue::Utf8(Some(val))) => Ok(val),
                    _ => Err(DataFusionError::NotImplemented(
                        "DepartmentIndex scan expected Utf8 literal".to_string(),
                    )),
                }
            }
            _ => Err(DataFusionError::Internal(format!(
                "DepartmentIndex scan called with unsupported filter: {:?}",
                filter
            ))),
        }?; // Extract the string value from the literal

        let row_ids = self.index.get(value).cloned().unwrap_or_default();

        let array = Arc::new(UInt64Array::from_iter_values(row_ids));
        let batch = RecordBatch::try_new(self.schema.clone(), vec![array as ArrayRef])?;

        // Wrap the single batch in a stream
        let stream = futures::stream::once(async { Ok(batch) });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }

    fn statistics(&self) -> DFStatistics {
        // TODO: Implement reasonable statistics estimation
        DFStatistics::new_unknown(&self.schema)
    }
}

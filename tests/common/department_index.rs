use std::any::Any;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{Result, ScalarValue, Statistics};
use datafusion::logical_expr::{Expr, Operator};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::SendableRecordBatchStream;

use datafusion_index_provider::physical::indexes::index::Index;
use datafusion_index_provider::physical::indexes::scan::index_scan_schema;

/// A simple index structure that maps department values to row indices
#[derive(Debug, Clone)]
pub struct DepartmentIndex {
    // Maps department to a vector of row IDs (u64)
    index: BTreeMap<String, Vec<u64>>,
}

impl DepartmentIndex {
    pub fn new(departments: &StringArray) -> Self {
        let mut index: BTreeMap<_, Vec<u64>> = BTreeMap::new();
        for i in 0..departments.len() {
            let row_id = i as u64; // Assuming row ID is the array index
            if departments.is_valid(i) {
                let department = departments.value(i).to_string();
                index.entry(department).or_default().push(row_id);
            }
        }
        DepartmentIndex { index }
    }

    #[allow(dead_code)] // Keep in case compiler flags as unused
    pub fn filter_rows(&self, op: &Operator, value: &str) -> Vec<u64> {
        match op {
            Operator::Eq => self.index.get(value).map_or(vec![], |rows| rows.clone()),
            _ => unimplemented!(),
        }
    }

    // Helper to get matching row IDs based on operator and value (String)
    // Only handles Eq and NotEq for now, consistent with original logic
    fn get_matching_ids(&self, op: Operator, val: &str) -> Result<HashSet<u64>> {
        let mut matching_ids = HashSet::new();

        match op {
            Operator::Eq => {
                if let Some(ids) = self.index.get(val) {
                    matching_ids.extend(ids.iter().copied());
                }
            }
            Operator::NotEq => {
                // Combine IDs from values less than val and greater than val
                // Note: BTreeMap iterates strings lexicographically
                let range1 = self.index.range((
                    std::ops::Bound::Unbounded,
                    std::ops::Bound::Excluded(val.to_string()),
                ));
                let range2 = self.index.range((
                    std::ops::Bound::Excluded(val.to_string()),
                    std::ops::Bound::Unbounded,
                ));
                for (_, ids) in range1.chain(range2) {
                    matching_ids.extend(ids.iter().copied());
                }
            }
            // String comparisons (<, <=, >, >=) are not handled here,
            // mirroring the limited support in the original scan function.
            _ => {
                log::warn!("Unsupported operator in DepartmentIndex scan: {:?}", op);
                // Return empty set for unsupported ops
            }
        }
        Ok(matching_ids)
    }
}

impl Index for DepartmentIndex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "department_index"
    }

    fn index_schema(&self) -> SchemaRef {
        index_scan_schema(DataType::UInt64)
    }

    fn table_name(&self) -> &str {
        "employee" // Matches EmployeeTableProvider table name
    }

    fn supports_predicate(&self, predicate: &Expr) -> Result<bool> {
        let cols = predicate.column_refs();
        Ok(cols.iter().any(|col| col.name == "department"))
    }

    // Simplified scan using helper method
    fn scan(
        &self,
        predicate: &Expr,
        _projection: Option<&Vec<usize>>,
        metrics: ExecutionPlanMetricsSet,
        _partition: usize,
    ) -> Result<SendableRecordBatchStream> {
        log::debug!("Scanning DepartmentIndex with predicate: {:?}", predicate);
        let baseline_metrics = BaselineMetrics::new(&metrics, _partition);
        let schema = self.index_schema();

        let mut matching_ids = HashSet::new(); // Default empty

        // --- Predicate Parsing ---
        if let Expr::BinaryExpr(be) = predicate {
            // Check if it's Column("department") Op Literal(Utf8)
            if let Expr::Column(col) = be.left.as_ref() {
                if col.name == "department" {
                    if let Expr::Literal(lit) = be.right.as_ref() {
                        if let ScalarValue::Utf8(Some(val)) = lit {
                            // Use the helper function to get IDs
                            matching_ids = self.get_matching_ids(be.op, val)?;
                        } else {
                            log::warn!(
                                "DepartmentIndex scan predicate has non-Utf8 literal: {:?}",
                                lit
                            );
                        }
                    }
                }
            }
        } else {
            log::warn!(
                "DepartmentIndex scan predicate is not a supported BinaryExpr: {:?}",
                predicate
            );
        }
        // --- End Predicate Parsing ---

        // --- Construct RecordBatch Stream ---
        if matching_ids.is_empty() {
            return Ok(Box::pin(MemoryStream::try_new(vec![], schema, None)?));
        }

        let row_ids: Vec<u64> = matching_ids.into_iter().collect();
        let id_array = Arc::new(UInt64Array::from(row_ids));
        let output_batch = RecordBatch::try_new(schema.clone(), vec![id_array])?;
        baseline_metrics.record_output(output_batch.num_rows());

        Ok(Box::pin(MemoryStream::try_new(
            vec![output_batch],
            schema,
            None,
        )?))
        // --- End Construct RecordBatch Stream ---
    }

    fn statistics(&self) -> Statistics {
        Statistics::new_unknown(self.index_schema().as_ref()) // Use new_unknown
    }
}

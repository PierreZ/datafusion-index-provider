use std::any::Any;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, StringArray, UInt64Array}; // Added Array trait import, removed unused ArrayRef
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;

use parking_lot::RwLock;

use datafusion::logical_expr::{Expr, Operator};
use datafusion::physical_plan::memory::{LazyBatchGenerator, LazyMemoryExec};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use datafusion_common::{DataFusionError, Result, Statistics};

// index_scan_schema is at the crate root, Index trait is in physical::indexes::index
use datafusion_index_provider::index_scan_schema;
use datafusion_index_provider::physical::indexes::index::Index;

// Helper struct to provide eagerly computed batches to LazyMemoryExec
#[derive(Debug)] // Added Debug derive
struct EagerBatchProvider {
    batches: Vec<RecordBatch>,
    idx: usize,
}

impl EagerBatchProvider {
    fn new(batch: RecordBatch) -> Self {
        Self {
            batches: vec![batch],
            idx: 0,
        }
    }
}

impl std::fmt::Display for EagerBatchProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EagerBatchProvider(batches: {}, idx: {})",
            self.batches.len(),
            self.idx
        )
    }
}

impl LazyBatchGenerator for EagerBatchProvider {
    fn generate_next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.idx < self.batches.len() {
            let batch = self.batches[self.idx].clone();
            self.idx += 1;
            Ok(Some(batch))
        } else {
            Ok(None)
        }
    }
}

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

    fn extract_department_values(&self, expression: &Expr) -> Result<HashSet<u64>> {
        match expression {
            Expr::BinaryExpr(be) => {
                if let Expr::Column(col_expr) = be.left.as_ref() {
                    if col_expr.name == "department" {
                        if let Expr::Literal(scalar_val, _) = be.right.as_ref() {
                            match scalar_val {
                                ScalarValue::Utf8(Some(val_str)) => {
                                    return self.get_matching_ids(be.op, val_str);
                                }
                                _ => {
                                    return Err(DataFusionError::Plan(format!(
                                        "Unsupported literal type for department index: {:?}",
                                        scalar_val
                                    )));
                                }
                            }
                        }
                    }
                }
            }
            Expr::InList(il) => {
                if let Expr::Column(col_expr) = il.expr.as_ref() {
                    if col_expr.name == "department" && !il.negated {
                        let mut combined_ids = HashSet::new();
                        for item_expr in &il.list {
                            if let Expr::Literal(ScalarValue::Utf8(Some(val_str)), _) = item_expr {
                                let ids_for_val = self.get_matching_ids(Operator::Eq, val_str)?;
                                combined_ids.extend(ids_for_val);
                            } else {
                                return Err(DataFusionError::Plan(format!(
                                    "Unsupported literal type in IN list for DepartmentIndex: {:?}",
                                    item_expr
                                )));
                            }
                        }
                        return Ok(combined_ids);
                    }
                }
            }
            _ => {} // Fall through to unsupported error
        }
        Err(DataFusionError::Plan(format!(
            "Unsupported expression for DepartmentIndex scan: {:?}. Only 'department = <literal>' or 'department IN (<literals>)' are supported.",
            expression
        )))
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

    fn scan_index(&self, expr: &Vec<Expr>, limit: Option<usize>) -> Result<Arc<dyn ExecutionPlan>> {
        let mut intersected_ids: Option<HashSet<u64>> = None;

        if expr.is_empty() {
            // If no expressions are provided, this implies scanning all entries matched by this index type (if any),
            // or more likely, it means no suitable filters were pushed down for this specific index scan.
            // For a filtered scan, returning an empty set is safest if no filters are given.
            intersected_ids = Some(HashSet::new());
        } else {
            for expression_filter in expr {
                let ids_for_current_expr = self.extract_department_values(expression_filter)?;
                if let Some(current_intersected_ids) = intersected_ids.as_mut() {
                    *current_intersected_ids = current_intersected_ids
                        .intersection(&ids_for_current_expr)
                        .cloned()
                        .collect();
                    // Optimization: if intersection is empty, no need to process further
                    if current_intersected_ids.is_empty() {
                        break;
                    }
                } else {
                    intersected_ids = Some(ids_for_current_expr);
                }
            }
        }

        let final_row_ids_set = intersected_ids.unwrap_or_default();
        let mut row_ids_vec: Vec<u64> = final_row_ids_set.into_iter().collect();

        row_ids_vec.sort_unstable();

        if let Some(l) = limit {
            row_ids_vec.truncate(l);
        }

        let schema = self.index_schema(); // From Index trait
        let row_id_array = Arc::new(UInt64Array::from(row_ids_vec)) as Arc<dyn Array>;
        let batch = RecordBatch::try_new(schema.clone(), vec![row_id_array])?;

        let eager_provider = EagerBatchProvider::new(batch);
        let lazy_generator: Arc<RwLock<dyn LazyBatchGenerator>> =
            Arc::new(RwLock::new(eager_provider));

        Ok(Arc::new(LazyMemoryExec::try_new(
            schema,
            vec![lazy_generator],
        )?))
    }

    fn statistics(&self) -> Statistics {
        Statistics::new_unknown(self.index_schema().as_ref()) // Use new_unknown
    }
}

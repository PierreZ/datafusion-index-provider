use std::any::Any;
use std::collections::{BTreeMap, HashSet};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;

use arrow::array::{Array, Int32Array, UInt64Array};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;

use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::logical_expr::{BinaryExpr, Operator}; // Expr is imported via prelude
use datafusion::physical_plan::memory::{LazyBatchGenerator, LazyMemoryExec};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;

use log;
use parking_lot::RwLock;

use datafusion_index_provider::index_scan_schema;
use datafusion_index_provider::physical::indexes::index::Index;

// Helper struct to provide eagerly computed batches to LazyMemoryExec
#[derive(Debug)]
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

/// A simple index structure that maps age values to row indices
#[derive(Debug, Clone)]
pub struct AgeIndex {
    // Maps age to a vector of row IDs (u64)
    index: BTreeMap<i32, Vec<u64>>,
}

impl AgeIndex {
    pub fn new(ages: &Int32Array) -> Self {
        let mut index: BTreeMap<_, Vec<u64>> = BTreeMap::new();
        for i in 0..ages.len() {
            let row_id = i as u64; // Assuming row ID is the array index
            if ages.is_valid(i) {
                let age = ages.value(i);
                index.entry(age).or_default().push(row_id);
            }
        }
        AgeIndex { index }
    }

    // Helper to get matching row IDs based on operator and value
    fn get_matching_ids(&self, op: Operator, val: i32) -> Result<HashSet<u64>> {
        let mut matching_ids = HashSet::new();

        match op {
            Operator::Eq => {
                if let Some(ids) = self.index.get(&val) {
                    matching_ids.extend(ids.iter().copied());
                }
            }
            Operator::NotEq => {
                for (key, ids) in self.index.iter() {
                    if *key != val {
                        matching_ids.extend(ids.iter().copied());
                    }
                }
            }
            Operator::Lt => {
                for (_, ids) in self.index.range((Unbounded, Excluded(&val))) {
                    matching_ids.extend(ids.iter().copied());
                }
            }
            Operator::LtEq => {
                for (_, ids) in self.index.range((Unbounded, Included(&val))) {
                    matching_ids.extend(ids.iter().copied());
                }
            }
            Operator::Gt => {
                for (_, ids) in self.index.range((Excluded(&val), Unbounded)) {
                    matching_ids.extend(ids.iter().copied());
                }
            }
            Operator::GtEq => {
                for (_, ids) in self.index.range((Included(&val), Unbounded)) {
                    matching_ids.extend(ids.iter().copied());
                }
            }
            _ => {
                unimplemented!("Unsupported operator for AgeIndex: {:?} ", op)
            }
        }
        Ok(matching_ids)
    }

    // Helper to extract age values from filter expressions
    fn extract_age_values(&self, filters: &[Expr]) -> Result<HashSet<u64>> {
        let mut intersected_ids: Option<HashSet<u64>> = None;
        let mut processed_age_filter = false;

        for filter_expr in filters {
            let mut current_expr_ids = HashSet::new();
            let mut filter_on_age = false;

            match filter_expr {
                Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                    match left.as_ref() {
                        Expr::Column(c) => {
                            if c.name == "age" {
                                filter_on_age = true;
                                match op {
                                    Operator::Eq
                                    | Operator::NotEq
                                    | Operator::Lt
                                    | Operator::LtEq
                                    | Operator::Gt
                                    | Operator::GtEq => {
                                        if let Expr::Literal(ScalarValue::Int32(Some(val)), _) =
                                            right.as_ref()
                                        {
                                            current_expr_ids = self.get_matching_ids(*op, *val)?;
                                        } else {
                                            log::warn!("AgeIndex: Right side of binary expression for age is not an Int32 literal: {:?}", right);
                                            return Err(DataFusionError::Plan(format!(
                                                "AgeIndex: Unsupported literal type for age: {:?}",
                                                right
                                            )));
                                        }
                                    }
                                    _ => {
                                        log::warn!(
                                            "AgeIndex: Unsupported operator for age column: {:?}",
                                            op
                                        );
                                        return Err(DataFusionError::Plan(format!(
                                            "AgeIndex: Unsupported operator for age: {:?}",
                                            op
                                        )));
                                    }
                                }
                            }
                        }
                        _ => {
                            log::debug!("AgeIndex: Binary expression left side is not a simple column: {:?}", left);
                        }
                    }
                }
                _ => {
                    // Potentially complex filter not directly handled, or not on 'age'
                    // If it contains 'age' indirectly, expr_to_columns might be needed, but we keep it simple for now.
                    log::debug!(
                        "AgeIndex: Skipping unsupported filter expression type: {:?}",
                        filter_expr
                    );
                }
            }

            if filter_on_age {
                processed_age_filter = true;
                if let Some(existing_ids) = intersected_ids.as_mut() {
                    existing_ids.retain(|id| current_expr_ids.contains(id));
                } else {
                    intersected_ids = Some(current_expr_ids);
                }
            }
        }
        // If no filters were processed for the 'age' column, return all ids.
        // Otherwise, return the intersected set (which could be empty if filters are restrictive or no data matches).
        if !processed_age_filter {
            Ok(self.index.values().flatten().copied().collect())
        } else {
            Ok(intersected_ids.unwrap_or_default())
        }
    }
}

impl Index for AgeIndex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "age_index"
    }

    fn index_schema(&self) -> SchemaRef {
        index_scan_schema(DataType::UInt64)
    }

    fn table_name(&self) -> &str {
        "employee" // Matches EmployeeTableProvider table name
    }

    fn supports_predicate(&self, predicate: &Expr) -> Result<bool> {
        let cols = predicate.column_refs();
        Ok(cols.iter().any(|col| col.name == "age"))
    }

    // TODO: Use LazyMemoryExec
    fn scan_index(
        &self,
        predicate: &Vec<Expr>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let final_row_ids_set = self.extract_age_values(predicate)?;
        let mut all_row_ids: Vec<u64> = final_row_ids_set.into_iter().collect();

        // Sort row IDs for consistent output and potentially better join performance later
        all_row_ids.sort_unstable();

        // Apply limit if provided
        if let Some(l) = limit {
            all_row_ids.truncate(l);
        }

        let schema = index_scan_schema(DataType::UInt64);

        let batch = if all_row_ids.is_empty() {
            RecordBatch::new_empty(schema.clone())
        } else {
            let row_id_array = Arc::new(UInt64Array::from(all_row_ids)) as Arc<dyn Array>;
            RecordBatch::try_new(schema.clone(), vec![row_id_array])?
        };

        // Create the EagerBatchProvider and LazyMemoryExec
        let batch_provider = EagerBatchProvider::new(batch);
        let exec_plan = LazyMemoryExec::try_new(
            schema, // Output schema of the scan (just the row_id column)
            vec![Arc::new(RwLock::new(batch_provider))], // Vector of batch generators
        )?;

        Ok(Arc::new(exec_plan))
    }

    fn statistics(&self) -> Statistics {
        Statistics::new_unknown(self.index_schema().as_ref()) // Use new_unknown
    }
}

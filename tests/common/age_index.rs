use std::any::Any;
use std::collections::{BTreeMap, HashSet};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;

use arrow::array::{Array, Int32Array, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, Operator};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{SendableRecordBatchStream, Statistics};
use datafusion::scalar::ScalarValue;

use datafusion_index_provider::physical::indexes::index::{Index, ROW_ID_COLUMN_NAME};

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

    #[allow(dead_code)] // Keep in case compiler flags as unused
    pub fn filter_rows(&self, op: &Operator, value: i32) -> Vec<u64> {
        match op {
            Operator::Gt => self
                .index
                .range((value + 1)..)
                .flat_map(|(_, rows)| rows)
                .copied()
                .collect(),
            Operator::GtEq => self
                .index
                .range(value..)
                .flat_map(|(_, rows)| rows)
                .copied()
                .collect(),
            Operator::Lt => self
                .index
                .range(..value)
                .flat_map(|(_, rows)| rows)
                .copied()
                .collect(),
            Operator::LtEq => self
                .index
                .range(..=value)
                .flat_map(|(_, rows)| rows)
                .copied()
                .collect(),
            Operator::Eq => self.index.get(&value).map_or(vec![], |rows| rows.clone()),
            _ => unimplemented!(),
        }
    }

    #[allow(dead_code)] // Keep in case compiler flags as unused
    pub fn filter_rows_range(&self, low: i32, high: i32) -> Vec<u64> {
        self.index
            .range(low..=high)
            .flat_map(|(_, rows)| rows)
            .copied()
            .collect()
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
                // Combine IDs from values less than val and greater than val
                let range1 = self.index.range((Unbounded, Excluded(&val)));
                let range2 = self.index.range((Excluded(&val), Unbounded));
                for (_, ids) in range1.chain(range2) {
                    matching_ids.extend(ids.iter().copied());
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
                log::warn!("Unsupported operator in AgeIndex scan: {:?}", op);
                // Return empty set for unsupported ops
            }
        }
        Ok(matching_ids)
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
        Arc::new(Schema::new(vec![Field::new(
            ROW_ID_COLUMN_NAME,
            DataType::UInt64,
            false,
        )]))
    }

    fn table_name(&self) -> &str {
        "employee" // Matches EmployeeTableProvider table name
    }

    fn supports_predicate(&self, predicate: &Expr) -> Result<bool> {
        let cols = predicate.column_refs();
        Ok(cols.iter().any(|col| col.name == "age"))
    }

    // Simplified scan using helper method
    fn scan(
        &self,
        predicate: &Expr,
        _projection: Option<&Vec<usize>>,
    ) -> Result<SendableRecordBatchStream> {
        log::debug!("Scanning AgeIndex with predicate: {:?}", predicate);
        let schema = self.index_schema();

        let mut matching_ids = HashSet::new(); // Default empty

        // --- Predicate Parsing ---
        if let Expr::BinaryExpr(be) = predicate {
            let (col_expr, op, lit_expr) = match (be.left.as_ref(), be.op, be.right.as_ref()) {
                // Case 1: Column Op Literal
                (Expr::Column(col), op, Expr::Literal(lit)) if col.name == "age" => (col, op, lit),
                // Case 2: Literal Op Column
                (Expr::Literal(lit), op, Expr::Column(col)) if col.name == "age" => {
                    // Swap operator for column comparison
                    (col, op.swap().unwrap_or(op), lit)
                }
                // Other cases not supported for this index
                _ => {
                    log::warn!(
                        "AgeIndex scan predicate structure not supported: {:?}",
                        predicate
                    );
                    (
                        // Return dummy values to avoid further processing
                        &datafusion_common::Column {
                            relation: None,
                            name: "".to_string(),
                            spans: Default::default(), // Fix: Add missing spans field
                        }, // Dummy column
                        Operator::Eq,              // Dummy Op
                        &ScalarValue::Int32(None), // Dummy Literal
                    )
                }
            };

            // Proceed only if the column name was 'age' (avoid processing dummy values)
            if col_expr.name == "age" {
                if let ScalarValue::Int32(Some(val)) = lit_expr {
                    // Use the helper function to get IDs with potentially swapped operator
                    matching_ids = self.get_matching_ids(op, *val)?;
                } else {
                    log::warn!(
                        "AgeIndex scan predicate has non-Int32 literal: {:?}",
                        lit_expr
                    );
                }
            }
        } else {
            // Log if predicate is not a simple binary expression we handle
            log::warn!(
                "AgeIndex scan predicate is not a supported BinaryExpr: {:?}",
                predicate
            );
            // Could potentially try to evaluate more complex expressions or return all ids,
            // but for now, we return empty results if the predicate isn't simple.
        }
        // --- End Predicate Parsing ---

        // --- Construct RecordBatch Stream ---
        if matching_ids.is_empty() {
            // Return empty stream if no matches or predicate not supported
            return Ok(Box::pin(MemoryStream::try_new(vec![], schema, None)?));
        }

        // Convert HashSet<u64> to UInt64Array
        let row_ids: Vec<u64> = matching_ids.into_iter().collect();
        let id_array = Arc::new(UInt64Array::from(row_ids));

        // Create RecordBatch with the row IDs
        let batch = RecordBatch::try_new(schema.clone(), vec![id_array])?;

        // Return stream with the single batch
        Ok(Box::pin(MemoryStream::try_new(vec![batch], schema, None)?))
        // --- End Construct RecordBatch Stream ---
    }

    fn statistics(&self) -> Statistics {
        Statistics::new_unknown(self.index_schema().as_ref()) // Use new_unknown
    }
}

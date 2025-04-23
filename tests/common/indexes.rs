use std::any::Any;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, Int32Array, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, Operator};
use datafusion::physical_plan::memory::{LazyBatchGenerator, MemoryStream};
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

    pub fn filter_rows_range(&self, low: i32, high: i32) -> Vec<u64> {
        self.index
            .range(low..=high)
            .flat_map(|(_, rows)| rows)
            .copied()
            .collect()
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
        "employee"
    }

    fn supports_predicate(&self, predicate: &Expr) -> Result<bool> {
        let cols = predicate.column_refs();
        Ok(cols.iter().any(|col| col.name == "age"))
    }

    fn scan(
        &self,
        predicate: &Expr,
        _projection: Option<&Vec<usize>>,
    ) -> Result<SendableRecordBatchStream> {
        log::debug!("Scanning AgeIndex with predicate: {:?}", predicate);
        let schema = self.index_schema();

        let mut matching_ids = HashSet::new();

        if let Expr::BinaryExpr(be) = predicate {
            if let Expr::Column(col) = be.left.as_ref() {
                if col.name == "age" {
                    if let Expr::Literal(lit) = be.right.as_ref() {
                        if let ScalarValue::Int32(Some(val)) = lit {
                            match be.op {
                                Operator::Eq => {
                                    if let Some(ids) = self.index.get(val) {
                                        matching_ids.extend(ids.iter().copied());
                                    }
                                }
                                Operator::NotEq => {
                                    for (age, ids) in self.index.iter() {
                                        if age != val {
                                            matching_ids.extend(ids.iter().copied());
                                        }
                                    }
                                }
                                Operator::Lt => {
                                    for (age, ids) in self.index.iter() {
                                        if age < val {
                                            matching_ids.extend(ids.iter().copied());
                                        }
                                    }
                                }
                                Operator::LtEq => {
                                    for (age, ids) in self.index.iter() {
                                        if age <= val {
                                            matching_ids.extend(ids.iter().copied());
                                        }
                                    }
                                }
                                Operator::Gt => {
                                    for (age, ids) in self.index.iter() {
                                        if age > val {
                                            matching_ids.extend(ids.iter().copied());
                                        }
                                    }
                                }
                                Operator::GtEq => {
                                    for (age, ids) in self.index.iter() {
                                        if age >= val {
                                            matching_ids.extend(ids.iter().copied());
                                        }
                                    }
                                }
                                _ => {
                                    log::warn!(
                                        "Unsupported operator in AgeIndex scan: {:?}",
                                        be.op
                                    );
                                    return Ok(Box::pin(MemoryStream::try_new(
                                        vec![],
                                        schema,
                                        None,
                                    )?));
                                }
                            }
                        } else {
                            log::warn!("AgeIndex scan predicate has non-Int32 literal: {:?}", lit);
                            return Ok(Box::pin(MemoryStream::try_new(vec![], schema, None)?));
                        }
                    }
                }
            }
        }

        let row_ids: Vec<u64> = matching_ids.into_iter().collect();
        if row_ids.is_empty() {
            return Ok(Box::pin(MemoryStream::try_new(vec![], schema, None)?));
        }
        let id_array = Arc::new(UInt64Array::from(row_ids));
        let batch = RecordBatch::try_new(schema.clone(), vec![id_array])?;

        Ok(Box::pin(MemoryStream::try_new(vec![batch], schema, None)?))
    }

    fn statistics(&self) -> Statistics {
        Statistics::new_unknown(self.index_schema().as_ref()) // Use new_unknown
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

    pub fn filter_rows(&self, op: &Operator, value: &str) -> Vec<u64> {
        match op {
            Operator::Eq => self.index.get(value).map_or(vec![], |rows| rows.clone()),
            _ => unimplemented!(),
        }
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
        Arc::new(Schema::new(vec![Field::new(
            ROW_ID_COLUMN_NAME,
            DataType::UInt64,
            false,
        )]))
    }

    fn table_name(&self) -> &str {
        "employee"
    }

    fn supports_predicate(&self, predicate: &Expr) -> Result<bool> {
        let cols = predicate.column_refs();
        Ok(cols.iter().any(|col| col.name == "department"))
    }

    fn scan(
        &self,
        predicate: &Expr,
        _projection: Option<&Vec<usize>>,
    ) -> Result<SendableRecordBatchStream> {
        log::debug!("Scanning DepartmentIndex with predicate: {:?}", predicate);
        let schema = self.index_schema();

        let mut matching_ids = HashSet::new();

        if let Expr::BinaryExpr(be) = predicate {
            if let Expr::Column(col) = be.left.as_ref() {
                if col.name == "department" {
                    if let Expr::Literal(lit) = be.right.as_ref() {
                        if let ScalarValue::Utf8(Some(val)) = lit {
                            match be.op {
                                Operator::Eq => {
                                    if let Some(ids) = self.index.get(val) {
                                        matching_ids.extend(ids.iter().copied());
                                    }
                                }
                                Operator::NotEq => {
                                    for (dept, ids) in self.index.iter() {
                                        if dept != val {
                                            matching_ids.extend(ids.iter().copied());
                                        }
                                    }
                                }
                                _ => {
                                    log::warn!(
                                        "Unsupported operator in DepartmentIndex scan: {:?}",
                                        be.op
                                    );
                                    return Ok(Box::pin(MemoryStream::try_new(
                                        vec![],
                                        schema,
                                        None,
                                    )?));
                                }
                            }
                        } else {
                            log::warn!(
                                "DepartmentIndex scan predicate has non-Utf8 literal: {:?}",
                                lit
                            );
                            return Ok(Box::pin(MemoryStream::try_new(vec![], schema, None)?));
                        }
                    }
                }
            }
        }

        let row_ids: Vec<u64> = matching_ids.into_iter().collect();
        if row_ids.is_empty() {
            return Ok(Box::pin(MemoryStream::try_new(vec![], schema, None)?));
        }
        let id_array = Arc::new(UInt64Array::from(row_ids));
        let batch = RecordBatch::try_new(schema.clone(), vec![id_array])?;

        Ok(Box::pin(MemoryStream::try_new(vec![batch], schema, None)?))
    }

    fn statistics(&self) -> Statistics {
        Statistics::new_unknown(self.index_schema().as_ref()) // Use new_unknown
    }
}

#[derive(Debug)]
pub struct NotSoLazyBatchGenerator {
    pub data: Vec<RecordBatch>,
}

impl std::fmt::Display for NotSoLazyBatchGenerator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NotSoLazyBatchGenerator")
    }
}

impl LazyBatchGenerator for NotSoLazyBatchGenerator {
    fn generate_next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.data.pop())
    }
}

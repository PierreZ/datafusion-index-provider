use std::collections::BTreeMap;

use arrow::array::{Int32Array, RecordBatch, StringArray};
use datafusion::error::Result;
use datafusion::{logical_expr::Operator, physical_plan::memory::LazyBatchGenerator};

/// A simple index structure that maps age values to row indices
#[derive(Debug)]
pub struct AgeIndex {
    // Maps age to a vector of row indices
    index: BTreeMap<i32, Vec<usize>>,
}

impl AgeIndex {
    pub fn new(ages: &Int32Array) -> Self {
        let mut index = BTreeMap::new();
        for (i, age) in ages.iter().enumerate() {
            if let Some(age) = age {
                index.entry(age).or_insert_with(Vec::new).push(i);
            }
        }
        AgeIndex { index }
    }

    pub fn filter_rows(&self, op: &Operator, value: i32) -> Vec<usize> {
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

    pub fn filter_rows_range(&self, low: i32, high: i32) -> Vec<usize> {
        self.index
            .range(low..=high)
            .flat_map(|(_, rows)| rows)
            .copied()
            .collect()
    }
}

/// A simple index structure that maps department values to row indices
#[derive(Debug)]
pub struct DepartmentIndex {
    // Maps department to a vector of row indices
    index: BTreeMap<String, Vec<usize>>,
}

impl DepartmentIndex {
    pub fn new(departments: &StringArray) -> Self {
        let mut index = BTreeMap::new();
        for (i, department) in departments.iter().enumerate() {
            if let Some(department) = department {
                index
                    .entry(department.to_string())
                    .or_insert_with(Vec::new)
                    .push(i);
            }
        }
        DepartmentIndex { index }
    }

    pub fn filter_rows(&self, op: &Operator, value: &str) -> Vec<usize> {
        match op {
            Operator::Eq => self.index.get(value).map_or(vec![], |rows| rows.clone()),
            _ => unimplemented!(),
        }
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

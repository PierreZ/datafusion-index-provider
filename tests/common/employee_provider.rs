use std::collections::HashSet;
use std::{any::Any, sync::Arc};

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::{empty::EmptyExec, ExecutionPlan};
use datafusion_index_provider::physical::indexes::index::Index;
use datafusion_index_provider::provider::IndexedTableProvider;

use crate::common::age_index::AgeIndex;
use crate::common::department_index::DepartmentIndex;
use crate::common::exec::IndexJoinExec;

/// A simple in-memory table provider that stores employee data with an age index
#[derive(Debug)]
pub struct EmployeeTableProvider {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    _age_index: Arc<AgeIndex>,
    _department_index: Arc<DepartmentIndex>,
    indexes: Vec<Arc<dyn Index>>,
}

#[async_trait]
impl IndexedTableProvider for EmployeeTableProvider {
    fn indexes(&self) -> Result<Vec<Arc<dyn Index>>> {
        Ok(self.indexes.clone())
    }

    fn get_indexed_columns_names(&self) -> Result<HashSet<String>> {
        let mut columns = HashSet::new();
        columns.insert("age".to_string());
        columns.insert("department".to_string());
        Ok(columns)
    }
}

impl Default for EmployeeTableProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl EmployeeTableProvider {
    pub fn new() -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
            Field::new("department", DataType::Utf8, false),
        ]));

        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);
        let age_array = Int32Array::from(vec![25, 30, 35, 28, 32]);
        let department_array = StringArray::from(vec![
            "Engineering",
            "Sales",
            "Marketing",
            "Engineering",
            "Sales",
        ]);

        let age_index = Arc::new(AgeIndex::new(&age_array));

        let department_index = Arc::new(DepartmentIndex::new(&department_array));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(age_array.clone()),
                Arc::new(department_array.clone()),
            ],
        )
        .unwrap();

        EmployeeTableProvider {
            schema,
            batches: vec![batch],
            _age_index: age_index.clone(),
            _department_index: department_index.clone(),
            indexes: vec![
                age_index as Arc<dyn Index>,
                department_index as Arc<dyn Index>,
            ],
        }
    }
}

#[async_trait]
impl TableProvider for EmployeeTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        log::debug!(
            "EmployeeTableProvider::scan. projection: {:?}, filters: {:?}, limit: {:?}",
            projection,
            filters,
            limit
        );

        if filters.is_empty() {
            return Ok(Arc::new(EmptyExec::new(self.schema.clone())));
        } else {
            let index_input = self
                .create_execution_plan_with_indexes(state, projection, filters, limit)
                .await?;

            Ok(Arc::new(IndexJoinExec::new(
                index_input,
                self.batches.clone(),
                projection.cloned(),
                self.schema.clone(),
            )))
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let indexed_columns = self.get_indexed_columns_names()?;
        let results = filters
            .iter()
            .map(|filter| {
                let cols = filter.column_refs();
                if cols.is_empty() {
                    return TableProviderFilterPushDown::Unsupported;
                }
                let all_indexed = cols.iter().all(|c| indexed_columns.contains(&c.name));
                if all_indexed {
                    match self.find_suitable_indexes(filter) {
                        Ok(indexes) if !indexes.is_empty() => TableProviderFilterPushDown::Exact,
                        _ => TableProviderFilterPushDown::Unsupported,
                    }
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect();
        Ok(results)
    }
}

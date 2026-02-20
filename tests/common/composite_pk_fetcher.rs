use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, Int32Array, StringArray, UInt64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion_index_provider::physical_plan::fetcher::RecordFetcher;

pub struct CompositePkFetcher {
    data: RecordBatch,
}

impl CompositePkFetcher {
    pub fn new(data: RecordBatch) -> Self {
        Self { data }
    }
}

impl fmt::Debug for CompositePkFetcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CompositePkFetcher")
    }
}

#[async_trait]
impl RecordFetcher for CompositePkFetcher {
    fn schema(&self) -> SchemaRef {
        self.data.schema()
    }

    async fn fetch(&self, index_batch: RecordBatch) -> Result<RecordBatch> {
        let req_tenants = index_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let req_eids = index_batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let data_tenants = self
            .data
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let data_eids = self
            .data
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let mut indices = Vec::new();
        for i in 0..req_tenants.len() {
            let req_t = req_tenants.value(i);
            let req_e = req_eids.value(i);

            for j in 0..data_tenants.len() {
                if data_tenants.value(j) == req_t && data_eids.value(j) == req_e {
                    indices.push(j as i32);
                    break;
                }
            }
        }

        let indices_array = Int32Array::from(indices);
        let new_columns: Result<Vec<Arc<dyn Array>>> = self
            .data
            .columns()
            .iter()
            .map(|col| {
                Ok(Arc::new(datafusion::arrow::compute::take(
                    col.as_ref(),
                    &indices_array,
                    None,
                )?) as Arc<dyn Array>)
            })
            .collect();

        Ok(RecordBatch::try_new(self.data.schema(), new_columns?)?)
    }
}

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::logical_expr::{Between, Operator};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use datafusion_common::{Column, Result, Spans};
use futures_core::Stream;
use futures_util::{FutureExt, StreamExt};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Represents information about an indexed column
#[derive(Debug, Clone)]
pub struct IndexedColumn {
    /// Name of the column that is indexed
    pub name: String,
    /// List of supported operators for this index
    pub supported_operators: Vec<Operator>,
}

/// A provider that supports indexed column lookups
#[async_trait]
pub trait IndexProvider: TableProvider {
    /// Returns a map of column names to their index information
    fn get_indexed_columns(&self) -> HashMap<String, IndexedColumn>;

    /// Returns whether a specific column and operator combination is supported by the index
    fn supports_index_operator(&self, column: &str, op: &Operator) -> bool {
        self.get_indexed_columns()
            .get(column)
            .map(|idx| idx.supported_operators.contains(op))
            .unwrap_or(false)
    }

    /// Optimizes a list of expressions by combining them when possible
    /// For example, multiple expressions on the same column could be combined into a single expression
    /// Returns a new list of optimized expressions
    fn optimize_exprs(&self, exprs: &[Expr]) -> Result<Vec<Expr>> {
        // Default implementation returns expressions as-is
        Ok(exprs.to_vec())
    }

    /// Creates an IndexLookupExec for the given filter expression
    /// Returns None if the filter cannot use an index
    fn create_index_lookup(&self, expr: &Expr) -> Result<Option<Arc<dyn ExecutionPlan>>>;

    /// Creates an execution plan that combines multiple index lookups
    /// Default implementation uses HashJoinExec when multiple indexes are used
    fn create_index_join(
        &self,
        lookups: Vec<Arc<dyn ExecutionPlan>>,
        projection: Option<&Vec<usize>>,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

/// Helper function to try combining multiple expressions on a column into a BETWEEN expression
/// Returns None if the expressions cannot be combined
pub fn try_combine_exprs_to_between(exprs: &[&Expr], column_name: &str) -> Option<Vec<Expr>> {
    let mut lower_bound = None;
    let mut upper_bound = None;
    let mut other_exprs = Vec::new();
    let mut relation_name = None;

    // First extract the relation name from any of the expressions
    for expr in exprs {
        if let Expr::BinaryExpr(binary) = expr {
            if let (Expr::Column(col), _) = (&*binary.left, &*binary.right) {
                if col.name == column_name {
                    relation_name = col.relation.clone();
                    break;
                }
            }
        }
    }

    for expr in exprs {
        if let Expr::BinaryExpr(binary) = expr {
            if let (Expr::Column(col), Expr::Literal(value)) = (&*binary.left, &*binary.right) {
                if col.name == column_name {
                    if let ScalarValue::Int32(Some(val)) = value {
                        match binary.op {
                            Operator::GtEq | Operator::Gt => {
                                // Take the highest lower bound
                                let val = if binary.op == Operator::Gt {
                                    val + 1
                                } else {
                                    *val
                                };
                                match lower_bound {
                                    None => lower_bound = Some(val),
                                    Some(current) if val > current => lower_bound = Some(val),
                                    _ => {}
                                }
                            }
                            Operator::LtEq | Operator::Lt => {
                                // Take the lowest upper bound
                                let val = if binary.op == Operator::Lt {
                                    val - 1
                                } else {
                                    *val
                                };
                                match upper_bound {
                                    None => upper_bound = Some(val),
                                    Some(current) if val < current => upper_bound = Some(val),
                                    _ => {}
                                }
                            }
                            _ => other_exprs.push((*expr).clone()),
                        }
                    }
                }
            }
        }
    }

    // If we have both bounds, create the expressions
    if let (Some(low), Some(high)) = (lower_bound, upper_bound) {
        let mut optimized = other_exprs;

        // Create BETWEEN low AND high
        optimized.push(Expr::Between(Between {
            negated: false,
            expr: Box::new(Expr::Column(Column {
                relation: relation_name.clone(),
                name: column_name.to_string(),
                spans: Spans::new(),
            })),
            // TODO: use the original scalarvalue instead of knowing it is a Int32
            low: Box::new(Expr::Literal(ScalarValue::Int32(Some(low)))),
            high: Box::new(Expr::Literal(ScalarValue::Int32(Some(high)))),
        }));

        log::debug!("Optimized expressions: {:?}", optimized);

        Some(optimized)
    } else {
        None
    }
}

#[async_trait]
pub trait MapIndexWithRecord: Send + Sync {
    async fn map_index_with_record(&mut self, index_batch: RecordBatch) -> Result<RecordBatch>;
}

pub struct ScanWithIndexStream {
    /// Input execution plan
    input: Option<SendableRecordBatchStream>,
    /// Copy of the data schema
    data_schema: SchemaRef,
    /// Execution time metrics
    baseline_metrics: BaselineMetrics,
    /// Mapper used to convert an index record entry to an Record entry
    mapper: Box<dyn MapIndexWithRecord>,
}

impl ScanWithIndexStream {
    pub fn new(
        input: SendableRecordBatchStream,
        partition: usize,
        mapper: Box<dyn MapIndexWithRecord>,
    ) -> Self {
        let metriccs = ExecutionPlanMetricsSet::new();
        let baseline_metrics = BaselineMetrics::new(&metriccs, partition);
        let schema = input.schema();
        Self {
            input: Some(input),
            data_schema: schema,
            baseline_metrics,
            mapper,
        }
    }
}

impl Stream for ScanWithIndexStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = match &mut self.input {
            // input has been cleared
            None => Poll::Ready(None),
            Some(input) => match input.poll_next_unpin(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
                Poll::Ready(Some(Ok(record_batch))) => {
                    match self
                        .mapper
                        .map_index_with_record(record_batch)
                        .poll_unpin(cx)
                    {
                        Poll::Ready(record) => Poll::Ready(Some(record)),
                        Poll::Pending => Poll::Pending,
                    }
                }
            },
        };
        self.baseline_metrics.record_poll(poll)
    }
}

impl RecordBatchStream for ScanWithIndexStream {
    fn schema(&self) -> SchemaRef {
        self.data_schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, UInt8Array, UInt8Builder};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::memory::MemoryStream;
    use std::collections::HashMap;
    use std::sync::Arc;

    struct Mapper {
        database: HashMap<u8, u8>,
    }

    impl Mapper {
        pub fn new_and_init(database: HashMap<u8, u8>) -> Self {
            Self { database }
        }
    }

    #[async_trait]
    impl MapIndexWithRecord for Mapper {
        async fn map_index_with_record(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
            let column = batch.column_by_name("id").expect("should find column id");
            let index_entry = column.as_any().downcast_ref::<UInt8Array>().unwrap();
            let index_entry: Vec<u8> = index_entry.iter().flatten().collect();

            let mut ids = Vec::with_capacity(batch.num_rows());
            let mut records = Vec::with_capacity(batch.num_rows());

            for index in index_entry {
                match self.database.get(&index) {
                    None => (),
                    Some(value) => {
                        ids.push(index);
                        records.push(*value);
                    }
                }
            }

            let schema = Schema::new(vec![
                Field::new("id", DataType::UInt8, false),
                Field::new("account_balance", DataType::UInt8, false),
            ]);

            let batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![
                    Arc::new(UInt8Array::from(ids)),
                    Arc::new(UInt8Array::from(records)),
                ],
            )?;

            Ok(batch)
        }
    }

    fn create_index_memory_stream(ids: Vec<u8>) -> Result<MemoryStream> {
        let schema = Schema::new(vec![Field::new("id", DataType::UInt8, false)]);

        let mut id_array = UInt8Builder::with_capacity(ids.len());
        for id in ids {
            id_array.append_value(id);
        }

        MemoryStream::try_new(
            vec![RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![Arc::new(id_array.finish())],
            )?],
            Arc::new(schema),
            None,
        )
    }

    #[tokio::test]
    async fn single_index() {
        let stream = Box::pin(create_index_memory_stream(vec![1, 2]).unwrap());

        let mut mapper = ScanWithIndexStream::new(
            stream,
            0,
            Box::new(Mapper::new_and_init(HashMap::from([(1, 42), (2, 43)]))),
        );

        let records = mapper
            .next()
            .await
            .expect("Should be allowed to poll on first try")
            .expect("should contain a RecordBatch");
        assert_eq!(2, records.num_rows(), "Bad number of rows");

        let column = records
            .column_by_name("account_balance")
            .expect("cannot retrieve column 'account_balance'");
        let data = column
            .as_any()
            .downcast_ref::<UInt8Array>()
            .expect("could to cast to u8");
        let projected_data: Vec<u8> = data.iter().flatten().collect();

        assert_eq!(
            42,
            projected_data.first().expect("should find index 0").clone()
        );
        assert_eq!(
            43,
            projected_data.get(1).expect("should find index 1").clone()
        );
    }
}

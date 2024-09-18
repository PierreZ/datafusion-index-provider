use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::JoinType;
use std::sync::Arc;

fn build_index_table(a: (&str, &Vec<i32>), b: (&str, &Vec<i32>)) -> Arc<dyn ExecutionPlan> {
    let batch = build_idx_i32(a, b);
    let schema = batch.schema();
    Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
}

pub fn build_idx_i32(a: (&str, &Vec<i32>), b: (&str, &Vec<i32>)) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new(a.0, DataType::Int32, false),
        Field::new(b.0, DataType::Int32, false),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(a.1.clone())),
            Arc::new(Int32Array::from(b.1.clone())),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn index_join() {
    let left = build_index_table(("idx1", &vec![42, 43, 44]), ("pk1", &vec![3, 4, 5]));
    let right = build_index_table(("idx2", &vec![40, 41, 42]), ("pk2", &vec![1, 2, 3]));

    let on = vec![(
        Arc::new(Column::new_with_schema("pk1", &left.schema()).unwrap()) as _,
        Arc::new(Column::new_with_schema("pk2", &right.schema()).unwrap()) as _,
    )];

    let join_exec = HashJoinExec::try_new(
        Arc::clone(&left),
        Arc::clone(&right),
        on,
        None,
        &JoinType::Inner,
        Some(vec![1]), // Projection to pk1, to avoid having (idx1, pk1, idx2, pk2)
        PartitionMode::CollectLeft,
        false,
    )
    .unwrap();

    let ctx = SessionContext::new();

    let stream = join_exec.execute(0, ctx.task_ctx()).unwrap();
    let batches: Vec<RecordBatch> = collect(stream).await.unwrap();
    assert_eq!(1, batches.len());

    let batch = batches.first().unwrap();
    assert_eq!(1, batch.num_columns());
    assert_eq!(1, batch.num_rows());

    let column = batch
        .column_by_name("pk1")
        .expect("cannot retrieve column 'pk1'");
    let data = column
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("could to cast to Int32");
    let projected_data: Vec<i32> = data.iter().flatten().collect();
    assert_eq!(1, projected_data.len());
    assert_eq!(3, *projected_data.first().unwrap());
}

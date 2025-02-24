use arrow::array::{RecordBatch, UInt64Array};
use arrow::datatypes::DataType::UInt64;
use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use std::collections::{BTreeMap, Bound, HashMap};
use std::fmt::Debug;
use std::sync::{Arc, OnceLock};

static CELL: OnceLock<CustomDataSourceInner> = OnceLock::new();

/// A User, with an id and a bank account
#[derive(Clone, Debug)]
pub struct User {
    pub id: u64,
    pub amount: u64,
    pub age: u64,
}

impl User {
    pub fn get_schema() -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("id", UInt64, false),
            Field::new("amount", UInt64, false),
            Field::new("age", UInt64, false),
        ]))
    }
}

pub struct CustomDataSourceInner {
    pub users: HashMap<u64, User>,
    pub idx_age: BTreeMap<u64, u64>,
    pub idx_amount: BTreeMap<u64, u64>,
}

impl Default for CustomDataSourceInner {
    fn default() -> Self {
        let mut s = Self {
            users: HashMap::new(),
            idx_age: BTreeMap::new(),
            idx_amount: BTreeMap::new(),
        };

        s.populate_users();

        s
    }
}

impl CustomDataSourceInner {
    pub fn add_user(&mut self, user: User) {
        self.users.insert(user.id, user.clone());
        self.idx_age.insert(user.age, user.id);
        self.idx_amount.insert(user.amount, user.id);
    }

    pub(crate) fn populate_users(&mut self) {
        self.add_user(User {
            id: 1,
            amount: 9_000,
            age: 15,
        });
        self.add_user(User {
            id: 2,
            amount: 100,
            age: 2,
        });
        self.add_user(User {
            id: 3,
            amount: 1_000,
            age: 50,
        });
    }
}

pub fn get_user(id: u64) -> Option<RecordBatch> {
    let maybe = CELL
        .get_or_init(CustomDataSourceInner::default)
        .users
        .get(&id)
        .cloned();

    if let Some(user) = maybe {
        let schema = User::get_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![user.id])),
                Arc::new(UInt64Array::from(vec![user.amount])),
                Arc::new(UInt64Array::from(vec![user.age])),
            ],
        )
        .unwrap();

        Some(batch)
    } else {
        None
    }
}

pub fn scan_age_index(range: (Bound<u64>, Bound<u64>)) -> Arc<dyn ExecutionPlan> {
    let (ages, values): (Vec<u64>, Vec<u64>) = CELL
        .get_or_init(CustomDataSourceInner::default)
        .idx_age
        .range(range)
        .unzip();
    build_index_table(("age", &ages), ("id", &values))
}

pub fn scan_amount_index(range: (Bound<u64>, Bound<u64>)) -> Arc<dyn ExecutionPlan> {
    let (ages, values): (Vec<u64>, Vec<u64>) = CELL
        .get_or_init(CustomDataSourceInner::default)
        .idx_amount
        .range(range)
        .unzip();
    build_index_table(("amount", &ages), ("id", &values))
}

fn build_index_table(a: (&str, &Vec<u64>), b: (&str, &Vec<u64>)) -> Arc<dyn ExecutionPlan> {
    let batch = build_idx_u64(a, b);
    let schema = batch.schema();
    Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
}

pub fn build_idx_u64(a: (&str, &Vec<u64>), b: (&str, &Vec<u64>)) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new(a.0, UInt64, false),
        Field::new(b.0, UInt64, false),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt64Array::from(a.1.clone())),
            Arc::new(UInt64Array::from(b.1.clone())),
        ],
    )
    .unwrap()
}

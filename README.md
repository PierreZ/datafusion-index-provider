# datafusion-index-provider
Prototype approaches to implementing Indexes in DataFusion

## How to Use

To use the `datafusion-index-provider` library to add indexing capabilities to your DataFusion `TableProvider`, you'll create custom `ExecutionPlan` nodes that DataFusion can integrate into its query plans. Here's how:

1.  **Define Your Data and Schema:**
    *   Ensure your data is available, typically as one or more `arrow::record_batch::RecordBatch` instances.
    *   Define the `arrow::datatypes::SchemaRef` for your table.

2.  **Implement the `Index` Trait for Each Indexed Column:**
    *   For each column (or combination of columns) you want to index, create a struct (e.g., `MyColumnIndex`).
    *   Implement the `datafusion_index_provider::physical::indexes::index::Index` trait for this struct.
        *   *See `tests/common/age_index.rs` for an example.*

3.  **Implement the `IndexedTableProvider` Trait: Altering the Physical Plan**
    
    The core of this library is the `IndexedTableProvider` trait. By implementing this trait for your `TableProvider`, you enable DataFusion's query optimizer to replace a standard full `TableScan` physical operator with a more efficient, index-driven sequence of operations.

    **Helicopter View: How the Physical Plan Changes**

    Imagine a query like `SELECT a, b FROM your_table WHERE indexed_col = X AND other_col = Y`.

    *   **Without indexing (standard DataFusion):**
        The physical plan (a tree of `ExecutionPlan`s) might look like this:

        ```text
        +-------------------------+
        | Projection(a, b)        |  <-- Produces final SendableRecordBatchStream
        +-------------------------+
                  ^
                  |  (Stream of RecordBatches with a, b, indexed_col, other_col after filtering)
        +-------------------------+
        | Filter(indexed_col = X  |
        |        AND other_col = Y)|
        +-------------------------+
                  ^
                  |  (Stream of RecordBatches with a, b, indexed_col, other_col)
        +-------------------------+
        | TableScan(your_table)   |  <-- Reads all data
        |  (cols: a,b,idx,other)  |
        +-------------------------+
        ```
        Here, `TableScan` reads all selected columns for all rows. The `Filter` processes all these rows, and finally `Projection` selects the output.

    *   **With `IndexedTableProvider`:**
        If `indexed_col` has an index, your provider helps DataFusion build a more efficient plan:

        ```text
        +-------------------------+
        | IndexJoinExec           |  <-- Your custom exec, produces final SendableRecordBatchStream
        |  (Project a,b;          |
        |   Filter other_col = Y) |
        +-------------------------+
                  ^       ^
                  |       | (Stream of base table data for specific RowIDs)
                  |       +---------------------------+
                  |       | Your Base Table Data      |
                  |       | (e.g., Vec<RecordBatch>)  |
                  |       +---------------------------+
                  |
                  | (Stream of RowIDs matching indexed_col = X)
        +-------------------------+
        | IndexLookupExec         |  <-- Your custom exec using the index
        |  (indexed_col = X)      |
        +-------------------------+
        ```
        In this indexed plan:
        1.  `IndexLookupExec`: Uses the index on `indexed_col` to quickly find and stream only the row identifiers (RowIDs) of rows matching `indexed_col = X`.
        2.  `IndexJoinExec`:
            *   Takes the stream of RowIDs.
            *   Fetches only these specific rows from your base table data (accessing only necessary columns like `a`, `b`, `other_col`).
            *   Applies any `remaining_filters` (like `other_col = Y`).
            *   Applies the final `projection` (`a`, `b`).
        This targeted approach avoids a full table scan, leading to significant performance gains if the index is selective.

    **Key `IndexedTableProvider` Methods and Their Roles in Plan Modification:**

    Your `TableProvider` struct must implement `datafusion_index_provider::provider::IndexedTableProvider`. This trait allows your provider to participate in query planning:

    *   `fn indexes(&self) -> Result<Vec<Arc<dyn Index>>>`:
        *   **Purpose:** Informs DataFusion about all `Index` implementations (from step 2) available for this table.
    *   `fn get_indexed_columns_names(&self) -> Result<HashSet<String>>`:
        *   **Purpose:** Provides a quick way for DataFusion to know which columns of your table are indexed.
    *   `fn analyze_and_optimize_filters(&self, filters: &[Expr]) -> Result<(Vec<Expr>, Vec<Expr>)>`:
        *   **(Often the default trait implementation is sufficient, but can be overridden for complex logic).**
        *   **Purpose:** DataFusion passes the filters from the query to this method. Your implementation (or the default) analyzes these filters, separating them into two groups:
            1.  `index_filters`: Expressions that can be satisfied by one of your declared indexes.
            2.  `remaining_filters`: Expressions that cannot be handled by an index and will need to be applied after data retrieval.
        *   **Impact:** This separation guides DataFusion on which filters can be pushed down into an index lookup.
    *   `fn create_index_scan_exec_for_expr(&self, expr: &Expr) -> Result<Arc<dyn ExecutionPlan>>`:
        *   **Purpose:** For each filter expression identified by `analyze_and_optimize_filters` as indexable (e.g., `indexed_col = X`), DataFusion calls this method.
        *   **Your Task:** Construct and return an `ExecutionPlan` (typically your custom `IndexLookupExec`). This plan, when executed by DataFusion, must use the appropriate `Index` to find matching row identifiers and produce a `SendableRecordBatchStream` containing these identifiers (e.g., as a `UInt64Array` column named `datafusion_index_provider::physical::ROW_ID_COLUMN_NAME`).
        *   **Impact:** This generates the first stage of the index-driven part of the physical plan (the `IndexLookupExec` in the example).
    *   `fn merge_indexes_streams(&self, lookups: Vec<Arc<dyn ExecutionPlan>>, projection: Option<&Vec<usize>>) -> Result<Arc<dyn ExecutionPlan>>`:
        *   **Purpose:** DataFusion calls this method with:
            *   `lookups`: A list of all `ExecutionPlan`s generated by `create_index_scan_exec_for_expr` for the current query (e.g., one for an `age` filter, another for a `department` filter if both are indexed and part of the query).
            *   `projection`: The final columns required by the query.
        *   **Your Task:**
            1.  If `lookups` contains multiple plans (e.g., for `indexed_col1 = A AND indexed_col2 = B`), you might first use `datafusion_index_provider::physical::joins::try_create_lookup_join` to create an `ExecutionPlan` that efficiently intersects (or unions, depending on logic) the streams of row identifiers from these individual lookups.
            2.  Then, take the resulting plan that streams row identifiers (whether it's from a single lookup or a joined one) and wrap it with your `IndexJoinExec` (or a similar custom `ExecutionPlan`).
            3.  This `IndexJoinExec` is responsible for:
                *   Taking the incoming `SendableRecordBatchStream` of final row identifiers.
                *   Fetching the data for only these rows from your base table's `RecordBatch`es.
                *   Applying the `projection` (and any `remaining_filters` identified by `analyze_and_optimize_filters`).
                *   Producing the final `SendableRecordBatchStream` of results.
        *   **Impact:** This generates the latter stage of the index-driven plan (the `IndexJoinExec` in the example), effectively replacing the need for a full `TableScan` followed by filtering.

    *   *See `tests/common/employee_provider.rs` for a detailed example of these methods creating `IndexLookupExec` and potentially using `IndexedTableScanExec` (often via a custom wrapper like the test's `IndexJoinExec`).*

**Helper Components Provided by This Library**

This library provides several pre-built `ExecutionPlan` structures, stream utilities, and helper functions to simplify the development of your custom `IndexedTableProvider`:

*   **Core Traits:**
    *   `Index` (`datafusion_index_provider::physical::indexes::index::Index`): The fundamental trait you implement to define the logic of your specific index (e.g., how to find row IDs for a given predicate).
    *   `IndexedTableProvider` (`datafusion_index_provider::provider::IndexedTableProvider`): The main trait your `TableProvider` implements to integrate with DataFusion's planning and to define how index lookups are translated into execution plans.

*   **Execution Plan Implementations (`ExecutionPlan`):**
    *   `IndexLookupExec` (`datafusion_index_provider::physical::indexes::scan::IndexLookupExec`):
        *   **Purpose:** An `ExecutionPlan` designed to take a collection of row identifiers (typically `Vec<u64>`) obtained from your `Index` logic. When executed by DataFusion, it produces a `SendableRecordBatchStream`. This stream yields `RecordBatch`es containing a single column of these row identifiers (named by the `ROW_ID_COLUMN_NAME` constant).
        *   **Usage:** You'll typically construct and return this from your `IndexedTableProvider::create_index_scan_exec_for_expr` method.
    *   `IndexedTableScanExec` (`datafusion_index_provider::record_fetch::IndexedTableScanExec`):
        *   **Purpose:** An `ExecutionPlan` designed to efficiently fetch specific rows from your base data (provided as `Vec<RecordBatch>`) given an input `SendableRecordBatchStream` of row identifiers.
        *   **Usage:** This is the core component for the data retrieval step. In your `IndexedTableProvider::merge_indexes_streams` method, after you have a final plan that streams row IDs (either from a single `IndexLookupExec` or after combining multiple using `try_create_lookup_join`), you would use `IndexedTableScanExec` (or a custom plan that utilizes it) to take that stream of row IDs and your table's actual data batches to produce the stream of final, projected data rows.

*   **Utility Functions & Constants:**
    *   `try_create_lookup_join` (`datafusion_index_provider::physical::joins::try_create_lookup_join`):
        *   **Purpose:** A helper function that takes two `ExecutionPlan`s (which are expected to stream row identifiers) and creates an appropriate DataFusion join operator (`HashJoinExec` or `SortMergeJoinExec`) to combine their row ID streams. This is useful for handling queries with predicates on multiple indexed columns (e.g., `WHERE indexed_col1 = A AND indexed_col2 = B`) to find the intersection (or union) of their row ID sets.
        *   **Usage:** Typically used within `IndexedTableProvider::merge_indexes_streams` when `lookups` contains more than one plan. The output of this function is another `ExecutionPlan` that streams the combined row IDs.
    *   `ROW_ID_COLUMN_NAME` (`datafusion_index_provider::physical::ROW_ID_COLUMN_NAME`):
        *   **Purpose:** A public constant string (`__row_id__`) representing the conventional column name for row identifiers. This name is used by `IndexLookupExec` for its output column and expected by components like `IndexedTableScanExec` or when configuring join conditions on row ID streams.
        *   **Usage:** Reference this constant when creating schemas for your index lookup streams and when defining join conditions involving row identifiers.

4.  **Example Physical Plan (from `EXPLAIN SELECT name, age FROM employees WHERE age = 25`):**
    ```
    IndexJoinExec: projection=true, batches=1
      RepartitionExec: partitioning=RoundRobinBatch(N), input_partitions=1
        IndexLookupExec: indices_count=1, sorted=false
    ```
    *   This plan shows the library in action:
        *   `IndexLookupExec`: The plan created by `create_index_scan_exec_for_expr` using the `AgeIndex`. It will stream row IDs for employees with `age = 25`.
        *   `RepartitionExec`: Standard DataFusion operator for parallelism.
        *   `IndexJoinExec`: The plan created by `merge_indexes_streams`. It takes the stream of row IDs and fetches the `name` and `age` from the base `employees` data, streaming the final result.

5.  **Instantiate and Register Your `IndexedTableProvider`:**
    *   Create an instance of your `IndexedTableProvider` (holding data and initialized `Index`es).
    *   Register it with DataFusion's `SessionContext`.
        ```rust
        use std::sync::Arc;
        use datafusion::execution::context::SessionContext;
        let ctx = SessionContext::new();
        let my_table_provider = Arc::new(MyIndexedTable::new(...));
        ctx.register_table("my_table_name", my_table_provider).unwrap();
        ```

6.  **Query Your Table:**
    *   Execute SQL. DataFusion's optimizer will now consider your index plans.

## Next TODOs

* improve metrics
* add more tests

# INSERT

## description

### Syntax

```sql
INSERT INTO table_name
[ PARTITION (p1, ...) ]
[ WITH LABEL label]
[ (column [, ...]) ]
[ [ hint [, ...] ] ]
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }
```

### Parameters

```plain text
tablet_name:  Destination table of imported data. It can also be in `db_name.table_name` form. 

partitions: Specifies the partition to be imported.It must be partitions existent in`table_name`. Multiple partition names are separated by commas. 

label: Specifies a label for the Insert task. 

column_name: Specified the destination column which must be existnent in `table_name`. 

expression: The corresponding expression that needs to be assigned to a column.

DEFAULT: Set default value to the corresponding column.

query: A normal query. The query results will be written to the target. 

hint: some indicators used to indicate the execution behavior of `INSERT`.  Both `streaming` and the default non `streaming` methods use the synchronous method to complete the execution of the `INSERT` statement.
In the non `streaming` mode, a label will be returned after execution, which is convenient for users to query the imported status through `SHOW LOAD`. 
```

### Note

When the `INSERT` statement is currently executed, the default behaviour is filtering for data that does not conform to the target table format, such as super long string. However, for business scenarios that require that data cannot be filtered, you can set the session variable `enable_insert_strict` as `true` to ensure that`INSERT` will not be executed successfully when data is filtered out.

## example

The`test` table contains two columns`c1` and `c2`.

1. Import a row of data into the`test`table.

    ```sql
    INSERT INTO test VALUES (1, 2);
    INSERT INTO test (c1, c2) VALUES (1, 2);
    INSERT INTO test (c1, c2) VALUES (1, DEFAULT);
    INSERT INTO test (c1) VALUES (1);
    ```

    The first and second statements have the same effect. When no target column is specified, the column order in the table is used as the default target column. The third and fourth statements express the same meaning. Use the default value of`c2`column to complete data import.

2. Import multiple rows of data into the`test`table at one time.

    ```sql
    INSERT INTO test VALUES (1, 2), (3, 2 + 2);
    INSERT INTO test (c1, c2) VALUES (1, 2), (3, 2 * 2);
    INSERT INTO test (c1) VALUES (1), (3);
    INSERT INTO test (c1, c2) VALUES (1, DEFAULT), (3, DEFAULT);
    ```

    The effects of the first and second statements are the same. Import two pieces of data into the`test`table at one time. The effects of the third and fourth statements are known. Use the default value of column`c2`to import two pieces of data into the`test`table.

3. Import a query statement result into the `test` table.

    ```sql
    INSERT INTO test SELECT * FROM test2;
    INSERT INTO test (c1, c2) SELECT * from test2;
    ```

4. Import a query statement result into the `test` table, and specify partition and label.

    ```sql
    INSERT INTO test PARTITION(p1, p2) WITH LABEL `label1` SELECT * FROM test2;
    INSERT INTO test WITH LABEL `label1` (c1, c2) SELECT * from test2;
    ```

    Asynchronous import is actually a synchronous import encapsulated into asynchronous. Filling in streaming is **as efficient as** not filling in.

    Since the previous import methods of StarRocks were asynchronous, in order to be compatible with the old usage habits, the `INSERT` statement without streaming will still return a label. The user needs to view the status of the`label`import job through the `SHOW LOAD`command.

## keyword

INSERT

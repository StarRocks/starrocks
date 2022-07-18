# ALTER TABLE

## description

This statement is used to modify an existing table. If no rollup index is specified, the default operation is base index.

The statement is divided into three types of operations: schema change, rollup, partition, which cannot appear in an ALTER TABLE statement at the same time. Schema change and rollup are asynchronous operations and are returned if the task is submitted successfully. User can then use the SHOW ALTER command to check the progress.

Partition is a synchronous operation, and a command return indicates that execution is finished.

Syntax:

```SQL
ALTER TABLE [database.]table
alter_clause1[, alter_clause2, ...];
```

alter_clause is divided into partition, rollup, schema change, rename, index and swap.

Partition supports the following several modifications:

1. Add partitions

    Syntax:

    ```SQL
    ADD PARTITION [IF NOT EXISTS] partition_name
    partition_desc ["key"="value"]
    [DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]]
    ```

    Note:

    1. Partition_desc supports the following two expressions:
        * VALUES LESS THAN [MAXVALUE|("value1", ...)]
        * VALUES [("value1", ...), ("value1", ...))
    2. partition is the left-closed-right-open interval. If the user only specifies the right boundary, the system will automatically determine the left boundary.
    3. If the bucket mode is not specified, the bucket method used by the built-in table is automatically used.
    4. If the bucket mode is specified, only the bucket number can be modified, and the bucket mode or bucket column cannot be modified.
    5. User can set some properties of the partition in ["key"="value"] (see CREATE TABLE for details).

2. Drop partitions

    Syntax:

    ```sql
    DROP PARTITION [IF EXISTS | FORCE] partition_name
    ```

    Note:
    1. Keep at least one partition for partitioned tables.
    2. After executing DROP PARTITION for a while, the dropped partition can be recovered by the RECOVER statement. See the RECOVER statement for details.
    3. If DROP PARTITION FORCE is executed, the partition will be deleted directly and cannot be recovered without checking whether there are any unfinished activities on the partition. Thus, generally this operation is not recommended.

3. Modify partition properties

    Syntax:

    ```sql
    MODIFY PARTITION p1|(p1[, p2, ...]) SET ("key" = "value", ...)
    ```

    Note:
    1. The following properties of the partition can currently be modified.
        * storage_medium
        * storage_cooldown_time
        * replication_num
        * in_memory

    2. For single-partition tables, partition_name is the same as the table name.

Rollup supports the following creation modes:

1. Create a rollup index

    Syntax:

    ```SQL
    ADD ROLLUP rollup_name (column_name1, column_name2, ...)
    [FROM from_index_name]
    [PROPERTIES ("key"="value", ...)]
    ```

    properties: Support setting timeout time and the default timeout time is one day.

    Example:

    ```SQL
    ADD ROLLUP r1(col1,col2) from r0
    ```

2. Creating rollup index in batches

    Syntax:

    ```SQL
    ADD ROLLUP [rollup_name (column_name1, column_name2, ...)
    [FROM from_index_name]
    [PROPERTIES ("key"="value", ...)],...]
    ```

    Example:

    ```SQL
    ADD ROLLUP r1(col1,col2) from r0, r2(col3,col4) from r0
    ```

3. Note:

    1. If from_index_name is not specified, then create from base index by default.
    2. The columns in the rollup table must be existing columns in from_index.
    3. In properties, user can specify the storage format. See CREATE TABLE for details.

4. Drop the rollup index.

    Syntax:

    ```SQL
    DROP ROLLUP rollup_name [PROPERTIES ("key"="value", ...)]
    ```

    Example:

    DROP ROLLUP r1

5. Batch dropping rollup index

    Syntax:

    ```sql
    DROP ROLLUP [rollup_name [PROPERTIES ("key"="value", ...)],...]
    ```

    Example: DROP ROLLUP r1,r2
    2.2 Note:

    1. You cannot drop the base index.

    schema change supports the following modifications:

6. Add a column to the specified location of the specified index

    Syntax:

    ```sql
    ADD COLUMN column_name column_type [KEY | agg_type] [DEFAULT "default_value"]
    [AFTER column_name|FIRST]
    [TO rollup_index_name]
    [PROPERTIES ("key"="value", ...)]
    ```

    Note:

    ```plain text
    1. If you add a value column to aggregation model, you need to specify agg_type.
    2. If you add a key column to non-aggregation models (such as DUPLICATE KEY) , you need to specify the KEY keyword.
    3. You cannot add a column that already exists in the base index to the rollup index.
     (You can recreate a rollup index if needed)
    ```

7. Add multiple columns to the specified index.

    Syntax:

    ```sql
    ADD COLUMN (column_name1 column_type [KEY | agg_type] DEFAULT "default_value", ...)
    [TO rollup_index_name]
    [PROPERTIES ("key"="value", ...)]
    ```

    Note:

    1. If you add value columnin aggregation models, you need to specify agg_type.

    2. If you add a key column in non-aggregation models, you need to specify the KEY keyword.

    3. You cannot add a column that already exists in the base index to the rollup index.

       (You can recreate a rollup index if needed)

8. Drop a column from the specified index.

    Syntax:

    ```sql
    DROP COLUMN column_name
    [FROM rollup_index_name]
    ```

    Note:
    1. You cannot drop partition column.
    2. If the column is dropped from the base index, it will also be dropped if it is included in the rollup index.

9. Modify the column type and column position of the specified index.

    Syntax:

    ```sql
    MODIFY COLUMN column_name column_type [KEY | agg_type] [NULL | NOT NULL] [DEFAULT "default_value"]
    [AFTER column_name|FIRST]
    [FROM rollup_index_name]
    [PROPERTIES ("key"="value", ...)]
    ```

    Note:
    1. If you modify the value column in aggregation models, you need to specify agg_type.
    2. If you modify the key column in non-aggregation models, you need to specify the KEY keyword.
    3. Only the type of column can be modified. The other properties of the column remain as they are currently. (i.e. other properties need to be explicitly written in the statement according to the original property, see example 8).
    4. The partition column cannot be modified.
    5. The following types of conversions are currently supported (accuracy loss is guaranteed by the user).
    Convert TINYINT/SMALLINT/INT/BIGINT to TINYINT/SMALLINT/INT/BIGINT/DOUBLE.
    Convert TINTINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE/DECIMAL to VARCHAR.
    VARCHAR supports modification of maximum length.
    Convert VARCHAR to TINTINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE.
    Convert VARCHAR to DATE (currently support six formats: "%Y-%m-%d", "%y-%m-%d", "%Y%m%d", "%y%m%d", "%Y/%m/%d, "%y/%m/%d")
    Convert DATETIME to DATE(only year-month-day information is retained, i.e.  `2019-12-09 21:47:05` <--> `2019-12-09`)
    Convert DATE to DATETIME (set hour, minute, second to zero, For example: `2019-12-09` <--> `2019-12-09 00:00:00`)
    Convert FLOAT to DOUBLE
    Convert INT to DATE (If the INT data fails to convert, the original data remains the same)
    6. Does not support conversion from NULL to NOT NULL.

10. Reorder the columns of the specified index.

    Syntax:

    ```sql
    ORDER BY (column_name1, column_name2, ...)
    [FROM rollup_index_name]
    [PROPERTIES ("key"="value", ...)]
    ```

    Note:
    1. All columns in the index must be written.
    2. The value column is listed after the key column.

11. Modify the properties of table. It urrently supports modifying the bloom filter column, the colocate_with property and the dynamic_partition property, the replication_num property and default.replication_num property.

    Syntax:

    ```sql
    PROPERTIES ("key"="value")
    ```

    Note:
    You can also modify the properties by merging into the above schema change operation. See the example below:

    rename supports modification of the following names:

## example

1. Modify the table name

    Syntax:

    ```sql
    RENAME new_table_name;
    ```

2. Modify the rollup index name

    Syntax:

    ```sql
    RENAME ROLLUP old_rollup_name new_rollup_name;
    ```

3. Modify the partition name

    Syntax:

    ```sql
    RENAME PARTITION old_partition_name new_partition_name;
    ```

bitmap index supports the following modifications:

1. Create a bitmap index

    Syntax:

    ```sql
    ADD INDEX index_name (column [, ...],) [USING BITMAP] [COMMENT 'balabala'];
    ```

    Note:

    ```plain text
    1. Bitmap index is only supported for the current version.
    2. A BITMAP index is created only in a single column.
    ```

2. Drop index

    Syntax:

    ```sql
    DROP INDEX index_name；
    ```

    swap supports the atomic exchange of two tables.

3. Atomic exchange of two tables.

    Syntax:

    ```sql
    SWAP WITH table_name
    ```

[table]

1. Alter the default number of replications of the table, which is used as the default number of the replications.

    ```sql
    ALTER TABLE example_db.my_table
    SET ("default.replication_num" = "2");
    ```

2. Alter the actual number of replications of  a single-partition table (single-partition table only).

    ```sql
    ALTER TABLE example_db.my_table
    SET ("replication_num" = "3");
    ```

[partition]

1. Add partition. Existing partition [MIN, 2013-01-01), add partition [2013-01-01, 2014-01-01), and use default bucket mode.

    ```sql
    ALTER TABLE example_db.my_table
    ADD PARTITION p1 VALUES LESS THAN ("2014-01-01");
    ```

2. Add partition and use the new number of buckets.

    ```sql
    ALTER TABLE example_db.my_table
    ADD PARTITION p1 VALUES LESS THAN ("2015-01-01")
    DISTRIBUTED BY HASH(k1) BUCKETS 20;
    ```

3. Add partition and use the new number of replications.

    ```sql
    ALTER TABLE example_db.my_table
    ADD PARTITION p1 VALUES LESS THAN ("2015-01-01")
    ("replication_num"="1");
    ```

4. Alter the number of partition replications.

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY PARTITION p1 SET("replication_num"="1");
    ```

5. Batch altering the specified partitions.

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY PARTITION (p1, p2, p4) SET("in_memory"="true");
    ```

6. Batch altering all partitions.

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY PARTITION (*) SET("storage_medium"="HDD");
    ```

7. Drop partition.

    ```sql
    ALTER TABLE example_db.my_table
    DROP PARTITION p1;
    ```

8. Add a partition that specifies upper and lower bounds.

    ```sql
    ALTER TABLE example_db.my_table
    ADD PARTITION p1 VALUES [("2014-01-01"), ("2014-02-01"));
    ```

[rollup]

1. Create index: example_rollup_index, based on base index(k1,k2,k3,v1,v2). Column-based storage.

    ```sql
    ALTER TABLE example_db.my_table
    ADD ROLLUP example_rollup_index(k1, k3, v1, v2)
    PROPERTIES("storage_type"="column");
    ```

2. Create index: example_rollup_index2, based on example_rollup_index(k1,k3,v1,v2).

    ```sql
    ALTER TABLE example_db.my_table
    ADD ROLLUP example_rollup_index2 (k1, v1)
    FROM example_rollup_index;
    ```

3. Create index: example_rollup_index3, based on base index (k1, k2, k3, v1), and custom rollup timeout time is one hour.

    ```sql
    ALTER TABLE example_db.my_table
    ADD ROLLUP example_rollup_index(k1, k3, v1)
    PROPERTIES("storage_type"="column", "timeout" = "3600");
    ```

4. Drop index: example_rollup_index2.

    ```sql
    ALTER TABLE example_db.my_table
    DROP ROLLUP example_rollup_index2;
    ```

[SchemaChange]

1. Add a key column new_col (non-aggregation model) to the col1 of example_rollup_index.

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT KEY DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

2. Add a value column new_col (non-aggregation model) to the col1 of example_rollup_index.

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

3. Add a key column new_col (aggregation model) to col1 of example_rollup_index.

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

4. Add a value column new_col SUM (aggregation model) to the col1 of example_rollup_index.

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT SUM DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

5. Add multiple columns to the example_rollup_index (aggregation model).

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN (col1 INT DEFAULT "1", col2 FLOAT SUM DEFAULT "2.3")
    TO example_rollup_index;
    ```

6. Drop a column from example_rollup_index.

    ```sql
    ALTER TABLE example_db.my_table
    DROP COLUMN col2
    FROM example_rollup_index;
    ```

7. Modify col1 column type of the base index to BIGINT and put it after col2 column.

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY COLUMN col1 BIGINT DEFAULT "1" AFTER col2;
    ```

8. Modify the maximum length of the val1 column of the base index. The original val1 is (val1 VARCHAR(32) REPLACE DEFAULT "abc").

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY COLUMN val1 VARCHAR(64) REPLACE DEFAULT "abc";
    ```

9. Reorder the columns in example_rollup_index (set the original column order: k1, k2, k3, v1, v2).

    ```sql
    ALTER TABLE example_db.my_table
    ORDER BY (k3,k1,k2,v2,v1)
    FROM example_rollup_index;
    ```

10. Perform both operations simultaneously.

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN v2 INT MAX DEFAULT "0" AFTER k2 TO example_rollup_index,
    ORDER BY (k3,k1,k2,v2,v1) FROM example_rollup_index;
    ```

11. Alter the bloom filter column of the table.

     ```sql
     ALTER TABLE example_db.my_table SET ("bloom_filter_columns"="k1,k2,k3");
     ```

     It can also be merged into the above schema change operation (note that the syntax of multiple clauses is slightly different).

     ```sql
     ALTER TABLE example_db.my_table
     DROP COLUMN col2
     PROPERTIES ("bloom_filter_columns"="k1,k2,k3");
     ```

12. Alter the Colocate property of the table.

     ```sql
     ALTER TABLE example_db.my_table set ("colocate_with" = "t1");
     ```

13. Alter the bucketing mode of the table from Random Distribution to Hash Distribution.

     ```sql
     ALTER TABLE example_db.my_table set ("distribution_type" = "hash");
     ```

14. Alter the dynamic partition properties of the table (support adding dynamic partition properties to tables).

     ```sql
     ALTER TABLE example_db.my_table set ("dynamic_partition.enable" = "false");
     ```

     If you need to add dynamic partition attributes to a table without dynamic partition properties, you need to specify all dynamic partition properties.

     ```sql
     ALTER TABLE example_db.my_table set ("dynamic_partition.enable" = "true", "dynamic_partition.time_unit" = "DAY", "dynamic_partition.end" = "3", "dynamic_partition.prefix" = "p", "dynamic_partition.buckets" = "32");
     ```

15. Alter the in_memory property of the table.

    ```sql
    ALTER TABLE example_db.my_table set ("in_memory" = "true");
    ```

[rename]

1. Rename table1 to table2.

    ```sql
    ALTER TABLE table1 RENAME table2;
    ```

2. Rename rollup index rollup1 of example_table to rollup2.

    ```sql
    ALTER TABLE example_table RENAME ROLLUP rollup1 rollup2;
    ```

3. Rename partition p1 of example_table to p2.

    ```sql
    ALTER TABLE example_table RENAME PARTITION p1 p2;
    ```

[index]

1. Create bitmap index for column siteid on table1.

    ```sql
    ALTER TABLE table1 ADD INDEX index_name (siteid) [USING BITMAP] COMMENT 'balabala';
    ```

2. Drop bitmap index of siteid column on table1.

    ```sql
    ALTER TABLE table1 DROP INDEX index_name;
    ```

[swap]

1. Atomic swap between table1 and table2.

    ```sql
    ALTER TABLE table1 SWAP WITH table2
    ```

## keyword

ALTER,TABLE,ROLLUP,COLUMN,PARTITION,RENAME,SWAP

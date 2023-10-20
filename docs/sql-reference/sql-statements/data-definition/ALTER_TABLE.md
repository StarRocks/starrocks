# ALTER TABLE

## Description

Modifies an existing table.

## Syntax

```SQL
ALTER TABLE [database.]table
alter_clause1[, alter_clause2, ...]
```

`alter_clause` is classified into six operations: partition, rollup, schema change, rename, index, and swap.

- partition: modifies partition properties, drops a partition, or adds a partition.
- rollup: creates or drops a rollup index.
- schema change: adds, drops, or reorder columns, or modify column type.
- rename: renames a table, rollup index, or partition. **Note that column name cannot be modified.**
- index: modifies index (only bitmap index can be modified.)
- swap: atomic exchange of two tables

> **NOTE**
>
> - Schema change, rollup, and partition cannot be used in one ALTER TABLE statement.
> - Schema change, rollup, and swap are asynchronous operations and are returned if the task is submitted successfully. User can use the [SHOW ALTER TABLE](../data-manipulation/SHOW_ALTER.md) command to check the progress.
> - Partition, rename, and index are synchronous operations, and a command return indicates that the execution is finished.

### Modify partition

#### Add a partition

Syntax:

```SQL
ALTER TABLE [database.]table 
ADD PARTITION [IF NOT EXISTS] <partition_name>
partition_desc ["key"="value"]
[DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]];
```

Note:

1. Partition_desc supports the following two expressions:

    ```plain
    VALUES LESS THAN [MAXVALUE|("value1", ...)]
    VALUES [("value1", ...), ("value1", ...))
    ```

2. partition is the left-closed-right-open interval. If the user only specifies the right boundary, the system will automatically determine the left boundary.
3. If the bucket mode is not specified, the bucket method used by the built-in table is automatically used.
4. If the bucket mode is specified, only the bucket number can be modified, and the bucket mode or bucket column cannot be modified.
5. User can set some properties of the partition in ["key"="value"]. See [CREATE TABLE](CREATE_TABLE.md) for details.

#### Drop a partition

Syntax:

```sql
-- Before 2.0
ALTER TABLE [database.]table
DROP PARTITION [IF EXISTS | FORCE] <partition_name>
-- 2.0 or later
ALTER TABLE [database.]table
DROP PARTITION [IF EXISTS] <partition_name> [FORCE]
```

Note:

1. Keep at least one partition for partitioned tables.
2. After executing DROP PARTITION for a while, the dropped partition can be recovered by the RECOVER statement. See the RECOVER statement for details.
3. If DROP PARTITION FORCE is executed, the partition will be deleted directly and cannot be recovered without checking whether there are any unfinished activities on the partition. Thus, generally this operation is not recommended.

#### Add a temporary partition

Syntax:

```sql
ALTER TABLE [database.]table 
ADD TEMPORARY PARTITION [IF NOT EXISTS] <partition_name>
partition_desc ["key"="value"]
[DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]]
```

#### Use a temporary partition to replace current partition

Syntax:

```sql
ALTER TABLE [database.]table
REPLACE PARTITION <partition_name>
partition_desc ["key"="value"]
WITH TEMPORARY PARTITION
partition_desc ["key"="value"]
[PROPERTIES ("key"="value", ...)]
```

#### Drop a temporary partition

Syntax:

```sql
ALTER TABLE [database.]table
DROP TEMPORARY PARTITION <partition_name>
```

#### Modify partition properties

Syntax:

```sql
ALTER TABLE [database.]table
MODIFY PARTITION p1|(p1[, p2, ...]) SET ("key" = "value", ...)
```

Note:

1. The following properties of a partition can be modified:

   - storage_medium
   - storage_cooldown_time
   - replication_num
   - in_memory

2. For single-partition tables, partition name is the same as the table name.

### Modify rollup index

#### Create a rollup index

Syntax:

```SQL
ALTER TABLE [database.]table 
ADD ROLLUP rollup_name (column_name1, column_name2, ...)
[FROM from_index_name]
[PROPERTIES ("key"="value", ...)]
```

PROPERTIES: Support setting timeout time and the default timeout time is one day.

Example:

```SQL
ALTER TABLE [database.]table 
ADD ROLLUP r1(col1,col2) from r0;
```

#### Create rollup indexes in batches

Syntax:

```SQL
ALTER TABLE [database.]table
ADD ROLLUP [rollup_name (column_name1, column_name2, ...)
[FROM from_index_name]
[PROPERTIES ("key"="value", ...)],...];
```

Example:

```sql
ALTER TABLE [database.]table
ADD ROLLUP r1(col1,col2) from r0, r2(col3,col4) from r0;
```

Note:

1. If from_index_name is not specified, then create from base index by default.
2. The columns in the rollup table must be existing columns in from_index.
3. In properties, user can specify the storage format. See CREATE TABLE for details.

#### Drop a rollup index

Syntax:

```sql
ALTER TABLE [database.]table
DROP ROLLUP rollup_name [PROPERTIES ("key"="value", ...)];
```

Example:

```sql
ALTER TABLE [database.]table DROP ROLLUP r1;
```

#### Batch drop rollup indexes

Syntax:

```sql
ALTER TABLE [database.]table
DROP ROLLUP [rollup_name [PROPERTIES ("key"="value", ...)],...];
```

Example:

```sql
ALTER TABLE [database.]table DROP ROLLUP r1, r2;
```

Note: You cannot drop the base index.

### Schema change

Schema change supports the following modifications.

#### Add a column to specified location of specified index

Syntax:

```sql
ALTER TABLE [database.]table
ADD COLUMN column_name column_type [KEY | agg_type] [DEFAULT "default_value"]
[AFTER column_name|FIRST]
[TO rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

Note:

```plain text
1. If you add a value column to an Aggregate table, you need to specify agg_type.
2. If you add a key column to a non-Aggregate table (such as a Duplicate Key table), you need to specify the KEY keyword.
3. You cannot add a column that already exists in the base index to the rollup index. (You can recreate a rollup index if needed.)
```

#### Add multiple columns to specified index

Syntax:

```sql
ALTER TABLE [database.]table
ADD COLUMN (column_name1 column_type [KEY | agg_type] DEFAULT "default_value", ...)
[TO rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

Note:

1. If you add a value column to an aggregate table, you need to specify `agg_type`.

2. If you add a key column to a non-aggregate table, you need to specify the KEY keyword.

3. You cannot add a column that already exists in the base index to the rollup index. (You can create another rollup index if needed.)

#### Drop a column from specified index

Syntax:

```sql
ALTER TABLE [database.]table
DROP COLUMN column_name
[FROM rollup_index_name];
```

Note:

1. You cannot drop partition column.
2. If the column is dropped from the base index, it will also be dropped if it is included in the rollup index.

#### Modify the column type and column position of specified index

Syntax:

```sql
ALTER TABLE [database.]table
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

   - Convert TINYINT/SMALLINT/INT/BIGINT to TINYINT/SMALLINT/INT/BIGINT/DOUBLE.
   - Convert TINTINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE/DECIMAL to VARCHAR. VARCHAR supports modification of maximum length.
   - Convert VARCHAR to TINTINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE.
   - Convert VARCHAR to DATE (currently support six formats: "%Y-%m-%d", "%y-%m-%d", "%Y%m%d", "%y%m%d", "%Y/%m/%d, "%y/%m/%d")
   - Convert DATETIME to DATE(only year-month-day information is retained, i.e.  `2019-12-09 21:47:05` `<-->` `2019-12-09`)
   - Convert DATE to DATETIME (set hour, minute, second to zero, For example: `2019-12-09` `<-->` `2019-12-09 00:00:00`)
   - Convert FLOAT to DOUBLE
   - Convert INT to DATE (If the INT data fails to convert, the original data remains the same)

6. Conversion from NULL to NOT NULL is not supported.

#### Reorder the columns of specified index

Syntax:

```sql
ALTER TABLE [database.]table
ORDER BY (column_name1, column_name2, ...)
[FROM rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

Note:

1. All columns in the index must be written.
2. The value column is listed after the key column.

#### Modify table properties

Currently, StarRocks supports modifying bloomfilter columns, colocate_with property, dynamic_partition property, enable_persistent_index property, replication_num property and default.replication_num property.

Syntax:

```sql
PROPERTIES ("key"="value")
```

Note:
You can also modify the properties by merging into the above schema change operation. See the following examples.

### Rename

Rename supports modification of table name, rollup index, and partition name.

#### Rename a table

```sql
ALTER TABLE <table_name> RENAME <new_table_name>
```

#### Rename a rollup index

```sql
ALTER TABLE [database.]table
RENAME ROLLUP <old_rollup_name> <new_rollup_name>
```

#### Rename a partition

```sql
ALTER TABLE [database.]table
RENAME PARTITION <old_partition_name> <new_partition_name>
```

### Modify bitmap indexes

Bitmap index supports the following modifications:

#### Create a bitmap index

Syntax:

```sql
 ALTER TABLE [database.]table
ADD INDEX index_name (column [, ...],) [USING BITMAP] [COMMENT 'balabala'];
```

Note:

```plain text
1. Bitmap index is only supported for the current version.
2. A BITMAP index is created only in a single column.
```

#### Drop an index

Syntax:

```sql
DROP INDEX index_name;
```

### Swap

Swap supports atomic exchange of two tables.

Syntax:

```sql
ALTER TABLE [database.]table
SWAP WITH table_name;
```

## Examples

### Table

1. Modify the default number of replicas of the table, which is used as the default number of replicas for newly added partitions.

    ```sql
    ALTER TABLE example_db.my_table
    SET ("default.replication_num" = "2");
    ```

2. Modify the actual number of replicas of a single-partition table.

    ```sql
    ALTER TABLE example_db.my_table
    SET ("replication_num" = "3");
    ```

### Partition

1. Add a partition and use the default bucketing mode. The existing partition is [MIN, 2013-01-01). The added partition is [2013-01-01, 2014-01-01).

    ```sql
    ALTER TABLE example_db.my_table
    ADD PARTITION p1 VALUES LESS THAN ("2014-01-01");
    ```

2. Add a partition and use the new number of buckets.

    ```sql
    ALTER TABLE example_db.my_table
    ADD PARTITION p1 VALUES LESS THAN ("2015-01-01")
    DISTRIBUTED BY HASH(k1) BUCKETS 20;
    ```

3. Add a partition and use the new number of replicas.

    ```sql
    ALTER TABLE example_db.my_table
    ADD PARTITION p1 VALUES LESS THAN ("2015-01-01")
    ("replication_num"="1");
    ```

4. Alter the number of replicas for a partition.

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY PARTITION p1 SET("replication_num"="1");
    ```

5. Batch alter the `in_memory` property of specified partitions.

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY PARTITION (p1, p2, p4) SET("in_memory"="true");
    ```

6. Batch alter the storage medium of all partitions.

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY PARTITION (*) SET("storage_medium"="HDD");
    ```

7. Drop a partition.

    ```sql
    ALTER TABLE example_db.my_table
    DROP PARTITION p1;
    ```

8. Add a partition that has upper and lower boundaries.

    ```sql
    ALTER TABLE example_db.my_table
    ADD PARTITION p1 VALUES [("2014-01-01"), ("2014-02-01"));
    ```

### Rollup

1. Create an index `example_rollup_index` based on the base index (k1,k2,k3,v1,v2). Column-based storage is used.

    ```sql
    ALTER TABLE example_db.my_table
    ADD ROLLUP example_rollup_index(k1, k3, v1, v2)
    PROPERTIES("storage_type"="column");
    ```

2. Create an index `example_rollup_index2` based on `example_rollup_index(k1,k3,v1,v2)`.

    ```sql
    ALTER TABLE example_db.my_table
    ADD ROLLUP example_rollup_index2 (k1, v1)
    FROM example_rollup_index;
    ```

3. Create an index `example_rollup_index3` based on the base index (k1, k2, k3, v1). The rollup timeout time is set to one hour.

    ```sql
    ALTER TABLE example_db.my_table
    ADD ROLLUP example_rollup_index3(k1, k3, v1)
    PROPERTIES("storage_type"="column", "timeout" = "3600");
    ```

4. Drop an index `example_rollup_index2`.

    ```sql
    ALTER TABLE example_db.my_table
    DROP ROLLUP example_rollup_index2;
    ```

### Schema Change

1. Add a key column `new_col` (non-aggregate column) after the `col1` column of `example_rollup_index`.

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT KEY DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

2. Add a value column `new_col` (non-aggregate column) after the `col1` column of `example_rollup_index`.

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

3. Add a key column `new_col` (aggregate column) after the `col1` column of `example_rollup_index`.

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

4. Add a value column `new_col SUM` (aggregate column) after the `col1` column of `example_rollup_index`.

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT SUM DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

5. Add multiple columns to `example_rollup_index` (aggregate).

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN (col1 INT DEFAULT "1", col2 FLOAT SUM DEFAULT "2.3")
    TO example_rollup_index;
    ```

6. Drop a column from `example_rollup_index`.

    ```sql
    ALTER TABLE example_db.my_table
    DROP COLUMN col2
    FROM example_rollup_index;
    ```

7. Modify the column type of col1 of the base index to BIGINT and put it after `col2`.

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY COLUMN col1 BIGINT DEFAULT "1" AFTER col2;
    ```

8. Modify the maximum length of the `val1` column of the base index to 64. The original length is 32.

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY COLUMN val1 VARCHAR(64) REPLACE DEFAULT "abc";
    ```

9. Reorder the columns in `example_rollup_index`. The original column order is k1, k2, k3, v1, v2.

    ```sql
    ALTER TABLE example_db.my_table
    ORDER BY (k3,k1,k2,v2,v1)
    FROM example_rollup_index;
    ```

10. Perform two operations (ADD COLUMN and ORDER BY) at one time.

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN v2 INT MAX DEFAULT "0" AFTER k2 TO example_rollup_index,
    ORDER BY (k3,k1,k2,v2,v1) FROM example_rollup_index;
    ```

11. Alter the bloomfilter columns of the table.

     ```sql
     ALTER TABLE example_db.my_table
     SET ("bloom_filter_columns"="k1,k2,k3");
     ```

     This operation can also be merged into the above schema change operation (note that the syntax of multiple clauses is slightly different).

     ```sql
     ALTER TABLE example_db.my_table
     DROP COLUMN col2
     PROPERTIES ("bloom_filter_columns"="k1,k2,k3");
     ```

12. Alter the Colocate property of the table.

     ```sql
     ALTER TABLE example_db.my_table
     SET ("colocate_with" = "t1");
     ```

13. Alter the bucketing mode of the table from Random Distribution to Hash Distribution.

     ```sql
     ALTER TABLE example_db.my_table
     SET ("distribution_type" = "hash");
     ```

14. Alter the dynamic partition property of the table.

     ```sql
     ALTER TABLE example_db.my_table
     SET ("dynamic_partition.enable" = "false");
     ```

     If you need to add dynamic partition properties to a table with no dynamic partition properties configured, you need to specify all the dynamic partition properties.

     ```sql
     ALTER TABLE example_db.my_table
     SET (
         "dynamic_partition.enable" = "true",
         "dynamic_partition.time_unit" = "DAY",
         "dynamic_partition.end" = "3",
         "dynamic_partition.prefix" = "p",
         "dynamic_partition.buckets" = "32"
         );
     ```

15. Alter the `in_memory` property of the table.

    ```sql
    ALTER TABLE example_db.my_table
    SET ("in_memory" = "true");
    ```

### Rename

1. Rename `table1` to `table2`.

    ```sql
    ALTER TABLE table1 RENAME table2;
    ```

2. Rename rollup index `rollup1` of `example_table` to `rollup2`.

    ```sql
    ALTER TABLE example_table RENAME ROLLUP rollup1 rollup2;
    ```

3. Rename partition `p1` of `example_table` to `p2`.

    ```sql
    ALTER TABLE example_table RENAME PARTITION p1 p2;
    ```

### Index

1. Create a bitmap index for column `siteid` in `table1`.

    ```sql
    ALTER TABLE table1
    ADD INDEX index_1 (siteid) [USING BITMAP] COMMENT 'balabala';
    ```

2. Drop the bitmap index of column `siteid` in `table1`.

    ```sql
    ALTER TABLE table1
    DROP INDEX index_1;
    ```

### Swap

1. Atomic swap between `table1` and `table2`.

    ```sql
    ALTER TABLE table1 SWAP WITH table2
    ```

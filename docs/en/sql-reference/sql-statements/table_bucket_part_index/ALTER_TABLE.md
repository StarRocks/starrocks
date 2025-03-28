---
displayed_sidebar: docs
---

# ALTER TABLE

## Description

Modifies an existing table, including:

- [Rename table, partition, index, or column](#rename)
- [Modify table comment](#alter-table-comment-from-v31)
- [Modify partitions (add/delete partitions and modify partition attributes)](#modify-partition)
- [Modify the bucketing method and number of buckets](#modify-the-bucketing-method-and-number-of-buckets-from-v32)
- [Modify columns (add/delete columns and change the order of columns)](#modify-columns-adddelete-columns-change-the-order-of-columns)
- [Create/delete rollup index](#modify-rollup-index)
- [Modify bitmap index](#modify-bitmap-indexes)
- [Modify table properties](#modify-table-properties)
- [Atomic swap](#swap)
- [Manual data version compaction](#manual-compaction-from-31)

:::tip
This operation requires the ALTER privilege on the destination table.
:::

## Syntax

```SQL
ALTER TABLE [<db_name>.]<tbl_name>
alter_clause1[, alter_clause2, ...]
```

`alter_clause` can held the following operations: rename, comment, partition, bucket, column, rollup index, bitmap index, table property, swap, and compaction.

- rename: renames a table, rollup index, partition, or column (supported from **v3.3.2 onwards**).
- comment: modifies the table comment (supported from **v3.1 onwards**).
- partition: modifies partition properties, drops a partition, or adds a partition.
- bucket: modifies the bucketing method and number of buckets.
- column: adds, drops, or reorders columns, or modifies column type.
- rollup index: creates or drops a rollup index.
- bitmap index: modifies index (only Bitmap index can be modified).
- swap: atomic exchange of two tables.
- compaction: performs manual compaction to merge versions of loaded data (supported from **v3.1 onwards**).

## Limits and usage notes

- Operations on partition, column, and rollup index cannot be performed in one ALTER TABLE statement.
- Column comments cannot be modified.
- One table can have only one ongoing schema change operation at a time. You cannot run two schema change commands on a table at the same time.
- Operations on bucket, column and rollup index are asynchronous operations. A success message is return immediately after the task is submitted. You can run the [SHOW ALTER TABLE](SHOW_ALTER.md) command to check the progress, and run the [CANCEL ALTER TABLE](CANCEL_ALTER_TABLE.md) command to cancel the operation.
- Operations on rename, comment, partition, bitmap index and swap are synchronous operations, and a command return indicates that the execution is finished.

### Rename

Rename supports modification of table name, rollup index, and partition name.

#### Rename a table

```sql
ALTER TABLE <tbl_name> RENAME <new_tbl_name>
```

#### Rename a rollup index

```sql
ALTER TABLE [<db_name>.]<tbl_name>
RENAME ROLLUP <old_rollup_name> <new_rollup_name>
```

#### Rename a partition

```sql
ALTER TABLE [<db_name>.]<tbl_name>
RENAME PARTITION <old_partition_name> <new_partition_name>
```

#### Rename a column

From v3.3.2 onwards, StarRocks supports renaming columns.

```sql
ALTER TABLE [<db_name>.]<tbl_name>
RENAME COLUMN <old_col_name> [ TO ] <new_col_name>
```

:::note

- After renaming a column from A to B, adding a new column named A is not supported.
- Materialized views built on a renamed column will not take effect. You must rebuild them upon the column with the new name.

:::

### Alter table comment (from v3.1)

Syntax:

```sql
ALTER TABLE [<db_name>.]<tbl_name> COMMENT = "<new table comment>";
```

:::tip
Currently, column comments cannot be modified.
:::

### Modify partition

#### ADD PARTITION(S)

You can choose to add range partitions or list partitions. Adding expression partitions is not supported.

Syntax：

- Range partitions

    ```SQL
    ALTER TABLE
        ADD { single_range_partition | multi_range_partitions } [distribution_desc] ["key"="value"];

    single_range_partition ::=
        PARTITION [IF NOT EXISTS] <partition_name> VALUES partition_key_desc

    partition_key_desc ::=
        { LESS THAN { MAXVALUE | value_list }
        | [ value_list , value_list ) } -- Note that [ represents a left-closed interval.

    value_list ::=
        ( <value> [, ...] )

    multi_range_partitions ::=
        { PARTITIONS START ("<start_date_value>") END ("<end_date_value>") EVERY ( INTERVAL <N> <time_unit> )
        | PARTITIONS START ("<start_integer_value>") END ("<end_integer_value>") EVERY ( <granularity> ) } -- The partition column values still need to be enclosed in double quotes even if the partition column values specified by START and END are integers. However, the interval values in the EVERY clause do not need to be enclosed in double quotes.
    ```

- List partitions

    ```SQL
    ALTER TABLE
        ADD PARTITION <partition_name> VALUES IN (value_list) [distribution_desc] ["key"="value"];

    value_list ::=
        value_item [, ...]

    value_item ::=
        { <value> | ( <value> [, ...] ) }
    ```

Parameters:

- Partition-related parameters:

  - For range partitions, you can add a single range partition (`single_range_partition`) or multiple range partitions in batch (`multi_range_partitions`).
  - For list partitions, you can only add a single list partition.

- `distribution_desc`:

   You can set the number of buckets for the new partition separately, but you cannot set the bucketing method separately.

- `"key"="value"`:

   You can set properties for the new partition. For details, see [CREATE TABLE](CREATE_TABLE.md#properties).

Examples:

- Range partitions

  - If the partition column is specified as `event_day` at table creation, for example `PARTITION BY RANGE(event_day)`, and a new partition needs to be added after table creation, you can execute:

    ```sql
    ALTER TABLE site_access ADD PARTITION p4 VALUES LESS THAN ("2020-04-30");
    ```

  - If the partition column is specified as `datekey` at table creation, for example `PARTITION BY RANGE (datekey)`, and multiple partitions need to be added in batch after table creation, you can execute:

    ```sql
    ALTER TABLE site_access
        ADD PARTITIONS START ("2021-01-05") END ("2021-01-10") EVERY (INTERVAL 1 DAY);
    ```

- List partitions

  - If a single partition column is specified at table creation, for example `PARTITION BY LIST (city)`, and a new partition needs to be added after table creation, you can execute:

    ```sql
    ALTER TABLE t_recharge_detail2
    ADD PARTITION pCalifornia VALUES IN ("Los Angeles","San Francisco","San Diego");
    ```

  - If multiple partition columns are specified at table creation, for example `PARTITION BY LIST (dt,city)`, and a new partition needs to be added after table creation, you can execute:

    ```sql
    ALTER TABLE t_recharge_detail4 
    ADD PARTITION p202204_California VALUES IN
    (
        ("2022-04-01", "Los Angeles"),
        ("2022-04-01", "San Francisco"),
        ("2022-04-02", "Los Angeles"),
        ("2022-04-02", "San Francisco")
    );
    ```

#### DROP PARTITION(S)

- Drop a single partition:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP PARTITION [ IF EXISTS ] <partition_name> [ FORCE ]
```

- Drop partitions in batch (Supported from v3.3.1):

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP PARTITIONS [ IF EXISTS ]  { partition_name_list | multi_range_partitions } [ FORCE ]

partition_name_list ::= ( <partition_name> [, ... ] )

multi_range_partitions ::=
    { START ("<start_date_value>") END ("<end_date_value>") EVERY ( INTERVAL <N> <time_unit> )
    | START ("<start_integer_value>") END ("<end_integer_value>") EVERY ( <granularity> ) } -- The partition column values still need to be enclosed in double quotes even if the partition column values are integers. However, the interval values in the EVERY clause do not need to be enclosed in double quotes.
```

Notes for `multi_range_partitions`:

- It only applies to Range Partitioning.
- The parameters involved is consistent with those in [ADD PARTITION(S)](#add-partitions).
- It only supports partitions with a single Partition Key.

- Drop partitions with Common Partition Expression (Supported from v3.4.1):

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP PARTITIONS WHERE <expr>
```

From v3.4.1 onwards, StarRocks supports dropping partitions using Common Partition Expression. You can specify a WHERE clause with an expression to filter the partitions to drop.
- The expression declares the partitions to be dropped. Partitions that meet the condition in the expression will be dropped in batch. Be cautious when proceeding.
- The expression can only contain partition columns and constants. Non-partition columns are not supported.
- Common Partition Expression applies to List partitions and Range partitions differently:
  - For tables with List partitions, StarRocks supports deleting partitions filtered by the Common Partition Expression.
  - For tables with Range partitions, StarRocks can only filter and delete partitions using the partition pruning capability of FE. Partitions correspond to predicates that are not supported by partition pruning cannot be filtered and deleted.

Example:

```sql
-- Drop the data earlier than the last three months. Column `dt` is the partition column of the table.
ALTER TABLE t1 DROP PARTITIONS WHERE dt < CURRENT_DATE() - INTERVAL 3 MONTH;
```

:::note

- Keep at least one partition for partitioned tables.
- If FORCE is not specified, you can recover the dropped partitions by using the [RECOVER](../backup_restore/RECOVER.md) command within a specified period (1 day by default).
- If FORCE is specified, the partitions will be deleted directly regardless of whether there are any unfinished operations on the partitions, and they cannot be recovered. Thus, generally, this operation is not recommended.

:::

#### Add a temporary partition

Syntax:

```sql
ALTER TABLE [<db_name>.]<tbl_name> 
ADD TEMPORARY PARTITION [IF NOT EXISTS] <partition_name>
partition_desc ["key"="value"]
[DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]]
```

#### Use a temporary partition to replace the current partition

Syntax:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
REPLACE PARTITION <partition_name>
partition_desc ["key"="value"]
WITH TEMPORARY PARTITION
partition_desc ["key"="value"]
[PROPERTIES ("key"="value", ...)]
```

#### Drop a temporary partition

Syntax:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP TEMPORARY PARTITION <partition_name>
```

#### Modify partition properties

**Syntax**

```sql
ALTER TABLE [<db_name>.]<tbl_name>
MODIFY PARTITION { <partition_name> | ( <partition1_name> [, <partition2_name> ...] ) | (*) }
SET ("key" = "value", ...);
```

**Usages**

- The following properties of a partition can be modified:

  - storage_medium
  - storage_cooldown_ttl or storage_cooldown_time
  - replication_num

- For the table that has only one partition, the partition name is the same as the table name. If the table is divided into multiple partitions, you can use `(*)`to modify the properties of all partitions, which is more convenient.

- Execute `SHOW PARTITIONS FROM <tbl_name>` to view the partition properties after modification.

### Modify the bucketing method and number of buckets (from v3.2)

Syntax:

```SQL
ALTER TABLE [<db_name>.]<table_name>
[ partition_names ]
[ distribution_desc ]

partition_names ::= 
    (PARTITION | PARTITIONS) ( <partition_name> [, <partition_name> ...] )

distribution_desc ::=
    DISTRIBUTED BY RANDOM [ BUCKETS <num> ] |
    DISTRIBUTED BY HASH ( <column_name> [, <column_name> ...] ) [ BUCKETS <num> ]
```

Example:

For example, the original table is a Duplicate Key table where hash bucketing is used and the number of buckets is automatically set by StarRocks.

```SQL
CREATE TABLE IF NOT EXISTS details (
    event_time DATETIME NOT NULL COMMENT "datetime of event",
    event_type INT NOT NULL COMMENT "type of event",
    user_id INT COMMENT "id of user",
    device_code INT COMMENT "device code",
    channel INT COMMENT ""
)
DUPLICATE KEY(event_time, event_type)
PARTITION BY date_trunc('day', event_time)
DISTRIBUTED BY HASH(user_id);

-- Insert data of several days
INSERT INTO details (event_time, event_type, user_id, device_code, channel) VALUES
-- Data of November 26th
('2023-11-26 08:00:00', 1, 101, 12345, 2),
('2023-11-26 09:15:00', 2, 102, 54321, 3),
('2023-11-26 10:30:00', 1, 103, 98765, 1),
-- Data of November 27th
('2023-11-27 08:30:00', 1, 104, 11111, 2),
('2023-11-27 09:45:00', 2, 105, 22222, 3),
('2023-11-27 11:00:00', 1, 106, 33333, 1),
-- Data of November 28th
('2023-11-28 08:00:00', 1, 107, 44444, 2),
('2023-11-28 09:15:00', 2, 108, 55555, 3),
('2023-11-28 10:30:00', 1, 109, 66666, 1);
```

#### Modify the bucketing method only

> **NOTICE**
>
> - The modification is applied to all partitions in the table and cannot be applied to specific partitions only.
> - Although only the bucketing method needs to be modified, the number of buckets still needs to be specified in the command using `BUCKETS <num>`. If `BUCKETS <num>` is not specified, it means that the number of buckets is automatically determined by StarRocks.

- The bucketing method is modified to random bucketing from hash bucketing and the number of buckets remains automatically set by StarRocks.

  ```SQL
  ALTER TABLE details DISTRIBUTED BY RANDOM;
  ```

- The keys for hash bucketing are modified to `user_id, event_time` from `event_time, event_type`. And the number of buckets remains automatically set by StarRocks.

  ```SQL
  ALTER TABLE details DISTRIBUTED BY HASH(user_id, event_time);
  ```

#### Modify the number of buckets only

> **NOTICE**
>
> Although only the number of buckets needs to be modified, the bucketing method still needs to be specified in the command, for example, `HASH(user_id)`.

- Modify the number of buckets for all partitions to 10 from being automatically set by StarRocks.

  ```SQL
  ALTER TABLE details DISTRIBUTED BY HASH(user_id) BUCKETS 10;
  ```

- Modify the number of buckets for specified partitions to 15 from being automatically set by StarRocks.

  ```SQL
  ALTER TABLE details PARTITIONS (p20231127, p20231128) DISTRIBUTED BY HASH(user_id) BUCKETS 15 ;
  ```

  > **NOTE**
  >
  > Partition names can be viewed by executing `SHOW PARTITIONS FROM <table_name>;`.

#### Modify both the bucketing method and the number of buckets

> **NOTICE**
>
> The modification is applied to all partitions in the table and cannot be applied to specific partitions only.

- Modify the bucketing method from hash bucketing to random bucketing, and change the number of buckets to 10 from being automatically set by StarRocks.

   ```SQL
   ALTER TABLE details DISTRIBUTED BY RANDOM BUCKETS 10;
   ```

- Modify the key for hash bucketing, and change the number of buckets to 10 from being automatically set by StarRocks. The key used for hashing bucketing is modified to `user_id, event_time` from the original `event_time, event_type`. The number of buckets is modified to 10 from automatically set by StarRocks.

  ```SQL
  ALTER TABLE details DISTRIBUTED BY HASH(user_id, event_time) BUCKETS 10;
  ``

### Modify columns (add/delete columns, change the order of columns)

#### Add a column to the specified location of the specified index

Syntax:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
ADD COLUMN column_name column_type [KEY | agg_type] [DEFAULT "default_value"]
[AFTER column_name|FIRST]
[TO rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

Note:

1. If you add a value column to an Aggregate table, you need to specify agg_type.
2. If you add a key column to a non-Aggregate table (such as a Duplicate Key table), you need to specify the KEY keyword.
3. You cannot add a column that already exists in the base index to the rollup index. (You can recreate a rollup index if needed.)

#### Add multiple columns to specified index

Syntax:

- Add multiple columns

  ```sql
  ALTER TABLE [<db_name>.]<tbl_name>
  ADD COLUMN (column_name1 column_type [KEY | agg_type] DEFAULT "default_value", ...)
  [TO rollup_index_name]
  [PROPERTIES ("key"="value", ...)]
  ```

- Add multiple columns and use AFTER to specify locations of the added columns

  ```sql
  ALTER TABLE [<db_name>.]<tbl_name>
  ADD COLUMN column_name1 column_type [KEY | agg_type] DEFAULT "default_value" AFTER column_name,
  ADD COLUMN column_name2 column_type [KEY | agg_type] DEFAULT "default_value" AFTER column_name
  [, ...]
  [TO rollup_index_name]
  [PROPERTIES ("key"="value", ...)]
  ```

Note:

1. If you add a value column to an Aggregate table, you need to specify `agg_type`.

2. If you add a key column to a non-Aggregate table, you need to specify the KEY keyword.

3. You cannot add a column that already exists in the base index to the rollup index. (You can create another rollup index if needed.)

#### Add a generated column (from v3.1)

Syntax:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
ADD COLUMN col_name data_type [NULL] AS generation_expr [COMMENT 'string']
```

You can add a generated column and specify its expression. [The generated column](../generated_columns.md) can be used to precompute and store the results of expressions, which significantly accelerates queries with the same complex expressions. Since v3.1, StarRocks supports generated columns.

#### Drop a column from specified index

Syntax:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP COLUMN column_name
[FROM rollup_index_name];
```

Note:

1. You cannot drop partition column.
2. If the column is dropped from the base index, it will also be dropped if it is included in the rollup index.

#### Modify the column type and column position of specified index

Syntax:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
MODIFY COLUMN column_name column_type [KEY | agg_type] [NULL | NOT NULL] [DEFAULT "default_value"]
[AFTER column_name|FIRST]
[FROM rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

Note:

1. If you modify the value column in aggregation models, you need to specify agg_type.
2. If you modify the key column in non-aggregation models, you need to specify the KEY keyword.
3. Only the type of column can be modified. The other properties of the column remain as they are currently. (i.e. other properties need to be explicitly written in the statement according to the original property, see example 8 in the [column](#column) part).
4. The partition column cannot be modified.
5. The following types of conversions are currently supported (accuracy loss is guaranteed by the user).

   - Convert TINYINT/SMALLINT/INT/BIGINT to TINYINT/SMALLINT/INT/BIGINT/DOUBLE.
   - Convert TINYINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE/DECIMAL to VARCHAR. VARCHAR supports modification of maximum length.
   - Convert VARCHAR to TINYINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE.
   - Convert VARCHAR to DATE (currently support six formats: "%Y-%m-%d", "%y-%m-%d", "%Y%m%d", "%y%m%d", "%Y/%m/%d, "%y/%m/%d")
   - Convert DATETIME to DATE(only year-month-day information is retained, i.e.  `2019-12-09 21:47:05` `<-->` `2019-12-09`)
   - Convert DATE to DATETIME (set hour, minute, second to zero, For example: `2019-12-09` `<-->` `2019-12-09 00:00:00`)
   - Convert FLOAT to DOUBLE
   - Convert INT to DATE (If the INT data fails to convert, the original data remains the same)

6. Conversion from NULL to NOT NULL is not supported.

#### Reorder the columns of specified index

Syntax:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
ORDER BY (column_name1, column_name2, ...)
[FROM rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

Note:

- All columns in the index must be written.
- The value column is listed after the key column.

#### Modify the sort key

Since v3.0, the sort keys for the Primary Key tables can be modified. v3.3 extends this support to Duplicate Key tables, Aggregate tables, and Unique Key tables.

The sort keys in Duplicate Key tables and Primary Key tables can be combination of any sort columns. The sort keys in Aggregate tables and Unique Key tables must include all key columns, but the order of the columns does not need to be same as the key columns.

Syntax:

```SQL
ALTER TABLE [<db_name>.]<table_name>
[ order_desc ]

order_desc ::=
    ORDER BY <column_name> [, <column_name> ...]
```

Example: modify the sort keys in Primary Key tables.

For example, the original table is a Primary Key table where the sort key and the primary key are coupled, which is `dt, order_id`.

```SQL
create table orders (
    dt date NOT NULL,
    order_id bigint NOT NULL,
    user_id int NOT NULL,
    merchant_id int NOT NULL,
    good_id int NOT NULL,
    good_name string NOT NULL,
    price int NOT NULL,
    cnt int NOT NULL,
    revenue int NOT NULL,
    state tinyint NOT NULL
) PRIMARY KEY (dt, order_id)
PARTITION BY date_trunc('day', dt)
DISTRIBUTED BY HASH(order_id);
```

Decouple the sort key from the primary key, and modify the sort key to `dt, revenue, state`.

```SQL
ALTER TABLE orders ORDER BY (dt, revenue, state);
```

#### Modify a STRUCT column to add or drop a field

From v3.2.10 and v3.3.2 onwards, StarRocks supports modifying a STRUCT column to add or drop a field, which can be nested or within an ARRAY type.

Syntax:

```sql
-- Add a field
ALTER TABLE [<db_name>.]<tbl_name> MODIFY COLUMN <column_name>
ADD FIELD field_path field_desc

-- Drop a field
ALTER TABLE [<db_name>.]<tbl_name> MODIFY COLUMN <column_name>
DROP FIELD field_path

field_path ::= [ { <field_name>. | [*]. } [ ... ] ]<field_name>

  -- Note that here `[*]` as a whole is a pre-defined symbol and represents all elements in the ARRAY field 
  -- when adding or removing a field in a STRUCT type nested within an ARRAY type.
  -- For detailed information, see the parameter description and examples of `field_path`.

field_desc ::= <field_type> [ AFTER <prior_field_name> | FIRST ]
```

Parameters:

- `field_path`: The field to be added or removed. This can be a simple field name, indicating a top-dimension field, for example, `new_field_name`, or a Column Access Path that represents a nested field, for example, `lv1_k1.lv2_k2.[*].new_field_name`.
- `[*]`: When a STRUCT type is nested within an ARRAY type, `[*]` represents all elements in the ARRAY field. It is used to add or remove a field in all STRUCT elements nested under the ARRAY field.
- `prior_field_name`: The field preceding the newly added field. Used in conjunction with the AFTER keyword to specify the order of the new field. You do not need to specify this parameter if the FIRST keyword is used, indicating the new field should be the first field. The dimension of `prior_field_name` is determined by `field_path` (specifically, the part preceding `new_field_name`, that is, `level1_k1.level2_k2.[*]`) and does not need to be specified explicitly.

Examples of `field_path`:

- Add or drop a sub-field in a STRUCT field nested within a STRUCT column.

  Suppose there is a column `fx stuct<c1 int, c2 struct <v1 int, v2 int>>`. The syntax to add a `v3` field under `c2` is:

  ```SQL
  ALTER TABLE tbl MODIFY COLUMN fx ADD FIELD c2.v3 INT
  ```

  After the operation, the column becomes `fx stuct<c1 int, c2 struct <v1 int, v2 int, v3 int>>`.

- Add or drop a sub-field in every STRUCT field nested within a ARRAY field.

  Suppose there is a column `fx struct<c1 int, c2 array<struct <v1 int, v2 int>>>`. The field `c2` is an ARRAY type, which contains a STRUCT with two fields `v1` and `v2`. The syntax to add a `v3` field to the nested STRUCT is:

  ```SQL
  ALTER TABLE tbl MODIFY COLUMN fx ADD FIELD c2.[*].v3 INT
  ```

  After the operation, the column becomes `fx struct<c1 int, c2 array<struct <v1 int, v2 int, v3 int>>>`.

For more usage instructions, see [Example - Column -14](#column).

:::note

- Currently, this feature is only supported in shared-nothing clusters.
- The table must have the `fast_schema_evolution` property enabled.
- Adding or dropping fields in the STRUCT type within a MAP type is not supported.
- Newly added fields cannot have default values or attributes such as Nullable specified. They default to being Nullable, with a default value of null.
- After this feature is used, downgrading the cluster directly to a version that does not support this feature is not allowed.

:::


### Modify rollup index

#### Create a rollup index

Syntax:

```SQL
ALTER TABLE [<db_name>.]<tbl_name> 
ADD ROLLUP rollup_name (column_name1, column_name2, ...)
[FROM from_index_name]
[PROPERTIES ("key"="value", ...)]
```

PROPERTIES: Support setting timeout time and the default timeout time is one day.

Example:

```SQL
ALTER TABLE [<db_name>.]<tbl_name> 
ADD ROLLUP r1(col1,col2) from r0;
```

#### Create rollup indexes in batches

Syntax:

```SQL
ALTER TABLE [<db_name>.]<tbl_name>
ADD ROLLUP [rollup_name (column_name1, column_name2, ...)
[FROM from_index_name]
[PROPERTIES ("key"="value", ...)],...];
```

Example:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
ADD ROLLUP r1(col1,col2) from r0, r2(col3,col4) from r0;
```

Note:

1. If from_index_name is not specified, then create from base index by default.
2. The columns in the rollup table must be existing columns in from_index.
3. In properties, user can specify the storage format. See CREATE TABLE for details.

#### Drop a rollup index

Syntax:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP ROLLUP rollup_name [PROPERTIES ("key"="value", ...)];
```

Example:

```sql
ALTER TABLE [<db_name>.]<tbl_name> DROP ROLLUP r1;
```

#### Batch drop rollup indexes

Syntax:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP ROLLUP [rollup_name [PROPERTIES ("key"="value", ...)],...];
```

Example:

```sql
ALTER TABLE [<db_name>.]<tbl_name> DROP ROLLUP r1, r2;
```

Note: You cannot drop the base index.

### Modify bitmap indexes

Bitmap index supports the following modifications:

#### Create a bitmap index

Syntax:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
ADD INDEX index_name (column [, ...],) [USING BITMAP] [COMMENT 'balabala'];
```

Note:

```plain text
1. Bitmap index is only supported for the current version.
2. A BITMAP index is created only in a single column.
```

#### Drop an bitmap index

Syntax:

```sql
DROP INDEX index_name;
```

### Modify table properties

Syntax:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
SET ("key" = "value")
```

Currently, StarRocks supports modifying the following table properties:

- `replication_num`
- `default.replication_num`
- `storage_cooldown_ttl`
- `storage_cooldown_time`
- Dynamic partitioning related properties
- `enable_persistent_index`
- `bloom_filter_columns`
- `colocate_with`
- `bucket_size` (supported since 3.2)
- `base_compaction_forbidden_time_ranges` (supported since v3.2.13)

:::note

- In most cases, you are only allowed to modify one property at a time. You can only modify multiple properties at a time only if these properties have the same prefix. Currently, only `dynamic_partition.` and `binlog.` are supported.
- You can also modify the properties by merging into the above operation on column. See the [following examples](#examples).

:::

### Swap

Swap supports atomic exchange of two tables.

Syntax:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
SWAP WITH <tbl_name>;
```

:::note
- Unique Key and Foreign Key constraints between OLAP tables will be validated during Swap to ensure that the constraints of the two tables being swapped are consistent. An error will be returned if inconsistencies are detected. If no inconsistencies are detected, Unique Key and Foreign Key constraints will be automatically swapped.
- Materialized views that are depended on the tables being swapped will be automatically set to inactive, and their Unique Key and Foreign Key constraints will be removed and no longer available.
:::

### Manual compaction (from 3.1)

StarRocks uses a compaction mechanism to merge different versions of loaded data. This feature can combine small files into large files, which effectively improves query performance.

Before v3.1, compaction is performed in two ways:

- Automatic compaction by system: Compaction is performed at the BE level in the background. Users cannot specify database or table for compaction.
- Users can perform compaction by calling an HTTP interface.

Starting from v3.1, StarRocks offers a SQL interface for users to manually perform compaction by running SQL commands. They can choose a specific table or partition for compaction. This provides more flexibility and control over the compaction process.

Shared-data clusters support this feature from v3.3.0 onwards.

> **NOTE**
>
> From v3.2.13 onwards, you can forbid Base Compaction within certain time range using the property [`base_compaction_forbidden_time_ranges`](./CREATE_TABLE.md#forbid-base-compaction).

Syntax:

```SQL
ALTER TABLE <tbl_name> [ BASE | CUMULATIVE ] COMPACT [ <partition_name> | ( <partition1_name> [, <partition2_name> ...] ) ]
```

That is:

```SQL
-- Perform compaction on the entire table.
ALTER TABLE <tbl_name> COMPACT

-- Perform compaction on a single partition.
ALTER TABLE <tbl_name> COMPACT <partition_name>

-- Perform compaction on multiple partitions.
ALTER TABLE <tbl_name> COMPACT (<partition1_name>[,<partition2_name>,...])

-- Perform cumulative compaction.
ALTER TABLE <tbl_name> CUMULATIVE COMPACT (<partition1_name>[,<partition2_name>,...])

-- Perform base compaction.
ALTER TABLE <tbl_name> BASE COMPACT (<partition1_name>[,<partition2_name>,...])
```

The `be_compactions` table in the `information_schema` database records compaction results. You can run `SELECT * FROM information_schema.be_compactions;` to query data versions after compaction.

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

3. Modify data writing and replication mode among replicas.

    ```sql
    ALTER TABLE example_db.my_table
    SET ("replicated_storage" = "false");
    ```

   This example sets the data writing and replication mode among replicas to "leaderless replication", which means data is written to multiple replicas at the same time without differentiating primary and secondary replicas. For more information, see the `replicated_storage` parameter in [CREATE TABLE](CREATE_TABLE.md).

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
    DISTRIBUTED BY HASH(k1);
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

5. Batch alter the number of replicas for specified partitions.

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY PARTITION (p1, p2, p4) SET("replication_num"="1");
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

### Rollup index

1. Create an rollup index `example_rollup_index` based on the base index (k1,k2,k3,v1,v2). Column-based storage is used.

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

### Column

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

6. Add multiple columns to `example_rollup_index` (aggregate) and specify the locations of the added columns using `AFTER`.

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN col1 INT DEFAULT "1" AFTER `k1`,
    ADD COLUMN col2 FLOAT SUM AFTER `v2`,
    TO example_rollup_index;
    ```

7. Drop a column from `example_rollup_index`.

    ```sql
    ALTER TABLE example_db.my_table
    DROP COLUMN col2
    FROM example_rollup_index;
    ```

8. Modify the column type of col1 of the base index to BIGINT and put it after `col2`.

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY COLUMN col1 BIGINT DEFAULT "1" AFTER col2;
    ```

9. Modify the maximum length of the `val1` column of the base index to 64. The original length is 32.

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY COLUMN val1 VARCHAR(64) REPLACE DEFAULT "abc";
    ```

10. Reorder the columns in `example_rollup_index`. The original column order is k1, k2, k3, v1, v2.

    ```sql
    ALTER TABLE example_db.my_table
    ORDER BY (k3,k1,k2,v2,v1)
    FROM example_rollup_index;
    ```

11. Perform two operations (ADD COLUMN and ORDER BY) at one time.

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN v2 INT MAX DEFAULT "0" AFTER k2 TO example_rollup_index,
    ORDER BY (k3,k1,k2,v2,v1) FROM example_rollup_index;
    ```

12. Alter the bloomfilter columns of the table.

     ```sql
     ALTER TABLE example_db.my_table
     SET ("bloom_filter_columns"="k1,k2,k3");
     ```

     This operation can also be merged into the above column operation (note that the syntax of multiple clauses is slightly different).

     ```sql
     ALTER TABLE example_db.my_table
     DROP COLUMN col2
     PROPERTIES ("bloom_filter_columns"="k1,k2,k3");
     ```

13. Modify the data type of multiple columns in a single statement.

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY COLUMN k1 VARCHAR(100) KEY NOT NULL,
    MODIFY COLUMN v2 DOUBLE DEFAULT "1" AFTER v1;
    ```

14. Add and drop fields in STRUCT-type data.

    **Prerequisites**: Create a table and insert a row of data.

    ```sql
    CREATE TABLE struct_test(
        c0 INT,
        c1 STRUCT<v1 INT, v2 STRUCT<v4 INT, v5 INT>, v3 INT>,
        c2 STRUCT<v1 INT, v2 ARRAY<STRUCT<v3 INT, v4 STRUCT<v5 INT, v6 INT>>>>
    )
    DUPLICATE KEY(c0)
    DISTRIBUTED BY HASH(`c0`) BUCKETS 1
    PROPERTIES (
        "fast_schema_evolution" = "true"
    );
    INSERT INTO struct_test VALUES (
        1, 
        ROW(1, ROW(2, 3), 4), 
        ROW(5, [ROW(6, ROW(7, 8)), ROW(9, ROW(10, 11))])
    );
    ```

    ```plain
    mysql> SELECT * FROM struct_test\G
    *************************** 1. row ***************************
    c0: 1
    c1: {"v1":1,"v2":{"v4":2,"v5":3},"v3":4}
    c2: {"v1":5,"v2":[{"v3":6,"v4":{"v5":7,"v6":8}},{"v3":9,"v4":{"v5":10,"v6":11}}]}
    ```

    - Add a new field to a STRUCT-type column.

    ```sql
    ALTER TABLE struct_test MODIFY COLUMN c1 ADD FIELD v4 INT AFTER v2;
    ```

    ```plain
    mysql> SELECT * FROM struct_test\G
    *************************** 1. row ***************************
    c0: 1
    c1: {"v1":1,"v2":{"v4":2,"v5":3},"v4":null,"v3":4}
    c2: {"v1":5,"v2":[{"v3":6,"v4":{"v5":7,"v6":8}},{"v3":9,"v4":{"v5":10,"v6":11}}]}
    ```

    - Add a new field to a nested STRUCT type.

    ```sql
    ALTER TABLE struct_test MODIFY COLUMN c1 ADD FIELD v2.v6 INT FIRST;
    ```

    ```plain
    mysql> SELECT * FROM struct_test\G
    *************************** 1. row ***************************
    c0: 1
    c1: {"v1":1,"v2":{"v6":null,"v4":2,"v5":3},"v4":null,"v3":4}
    c2: {"v1":5,"v2":[{"v3":6,"v4":{"v5":7,"v6":8}},{"v3":9,"v4":{"v5":10,"v6":11}}]}
    ```

    - Add a new field to a STRUCT type in an array.

    ```sql
    ALTER TABLE struct_test MODIFY COLUMN c2 ADD FIELD v2.[*].v7 INT AFTER v3;
    ```

    ```plain
    mysql> SELECT * FROM struct_test\G
    *************************** 1. row ***************************
    c0: 1
    c1: {"v1":1,"v2":{"v6":null,"v4":2,"v5":3},"v4":null,"v3":4}
    c2: {"v1":5,"v2":[{"v3":6,"v7":null,"v4":{"v5":7,"v6":8}},{"v3":9,"v7":null,"v4":{"v5":10,"v6":11}}]}
    ```

    - Drop a field from a STRUCT-type column.

    ```sql
    ALTER TABLE struct_test MODIFY COLUMN c1 DROP FIELD v3;
    ```

    ```plain
    mysql> SELECT * FROM struct_test\G
    *************************** 1. row ***************************
    c0: 1
    c1: {"v1":1,"v2":{"v6":null,"v4":2,"v5":3},"v4":null}
    c2: {"v1":5,"v2":[{"v3":6,"v7":null,"v4":{"v5":7,"v6":8}},{"v3":9,"v7":null,"v4":{"v5":10,"v6":11}}]}
    ```

    - Drop a field from a nested STRUCT type.

    ```sql
    ALTER TABLE struct_test MODIFY COLUMN c1 DROP FIELD v2.v4;
    ```

    ```plain
    mysql> SELECT * FROM struct_test\G
    *************************** 1. row ***************************
    c0: 1
    c1: {"v1":1,"v2":{"v6":null,"v5":3},"v4":null}
    c2: {"v1":5,"v2":[{"v3":6,"v7":null,"v4":{"v5":7,"v6":8}},{"v3":9,"v7":null,"v4":{"v5":10,"v6":11}}]}
    ```

    - Drop a field from a STRUCT type in an array.

    ```sql
    ALTER TABLE struct_test MODIFY COLUMN c2 DROP FIELD v2.[*].v3;
    ```

    ```plain
    mysql> SELECT * FROM struct_test\G
    *************************** 1. row ***************************
    c0: 1
    c1: {"v1":1,"v2":{"v6":null,"v5":3},"v4":null}
    c2: {"v1":5,"v2":[{"v7":null,"v4":{"v5":7,"v6":8}},{"v7":null,"v4":{"v5":10,"v6":11}}]}
    ```

### Table property

1. Alter the Colocate property of the table.

     ```sql
     ALTER TABLE example_db.my_table
     SET ("colocate_with" = "t1");
     ```

2. Alter the dynamic partition property of the table.

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

### Bitmap index

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

Atomic swap between `table1` and `table2`.

```sql
ALTER TABLE table1 SWAP WITH table2
```

### Manual compaction

```sql
CREATE TABLE compaction_test( 
    event_day DATE,
    pv BIGINT)
DUPLICATE KEY(event_day)
PARTITION BY date_trunc('month', event_day)
DISTRIBUTED BY HASH(event_day) BUCKETS 8
PROPERTIES("replication_num" = "3");

INSERT INTO compaction_test VALUES
('2023-02-14', 2),
('2033-03-01',2);
{'label':'insert_734648fa-c878-11ed-90d6-00163e0dcbfc', 'status':'VISIBLE', 'txnId':'5008'}

INSERT INTO compaction_test VALUES
('2023-02-14', 2),('2033-03-01',2);
{'label':'insert_85c95c1b-c878-11ed-90d6-00163e0dcbfc', 'status':'VISIBLE', 'txnId':'5009'}

ALTER TABLE compaction_test COMPACT;

ALTER TABLE compaction_test COMPACT p203303;

ALTER TABLE compaction_test COMPACT (p202302,p203303);

ALTER TABLE compaction_test CUMULATIVE COMPACT (p202302,p203303);

ALTER TABLE compaction_test BASE COMPACT (p202302,p203303);
```

## References

- [CREATE TABLE](CREATE_TABLE.md)
- [SHOW CREATE TABLE](SHOW_CREATE_TABLE.md)
- [SHOW TABLES](SHOW_TABLES.md)
- [SHOW ALTER TABLE](SHOW_ALTER.md)
- [DROP TABLE](DROP_TABLE.md)

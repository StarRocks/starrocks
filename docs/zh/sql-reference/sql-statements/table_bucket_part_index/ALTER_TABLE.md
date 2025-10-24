---
displayed_sidebar: docs
---

# ALTER TABLE

import Beta from '../../../_assets/commonMarkdown/_beta.mdx'

## 描述

修改现有表，包括：

- [修改表名、分区名、索引名、列名](#rename-对名称进行修改)
- [修改表注释](#修改表的注释31-版本起)
- [修改分区（增删分区和修改分区属性）](#操作-partition-相关语法)
- [修改分桶方式和分桶数量](#修改分桶方式和分桶数量自-32-版本起)
- [修改列（增删列和修改列顺序和注释）](#修改列添加删除列改变列的顺序或注释)
- [创建或删除 rollup index](#操作-rollup-index-语法)
- [修改 bitmap index](#bitmap-index-修改)
- [修改表的属性](#修改表的属性)
- [对表进行原子替换](#swap-将两个表原子替换)
- [手动执行 Compaction 合并表数据](#手动-compaction31-版本起)
- [删除主键索引](#删除主键索引-339-版本起)

:::tip
此操作需要对目标表具有ALTER权限。
:::

## 语法

```SQL
ALTER TABLE [<db_name>.]<tbl_name>
alter_clause1[, alter_clause2, ...]
```

`alter_clause`可以包含以下操作：重命名、注释、分区、分桶、列、Rollup、索引、表属性、原子替换和 Compaction。

- rename: 修改表名、rollup 名、partition 名或列名（从 3.3.2 版本开始支持）。
- comment: 修改表的注释。**从 3.1 版本开始支持。**
- partition: 修改分区属性，删除分区，增加分区。
- bucket：修改分桶方式和分桶数量。
- column: 增加列，删除列，调整列顺序，修改列类型以及注释
- rollup: 创建或删除 Rollup。
- index: 修改索引。
- swap: 原子替换两张表。
- compaction: 对指定表或分区手动执行 Compaction（数据版本合并）。**从 3.1 版本开始支持。**
- drop persistent index: 存算分离下删除主键索引。**从 3.3.9 版本开始支持。**

## 限制和使用注意事项

- 在一个ALTER TABLE语句中不能同时对分区、列和 Rollup 进行操作。
- 一个表一次只能有一个正在进行的schema change操作。不能同时在一个表上运行两个schema change命令。
- 对分桶、列和 Rollup 的操作是异步操作。任务提交后会立即返回成功消息。可以运行[SHOW ALTER TABLE](SHOW_ALTER.md)命令检查进度，并运行[CANCEL ALTER TABLE](CANCEL_ALTER_TABLE.md)命令取消操作。
- 对重命名、注释、分区、索引和原子替换的操作是同步操作，命令返回表示执行已完成。

### 重命名

重命名支持修改表名、Rollup 名和分区名。

#### 重命名表

```sql
ALTER TABLE <tbl_name> RENAME <new_tbl_name>
```

#### 重命名 Rollup

```sql
ALTER TABLE [<db_name>.]<tbl_name>
RENAME ROLLUP <old_rollup_name> <new_rollup_name>
```

#### 重命名分区

```sql
ALTER TABLE [<db_name>.]<tbl_name>
RENAME PARTITION <old_partition_name> <new_partition_name>
```

#### 重命名列

从v3.3.2起，StarRocks支持重命名列。

```sql
ALTER TABLE [<db_name>.]<tbl_name>
RENAME COLUMN <old_col_name> [ TO ] <new_col_name>
```

:::note

- 将列从A重命名为B后，不支持添加名为A的新列。
- 基于重命名列构建的物化视图将失效。必须在具有新名称的列上重建它们。

:::

### 修改表注释（从v3.1起）

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name> COMMENT = "<new table comment>";
```

### 修改分区

#### 添加分区

您必须严格遵循相应的语法来添加 Range 分区或 List 分区。

:::note
- 不支持添加表达式分区。
- 请注意，尽管 `PARTITION BY date_trunc(column)` 和 `PARTITION BY time_slice(column)` 的格式为表达式分区，两者都属于属于 Range 分区。因此，您可以使用以下 Range 分区的语法，为采用此类分区策略的表添加新分区。
:::

语法：

- Range分区

    ```SQL
    ALTER TABLE
        ADD { single_range_partition | multi_range_partitions } [distribution_desc] ["key"="value"];

    single_range_partition ::=
        PARTITION [IF NOT EXISTS] <partition_name> VALUES partition_key_desc

    partition_key_desc ::=
        { LESS THAN { MAXVALUE | value_list }
        | [ value_list , value_list ) } -- 注意，[ 表示左闭区间。

    value_list ::=
        ( <value> [, ...] )

    multi_range_partitions ::=
        { PARTITIONS START ("<start_date_value>") END ("<end_date_value>") EVERY ( INTERVAL <N> <time_unit> )
        | PARTITIONS START ("<start_integer_value>") END ("<end_integer_value>") EVERY ( <granularity> ) } -- 即使START和END指定的分区列值是整数，分区列值仍需用双引号括起来。但EVERY子句中的间隔值不需要用双引号括起来。
    ```

- List分区

    ```SQL
    ALTER TABLE
        ADD PARTITION <partition_name> VALUES IN (value_list) [distribution_desc] ["key"="value"];

    value_list ::=
        value_item [, ...]

    value_item ::=
        { <value> | ( <value> [, ...] ) }
    ```

参数：

- 分区相关参数：

  - 对于Range分区，可以添加单个Range分区（`single_range_partition`）或批量添加多个Range分区（`multi_range_partitions`）。
  - 对于List分区，只能添加单个List分区。

- `distribution_desc`：

   可以为新分区单独设置桶的数量，但不能单独设置分桶方法。

- `"key"="value"`：

   可以为新分区设置属性。详情请参见[CREATE TABLE](CREATE_TABLE.md#properties)。

示例：

- Range分区

  - 如果在创建表时指定了分区列为`event_day`，例如`PARTITION BY RANGE(event_day)`，并且在创建表后需要添加新分区，可以执行：

    ```sql
    ALTER TABLE site_access ADD PARTITION p4 VALUES LESS THAN ("2020-04-30");
    ```

  - 如果在创建表时指定了分区列为`datekey`，例如`PARTITION BY RANGE (datekey)`，并且在创建表后需要批量添加多个分区，可以执行：

    ```sql
    ALTER TABLE site_access
        ADD PARTITIONS START ("2021-01-05") END ("2021-01-10") EVERY (INTERVAL 1 DAY);
    ```

- List分区

  - 如果在创建表时指定了单个分区列，例如`PARTITION BY LIST (city)`，并且在创建表后需要添加新分区，可以执行：

    ```sql
    ALTER TABLE t_recharge_detail2
    ADD PARTITION pCalifornia VALUES IN ("Los Angeles","San Francisco","San Diego");
    ```

  - 如果在创建表时指定了多个分区列，例如`PARTITION BY LIST (dt,city)`，并且在创建表后需要添加新分区，可以执行：

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

#### 删除分区

- 删除单个分区：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP PARTITION [ IF EXISTS ] <partition_name> [ FORCE ]
```

- 批量删除分区（从v3.4.0起支持）：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP PARTITIONS [ IF EXISTS ]  { partition_name_list | multi_range_partitions } [ FORCE ]

partition_name_list ::= ( <partition_name> [, ... ] )

multi_range_partitions ::=
    { START ("<start_date_value>") END ("<end_date_value>") EVERY ( INTERVAL <N> <time_unit> )
    | START ("<start_integer_value>") END ("<end_integer_value>") EVERY ( <granularity> ) } -- 即使分区列值是整数，分区列值仍需用双引号括起来。但EVERY子句中的间隔值不需要用双引号括起来。
```

  `multi_range_partitions`的注意事项：

  - 仅适用于Range分区。
  - 涉及的参数与[添加分区](#添加分区)中的参数一致。
  - 仅支持具有单个分区键的分区。

- 使用通用分区表达式删除分区（从v3.5.0起支持）：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP PARTITIONS WHERE <expr>
```

从v3.5.0起，StarRocks支持使用通用分区表达式删除分区。可以使用带有表达式的WHERE子句来过滤要删除的分区。
- 表达式声明要删除的分区。符合表达式条件的分区将被批量删除。操作时请谨慎。
- 表达式只能包含分区列和常量。不支持非分区列。
- 通用分区表达式在List分区和Range分区中的应用不同：
  - 对于List分区的表，StarRocks支持通过通用分区表达式删除分区。
  - 对于Range分区的表，StarRocks只能使用FE的分区裁剪功能来过滤和删除分区。对于不支持分区裁剪的谓词对应的分区无法被过滤和删除。

示例：

```sql
-- 删除早于最近三个月的数据。列`dt`是表的分区列。
ALTER TABLE t1 DROP PARTITIONS WHERE dt < CURRENT_DATE() - INTERVAL 3 MONTH;
```

:::note

- 对于分区表，至少保留一个分区。
- 如果未指定FORCE，可以在指定时间内（默认为1天）使用[RECOVER](../backup_restore/RECOVER.md)命令恢复已删除的分区。
- 如果指定了FORCE，分区将被直接删除，无论分区上是否有未完成的操作，并且无法恢复。因此，通常不推荐此操作。

:::

#### 添加临时分区

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name> 
ADD TEMPORARY PARTITION [IF NOT EXISTS] <partition_name>
{ single_range_partition | multi_range_partitions | list_partitions }
[DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]]

-- 有关 single_range_partition 和 multi_range_partitions 的详细信息，请参阅本页面中的“添加分区”部分。

list_partitions::= 
    PARTITION <partition_name> VALUES IN (value_list)

value_list ::=
    value_item [, ...]

value_item ::=
    { <value> | ( <value> [, ...] ) }
```

#### 使用临时分区替换当前分区

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
REPLACE PARTITION <partition_name>
partition_desc ["key"="value"]
WITH TEMPORARY PARTITION
partition_desc ["key"="value"]
[PROPERTIES ("key"="value", ...)]
```

#### 删除临时分区

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP TEMPORARY PARTITION <partition_name>
```

#### 修改分区属性

**语法**

```sql
ALTER TABLE [<db_name>.]<tbl_name>
MODIFY PARTITION { <partition_name> | ( <partition1_name> [, <partition2_name> ...] ) | (*) }
SET ("key" = "value", ...);
```

**用法**

- 可以修改分区的以下属性：

  - 存储介质
  - storage_cooldown_ttl或storage_cooldown_time
  - 副本数量

- 对于只有一个分区的表，分区名与表名相同。如果表被划分为多个分区，可以使用`(*)`来修改所有分区的属性，这样更方便。

- 执行`SHOW PARTITIONS FROM <tbl_name>`查看修改后的分区属性。

### 修改分桶方法和桶的数量（从v3.2起）

语法：

```SQL
ALTER TABLE [<db_name>.]<table_name>
[ partition_names ]
[ distribution_desc ]

partition_names ::= 
    (PARTITION | PARTITIONS) ( <partition_name> [, <partition_name> ...] )

distribution_desc ::=
    DISTRIBUTED BY RANDOM [ BUCKETS <num> ] |
    DISTRIBUTED BY HASH ( <column_name> [, <column_name> ...] ) [ [ DEFAULT ] BUCKETS <num> ]
```

示例：

例如，原始表是一个明细表，使用哈希分桶，桶的数量由StarRocks自动设置。

```SQL
CREATE TABLE IF NOT EXISTS details (
    event_time DATETIME NOT NULL COMMENT "事件的日期时间",
    event_type INT NOT NULL COMMENT "事件类型",
    user_id INT COMMENT "用户ID",
    device_code INT COMMENT "设备代码",
    channel INT COMMENT ""
)
DUPLICATE KEY(event_time, event_type)
PARTITION BY date_trunc('day', event_time)
DISTRIBUTED BY HASH(user_id);

-- 插入几天的数据
INSERT INTO details (event_time, event_type, user_id, device_code, channel) VALUES
-- 11月26日的数据
('2023-11-26 08:00:00', 1, 101, 12345, 2),
('2023-11-26 09:15:00', 2, 102, 54321, 3),
('2023-11-26 10:30:00', 1, 103, 98765, 1),
-- 11月27日的数据
('2023-11-27 08:30:00', 1, 104, 11111, 2),
('2023-11-27 09:45:00', 2, 105, 22222, 3),
('2023-11-27 11:00:00', 1, 106, 33333, 1),
-- 11月28日的数据
('2023-11-28 08:00:00', 1, 107, 44444, 2),
('2023-11-28 09:15:00', 2, 108, 55555, 3),
('2023-11-28 10:30:00', 1, 109, 66666, 1);
```

#### 仅修改分桶方法

> **注意**
>
> - 修改适用于表中的所有分区，不能仅应用于特定分区。
> - 虽然只需要修改分桶方法，但仍需在命令中使用`BUCKETS <num>`指定桶的数量。如果未指定`BUCKETS <num>`，则表示桶的数量由StarRocks自动确定。

- 将分桶方法从哈希分桶修改为随机分桶，桶的数量仍由StarRocks自动设置。

  ```SQL
  ALTER TABLE details DISTRIBUTED BY RANDOM;
  ```

- 将哈希分桶的键从`event_time, event_type`修改为`user_id, event_time`。桶的数量仍由StarRocks自动设置。

  ```SQL
  ALTER TABLE details DISTRIBUTED BY HASH(user_id, event_time);
  ```

#### 仅修改桶的数量

> **注意**
>
> 虽然只需要修改桶的数量，但仍需在命令中指定分桶方法，例如`HASH(user_id)`。

- 将所有分区的桶的数量从StarRocks自动设置修改为10。

  ```SQL
  ALTER TABLE details DISTRIBUTED BY HASH(user_id) BUCKETS 10;
  ```

- 将表的默认桶数从由StarRocks自动设置的值修改为10，**同时保持现有分区的桶数不变**（该功能自v3.5.8及v4.0.1版本起支持）。

  ```SQL
  ALTER TABLE details DISTRIBUTED BY HASH(user_id) DEFAULT BUCKETS 10;
  ```

  > **注意**
  >
  > 不能同时指定 `partition_names` 和 `DEFAULT`。

- 将指定分区的桶的数量从StarRocks自动设置修改为15。

  ```SQL
  ALTER TABLE details PARTITIONS (p20231127, p20231128) DISTRIBUTED BY HASH(user_id) BUCKETS 15 ;
  ```

  > **注意**
  >
  > 可以通过执行`SHOW PARTITIONS FROM <table_name>;`查看分区名称。

#### 同时修改分桶方法和桶的数量

> **注意**
>
> 修改适用于表中的所有分区，不能仅应用于特定分区。

- 将分桶方法从哈希分桶修改为随机分桶，并将桶的数量从StarRocks自动设置修改为10。

   ```SQL
   ALTER TABLE details DISTRIBUTED BY RANDOM BUCKETS 10;
   ```

- 修改哈希分桶的键，并将桶的数量从StarRocks自动设置修改为10。用于哈希分桶的键从原来的`event_time, event_type`修改为`user_id, event_time`。桶的数量从StarRocks自动设置修改为10。

  ```SQL
  ALTER TABLE details DISTRIBUTED BY HASH(user_id, event_time) BUCKETS 10;
  ```

### 修改列（添加/删除列，改变列的顺序或注释）

#### 在指定索引的指定位置添加列

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
ADD COLUMN column_name column_type [KEY | agg_type] [DEFAULT "default_value"]
[AFTER column_name|FIRST]
[TO rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

注意：

1. 如果向聚合表中添加值列，需要指定agg_type。
2. 如果向非聚合表（如明细表）中添加键列，需要指定KEY关键字。
3. 不能将已经存在于基础索引中的列添加到 Rollup 中。（如有需要，可以重新创建 Rollup。）

#### 向指定索引添加多个列

语法：

- 添加多个列

  ```sql
  ALTER TABLE [<db_name>.]<tbl_name>
  ADD COLUMN (column_name1 column_type [KEY | agg_type] DEFAULT "default_value", ...)
  [TO rollup_index_name]
  [PROPERTIES ("key"="value", ...)]
  ```

- 添加多个列并使用AFTER指定添加列的位置

```sql
  ALTER TABLE [<db_name>.]<tbl_name>
  ADD COLUMN column_name1 column_type [KEY | agg_type] DEFAULT "default_value" AFTER column_name,
  ADD COLUMN column_name2 column_type [KEY | agg_type] DEFAULT "default_value" AFTER column_name
  [, ...]
  [TO rollup_index_name]
  [PROPERTIES ("key"="value", ...)]
  ```

注意：

1. 如果向聚合表中添加值列，需要指定`agg_type`。

2. 如果向非聚合表中添加键列，需要指定KEY关键字。

3. 不能将已经存在于基础索引中的列添加到 Rollup 中。（如有需要，可以创建另一个 Rollup。）

#### 添加生成列（从v3.1起）

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
ADD COLUMN col_name data_type [NULL] AS generation_expr [COMMENT 'string']
```

可以添加生成列并指定其表达式。[生成列](../generated_columns.md)可以用于预计算和存储表达式的结果，这显著加速了具有相同复杂表达式的查询。从v3.1起，StarRocks支持生成列。

#### 从指定索引中删除列

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP COLUMN column_name
[FROM rollup_index_name];
```

注意：

1. 不能删除分区列。
2. 如果从基础索引中删除列，并且该列包含在 Rollup 中，也会被删除。

#### 修改列类型、位置、注释和其他属性

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
MODIFY COLUMN <column_name> 
[ column_type [ KEY | agg_type ] ] [ NULL | NOT NULL ] 
[ DEFAULT "<default_value>"] [ COMMENT "<new_column_comment>" ]
[ AFTER <column_name> | FIRST ]
[ FROM rollup_index_name ]
[ PROPERTIES ("key"="value", ...) ]
```

注意：

1. 如果修改聚合模型中的值列，需要指定agg_type。
2. 如果修改非聚合模型中的键列，需要指定KEY关键字。
3. 只能修改列的类型。列的其他属性保持不变。（即其他属性需要在语句中根据原属性显式写出，参见[列](#column)部分的示例8）。
4. 不能修改分区列。
5. 目前支持以下类型的转换（精度损失由用户保证）。

   - 将TINYINT/SMALLINT/INT/BIGINT转换为TINYINT/SMALLINT/INT/BIGINT/DOUBLE。
   - 将TINYINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE/DECIMAL转换为VARCHAR。VARCHAR支持修改最大长度。
   - 将VARCHAR转换为TINYINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE。
   - 将VARCHAR转换为DATE（目前支持六种格式："%Y-%m-%d", "%y-%m-%d", "%Y%m%d", "%y%m%d", "%Y/%m/%d, "%y/%m/%d"）
   - 将DATETIME转换为DATE（仅保留年月日信息，即`2019-12-09 21:47:05` `<-->` `2019-12-09`）
   - 将DATE转换为DATETIME（将小时、分钟、秒设置为零，例如：`2019-12-09` `<-->` `2019-12-09 00:00:00`）
   - 将FLOAT转换为DOUBLE
   - 将INT转换为DATE（如果INT数据转换失败，原始数据保持不变）

6. 不支持从NULL转换为NOT NULL。
7. 您可以在单个 MODIFY COLUMN 子句中修改多个属性。但某些属性的组合不支持。

#### 重新排序指定索引的列

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
ORDER BY (column_name1, column_name2, ...)
[FROM rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

注意：

- 索引中的所有列必须写出。
- 值列列在键列之后。

#### 修改排序键

从v3.0起，可以修改主键表的排序键。v3.3扩展了对明细表、聚合表和更新表的支持。

明细表和主键表中的排序键可以是任意排序列的组合。聚合表和更新表中的排序键必须包含所有键列，但列的顺序不需要与键列相同。

语法：

```SQL
ALTER TABLE [<db_name>.]<table_name>
[ order_desc ]

order_desc ::=
    ORDER BY <column_name> [, <column_name> ...]
```

示例：修改主键表中的排序键。

例如，原始表是一个主键表，其中排序键和主键耦合，即`dt, order_id`。

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

将排序键与主键解耦，并将排序键修改为`dt, revenue, state`。

```SQL
ALTER TABLE orders ORDER BY (dt, revenue, state);
```

#### 修改STRUCT列以添加或删除字段

<Beta />

从v3.2.10和v3.3.2起，StarRocks支持修改STRUCT列以添加或删除字段，该字段可以是嵌套的或在ARRAY类型中。

语法：

```sql
-- 添加字段
ALTER TABLE [<db_name>.]<tbl_name> MODIFY COLUMN <column_name>
ADD FIELD field_path field_desc

-- 删除字段
ALTER TABLE [<db_name>.]<tbl_name> MODIFY COLUMN <column_name>
DROP FIELD field_path

field_path ::= [ { <field_name>. | [*]. } [ ... ] ]<field_name>

  -- 注意，这里的`[*]`作为一个整体是一个预定义符号，表示在ARRAY字段中添加或删除STRUCT类型的字段。
  -- 有关详细信息，请参见`field_path`的参数说明和示例。

field_desc ::= <field_type> [ AFTER <prior_field_name> | FIRST ]
```

参数：

- `field_path`：要添加或删除的字段。这可以是一个简单的字段名，表示顶层字段，例如`new_field_name`，或者是表示嵌套字段的列访问路径，例如`lv1_k1.lv2_k2.[*].new_field_name`。
- `[*]`：当STRUCT类型嵌套在ARRAY类型中时，`[*]`表示ARRAY字段中的所有元素。用于在ARRAY字段下的所有STRUCT元素中添加或删除字段。
- `prior_field_name`：新添加字段之前的字段。与AFTER关键字一起使用以指定新字段的顺序。如果使用FIRST关键字，则不需要指定此参数，表示新字段应为第一个字段。`prior_field_name`的维度由`field_path`确定（具体来说，是`new_field_name`之前的部分，即`level1_k1.level2_k2.[*]`），不需要显式指定。

`field_path`示例：

- 在嵌套在STRUCT列中的STRUCT字段中添加或删除子字段。

  假设有一个列`fx stuct<c1 int, c2 struct <v1 int, v2 int>>`。在`c2`下添加`v3`字段的语法是：

  ```SQL
  ALTER TABLE tbl MODIFY COLUMN fx ADD FIELD c2.v3 INT
  ```

  操作后，列变为`fx stuct<c1 int, c2 struct <v1 int, v2 int, v3 int>>`。

- 在嵌套在ARRAY字段中的每个STRUCT字段中添加或删除子字段。

  假设有一个列`fx struct<c1 int, c2 array<struct <v1 int, v2 int>>>`。字段`c2`是一个ARRAY类型，其中包含一个具有两个字段`v1`和`v2`的STRUCT。向嵌套的STRUCT中添加`v3`字段的语法是：

  ```SQL
  ALTER TABLE tbl MODIFY COLUMN fx ADD FIELD c2.[*].v3 INT
  ```

  操作后，列变为`fx struct<c1 int, c2 array<struct <v1 int, v2 int, v3 int>>>`。

有关更多用法说明，请参见[示例 - 列 -14](#column)。

:::note

- 目前，此功能仅在存算一体集群中支持。
- 表必须启用`fast_schema_evolution`属性。
- 不支持在 STRUCT 类型中修改一个 MAP 子字段的 Value 类型，不管该 Value 类型是 ARRAY、STRUCT 还是 MAP。
- 新添加的字段不能有默认值或可空等属性。它们默认为可空，默认值为null。
- 使用此功能后，不允许直接降级集群到不支持此功能的版本。

:::

### 修改 Rollup

#### 创建 Rollup

语法：

```SQL
ALTER TABLE [<db_name>.]<tbl_name> 
ADD ROLLUP rollup_name (column_name1, column_name2, ...)
[FROM from_index_name]
[PROPERTIES ("key"="value", ...)]
```

PROPERTIES：支持设置超时时间，默认超时时间为一天。

示例：

```SQL
ALTER TABLE [<db_name>.]<tbl_name> 
ADD ROLLUP r1(col1,col2) from r0;
```

#### 批量创建 Rollup

语法：

```SQL
ALTER TABLE [<db_name>.]<tbl_name>
ADD ROLLUP [rollup_name (column_name1, column_name2, ...)
[FROM from_index_name]
[PROPERTIES ("key"="value", ...)],...];
```

示例：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
ADD ROLLUP r1(col1,col2) from r0, r2(col3,col4) from r0;
```

注意：

1. 如果未指定from_index_name，则默认从基础索引创建。
2. 汇总表中的列必须是from_index中已存在的列。
3. 在属性中，用户可以指定存储格式。详情请参见CREATE TABLE。

#### 删除 Rollup

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP ROLLUP rollup_name [PROPERTIES ("key"="value", ...)];
```

示例：

```sql
ALTER TABLE [<db_name>.]<tbl_name> DROP ROLLUP r1;
```

#### 批量删除 Rollup

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP ROLLUP [rollup_name [PROPERTIES ("key"="value", ...)],...];
```

示例：

```sql
ALTER TABLE [<db_name>.]<tbl_name> DROP ROLLUP r1, r2;
```

注意：不能删除基础索引。

### 修改索引

您可以创建或删除以下索引：
- [Bitmap 索引](../../../table_design/indexes/Bitmap_index.md)
- [N-Gram bloom filter 索引](../../../table_design/indexes/Ngram_Bloom_Filter_Index.md)
- [全文倒排索引](../../../table_design/indexes/inverted_index.md)
- [向量索引](../../../table_design/indexes/vector_index.md)

#### 创建索引

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
ADD INDEX index_name (column [, ...],) [USING { BITMAP | NGRAMBF | GIN | VECTOR } ] [COMMENT '<comment>']
```

有关创建这些索引的详细说明和示例，请参阅上述对应的教程。

#### 删除索引

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP INDEX index_name;
```

### 修改表属性

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
SET ("key" = "value")
```

目前，StarRocks支持修改以下表属性：

- `replication_num`
- `default.replication_num`
- `default.storage_medium`
- 动态分区相关属性
- `enable_persistent_index`
- `bloom_filter_columns`
- `colocate_with`
- `bucket_size`（从3.2起支持）
- `base_compaction_forbidden_time_ranges`（从v3.2.13起支持）

:::note

- 在大多数情况下，只允许一次修改一个属性。只有在这些属性具有相同前缀时，才可以一次修改多个属性。目前，仅支持`dynamic_partition.`和`binlog.`。
- 还可以通过合并到上述列操作中来修改属性。请参见[以下示例](#examples)。

:::

### 原子替换

支持两个表的原子替换。

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
SWAP WITH <tbl_name>;
```

:::note
- OLAP表之间的唯一键和外键约束将在原子替换期间进行验证，以确保替换的两个表的约束一致。如果检测到不一致，将返回错误。如果未检测到不一致，唯一键和外键约束将自动替换。
- 依赖于被替换表的物化视图将自动设置为非活动状态，其唯一键和外键约束将被移除且不再可用。
:::

### 手动 Compaction（从3.1起）

StarRocks使用 Compaction 机制来合并已加载数据的不同版本。此功能可以将小文件合并为大文件，从而有效提高查询性能。

在v3.1之前，合并有两种方式：

- 系统自动 Compaction ：在BE级别后台执行 Compaction 。用户不能指定数据库或表进行 Compaction 。
- 用户可以通过调用 HTTP 接口执行 Compaction 。

从 v3.1 起，StarRocks提供了一个SQL接口，用户可以通过运行SQL命令手动执行 Compaction 。他们可以选择特定的表或分区进行 Compaction。这提供了对 Compaction 过程的更多灵活性和控制。

存算分离集群从v3.3.0起支持此功能。

> **注意**
>
> 从v3.2.13起，可以使用属性[`base_compaction_forbidden_time_ranges`](./CREATE_TABLE.md#forbid-base-compaction)在特定时间范围内禁止基础合并。

语法：

```SQL
ALTER TABLE <tbl_name> [ BASE | CUMULATIVE ] COMPACT [ <partition_name> | ( <partition1_name> [, <partition2_name> ...] ) ]
```

即：

```SQL
-- 对整个表执行 Compaction。
ALTER TABLE <tbl_name> COMPACT

-- 对单个分区执行 Compaction。
ALTER TABLE <tbl_name> COMPACT <partition_name>

-- 对多个分区执行 Compaction。
ALTER TABLE <tbl_name> COMPACT (<partition1_name>[,<partition2_name>,...])

-- 执行增量 Compaction。
ALTER TABLE <tbl_name> CUMULATIVE COMPACT (<partition1_name>[,<partition2_name>,...])

-- 执行 Base Compaction。
ALTER TABLE <tbl_name> BASE COMPACT (<partition1_name>[,<partition2_name>,...])
```

`information_schema`数据库中的`be_compactions`表记录 Compaction 结果。可以运行`SELECT * FROM information_schema.be_compactions;`查询 Compaction 后的数据版本。

### 删除主键索引 (3.3.9 版本起)

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP PERSISTENT INDEX ON TABLETS(<tablet_id>[, <tablet_id>, ...]);
```

> **说明**
>
> 只支持在存算分离集群中删除 CLOUD_NATIVE 类型的主键索引

## 示例

### 表

1. 修改表的默认副本数量，该数量用作新添加分区的默认副本数量。

    ```sql
    ALTER TABLE example_db.my_table
    SET ("default.replication_num" = "2");
    ```

2. 修改单分区表的实际副本数量。

    ```sql
    ALTER TABLE example_db.my_table
    SET ("replication_num" = "3");
    ```

3. 修改副本之间的数据写入和复制模式。

    ```sql
    ALTER TABLE example_db.my_table
    SET ("replicated_storage" = "false");
    ```

   此示例将副本之间的数据写入和复制模式设置为“无主复制”，即数据同时写入多个副本，而不区分主副本和从副本。有关更多信息，请参见[CREATE TABLE](CREATE_TABLE.md)中的`replicated_storage`参数。

### 分区

1. 添加分区并使用默认分桶模式。现有分区为[MIN, 2013-01-01)。添加的分区为[2013-01-01, 2014-01-01)。

    ```sql
    ALTER TABLE example_db.my_table
    ADD PARTITION p1 VALUES LESS THAN ("2014-01-01");
    ```

2. 添加分区并使用新的桶数量。

    ```sql
    ALTER TABLE example_db.my_table
    ADD PARTITION p1 VALUES LESS THAN ("2015-01-01")
    DISTRIBUTED BY HASH(k1);
    ```

3. 添加分区并使用新的副本数量。

    ```sql
    ALTER TABLE example_db.my_table
    ADD PARTITION p1 VALUES LESS THAN ("2015-01-01")
    ("replication_num"="1");
    ```

4. 修改分区的副本数量。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY PARTITION p1 SET("replication_num"="1");
    ```

5. 批量修改指定分区的副本数量。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY PARTITION (p1, p2, p4) SET("replication_num"="1");
    ```

6. 批量修改所有分区的存储介质。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY PARTITION (*) SET("storage_medium"="HDD");
    ```

7. 删除分区。

    ```sql
    ALTER TABLE example_db.my_table
    DROP PARTITION p1;
    ```

8. 添加具有上下边界的分区。

    ```sql
    ALTER TABLE example_db.my_table
    ADD PARTITION p1 VALUES [("2014-01-01"), ("2014-02-01"));
    ```

### Rollup

1. 基于基础索引（k1,k2,k3,v1,v2）创建 Rollup `example_rollup_index`。使用列存储。

    ```sql
    ALTER TABLE example_db.my_table
    ADD ROLLUP example_rollup_index(k1, k3, v1, v2)
    PROPERTIES("storage_type"="column");
    ```

2. 基于 `example_rollup_index(k1,k3,v1,v2)` 创建 Rollup `example_rollup_index2`。

    ```sql
    ALTER TABLE example_db.my_table
    ADD ROLLUP example_rollup_index2 (k1, v1)
    FROM example_rollup_index;
    ```

3. 基于基础 Rollup（k1, k2, k3, v1）创建 Rollup `example_rollup_index3`。超时时间设置为一小时。

    ```sql
    ALTER TABLE example_db.my_table
    ADD ROLLUP example_rollup_index3(k1, k3, v1)
    PROPERTIES("storage_type"="column", "timeout" = "3600");
    ```

4. 删除 Rollup `example_rollup_index2`。

    ```sql
    ALTER TABLE example_db.my_table
    DROP ROLLUP example_rollup_index2;
    ```

### 列

1. 在`example_rollup_index`的`col1`列之后添加一个键列`new_col`（非聚合列）。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT KEY DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

2. 在`example_rollup_index`的`col1`列之后添加一个值列`new_col`（非聚合列）。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

3. 在`example_rollup_index`的`col1`列之后添加一个键列`new_col`（聚合列）。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

4. 在`example_rollup_index`的`col1`列之后添加一个值列`new_col SUM`（聚合列）。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT SUM DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

5. 向`example_rollup_index`（聚合）添加多个列。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN (col1 INT DEFAULT "1", col2 FLOAT SUM DEFAULT "2.3")
    TO example_rollup_index;
    ```

6. 向`example_rollup_index`（聚合）添加多个列，并使用AFTER指定添加列的位置。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN col1 INT DEFAULT "1" AFTER `k1`,
    ADD COLUMN col2 FLOAT SUM AFTER `v2`,
    TO example_rollup_index;
    ```

7. 从`example_rollup_index`中删除列。

    ```sql
    ALTER TABLE example_db.my_table
    DROP COLUMN col2
    FROM example_rollup_index;
    ```

8. 将基础索引的col1列的列类型修改为BIGINT，并将其放在`col2`之后。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY COLUMN col1 BIGINT DEFAULT "1" AFTER col2;
    ```

9. 将基础索引的`val1`列的最大长度修改为64。原始长度为32。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY COLUMN val1 VARCHAR(64) REPLACE DEFAULT "abc";
    ```

10. 重新排序`example_rollup_index`中的列。原始列顺序为k1, k2, k3, v1, v2。

    ```sql
    ALTER TABLE example_db.my_table
    ORDER BY (k3,k1,k2,v2,v1)
    FROM example_rollup_index;
    ```

11. 一次执行两个操作（ADD COLUMN和ORDER BY）。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN v2 INT MAX DEFAULT "0" AFTER k2 TO example_rollup_index,
    ORDER BY (k3,k1,k2,v2,v1) FROM example_rollup_index;
    ```

12. 修改表的bloomfilter列。

     ```sql
     ALTER TABLE example_db.my_table
     SET ("bloom_filter_columns"="k1,k2,k3");
     ```

     此操作也可以合并到上述列操作中（注意多个子句的语法略有不同）。

     ```sql
     ALTER TABLE example_db.my_table
     DROP COLUMN col2
     PROPERTIES ("bloom_filter_columns"="k1,k2,k3");
     ```

13. 在单个语句中修改多个列的数据类型。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY COLUMN k1 VARCHAR(100) KEY NOT NULL,
    MODIFY COLUMN v2 DOUBLE DEFAULT "1" AFTER v1;
    ```

14. 在STRUCT类型数据中添加和删除字段。

    **前提条件**：创建一个表并插入一行数据。

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

    - 向STRUCT类型列添加新字段。

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

    - 向嵌套的STRUCT类型中添加新字段。

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

    - 向数组中的STRUCT类型添加新字段。

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

    - 从STRUCT类型列中删除字段。

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

    - 从嵌套的STRUCT类型中删除字段。

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

    - 从数组中的STRUCT类型中删除字段。

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

### 表属性

1. 修改表的Colocate属性。

     ```sql
     ALTER TABLE example_db.my_table
     SET ("colocate_with" = "t1");
     ```

2. 修改表的动态分区属性。

     ```sql
     ALTER TABLE example_db.my_table
     SET ("dynamic_partition.enable" = "false");
     ```

     如果需要向未配置动态分区属性的表添加动态分区属性，则需要指定所有动态分区属性。

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

3. 修改表的存储介质属性。

     ```sql
     ALTER TABLE example_db.my_table SET("default.storage_medium"="SSD");
     ```

### 重命名

1. 将`table1`重命名为`table2`。

    ```sql
    ALTER TABLE table1 RENAME table2;
    ```

2. 将`example_table`的 Rollup `rollup1`重命名为`rollup2`。

    ```sql
    ALTER TABLE example_table RENAME ROLLUP rollup1 rollup2;
    ```

3. 将`example_table`的分区`p1`重命名为`p2`。

    ```sql
    ALTER TABLE example_table RENAME PARTITION p1 p2;
    ```

### 索引

1. 为 `table1` 中的列 `siteid` 创建 Bitmap 索引。

    ```sql
    ALTER TABLE table1
    ADD INDEX index_1 (siteid) USING BITMAP COMMENT 'site_id_bitmap';
    ```

2. 删除 `table1` 中列 `siteid` 的 Bitmap 索引。

    ```sql
    ALTER TABLE table1
    DROP INDEX index_1;
    ```

### 原子替换

在`table1`和`table2`之间进行原子替换。

```sql
ALTER TABLE table1 SWAP WITH table2
```

### 手动 Compaction

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

### 删除主键索引

删除 `db1.test_tbl` 中 Tablet `100` 和 `101` 的主键索引 。

```sql
ALTER TABLE db1.test_tbl DROP PERSISTENT INDEX ON TABLETS (100, 101);
```

## 相关参考

- [CREATE TABLE](CREATE_TABLE.md)
- [SHOW CREATE TABLE](SHOW_CREATE_TABLE.md)
- [SHOW TABLES](SHOW_TABLES.md)
- [SHOW ALTER TABLE](SHOW_ALTER.md)
- [DROP TABLE](DROP_TABLE.md)
```

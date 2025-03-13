---
keywords: ['xiugai'] 
displayed_sidebar: docs
---

# ALTER TABLE

## 功能

该语句用于修改已有表，包括：

- [修改表名、分区名、索引名、列名](#rename-对名称进行修改)
- [修改表注释](#修改表的注释31-版本起)
- [修改分区（增删分区和修改分区属性）](#操作-partition-相关语法)
- [修改分桶方式和分桶数量](#修改分桶方式和分桶数量自-32-版本起)
- [修改列（增删列和修改列顺序）](#修改列增删列和修改列顺序)
- [创建或删除 rollup index](#操作-rollup-index-语法)
- [修改 bitmap index](#bitmap-index-修改)
- [修改表的属性](#修改表的属性)
- [对表进行原子替换](#swap-将两个表原子替换)
- [手动执行 compaction 合并表数据](#手动-compaction31-版本起)

:::tip
该操作需要有对应表的 ALTER 权限。
:::

## 语法

ALTER TABLE 语法格式如下：

```SQL
ALTER TABLE [<db_name>.]<tbl_name>
alter_clause1[, alter_clause2, ...]
```

其中 **alter_clause** 分为 rename、comment、partition、bucket、column、rollup index、bitmap index、table property、swap、compaction 相关修改操作：

- rename: 修改表名、rollup index 名、partition 名或列名（从 3.3.2 版本开始支持）。
- comment: 修改表的注释。**从 3.1 版本开始支持。**
- partition: 修改分区属性，删除分区，增加分区。
- bucket：修改分桶方式和分桶数量。
- column: 增加列，删除列，调整列顺序，修改列类型。*
- rollup index: 创建或删除 rollup index。
- bitmap index: 修改 bitmap index。
- swap: 原子替换两张表。
- compaction: 对指定表或分区手动执行 Compaction（数据版本合并）。**从 3.1 版本开始支持。**

## 使用限制和注意事项

- partition、column 和 rollup index <!--是否包含compaction，bucket和column/rollupindex可以在一起吗-->这些操作不能同时出现在一条 `ALTER TABLE` 语句中。
- 当前还不支持修改列注释。
- 每张表仅支持一个进行中的 Schema Change 操作。不能对同一张表同时执行两条 Schema Change 命令。
- bucket、column、rollup index <!--是否包含compaction和fast schema evolution-->是异步操作，命令提交成功后会立即返回一个成功消息，您可以使用 [SHOW ALTER TABLE](SHOW_ALTER.md) 语句查看操作的进度。如果需要取消正在进行的操作，则您可以使用 [CANCEL ALTER TABLE](SHOW_ALTER.md)。
- rename、comment、partition、bitmap index 和 swap 是同步操作，命令返回表示执行完毕。

### Rename 对名称进行修改

#### 修改表名

语法：

```sql
ALTER TABLE <tbl_name> RENAME <new_tbl_name>;
```

#### 修改 rollup index 名称 (RENAME ROLLUP)

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
RENAME ROLLUP old_rollup_name new_rollup_name;
```

#### 修改 partition 名称 (RENAME PARTITION)

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
RENAME PARTITION <old_partition_name> <new_partition_name>;
```

#### 修改列名（RENAME COLUMN）

自 v3.3.2 起，StarRocks 支持修改列名。

```sql
ALTER TABLE [<db_name>.]<tbl_name>
RENAME COLUMN <old_col_name> [ TO ] <new_col_name>
```

:::note

- 在将某列由 A 重命名为 B 后，不支持继续增加 A 列。
- 在列名变更后，基于该列创建的物化视图将不再生效，您需要根据新的列名重新创建。

:::

### 修改表的注释（3.1 版本起）

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name> COMMENT = "<new table comment>";
```

:::tip
当前还不支持修改列注释。
:::

### 操作 partition 相关语法

#### 增加分区 (ADD PARTITION(S))

增加分区时支持使用 Range 分区和 List 分区。不支持增加表达式分区。

语法：

- Range 分区

    ```SQL
    ALTER TABLE
        ADD { single_range_partition | multi_range_partitions } [distribution_desc] ["key"="value"];
    
    single_range_partition ::=
        PARTITION [IF NOT EXISTS] <partition_name> VALUES partition_key_desc

    partition_key_desc ::=
        { LESS THAN { MAXVALUE | value_list }
        | [ value_list , value_list ) } -- 注意此处的 [ 代表左闭合区间

    value_list ::=
        ( <value> [, ...] )

    multi_range_partitions ::=
        { PARTITIONS START ("<start_date_value>") END ("<end_date_value>") EVERY ( INTERVAL <N> <time_unit> )
        | PARTITIONS START ("<start_integer_value>") END ("<end_integer_value>") EVERY ( <granularity> ) } -- 即使 START、END 所指定的分区列值为整数，也需要使用英文引号包裹，而 EVERY 子句中的分区增量值不用英文引号包裹。
    ```

- List 分区

    ```SQL
    ALTER TABLE
        ADD PARTITION <partition_name> VALUES IN (value_list) [distribution_desc] ["key"="value"];

    value_list ::=
        value_item [, ...]

    value_item ::=
        { <value> | ( <value> [, ...] ) }
    ```

参数：

- 分区相关参数

  - Range 分区支持新增单个分区 `single_range_partition` 或者批量创建分区 `multi_range_partition`。
  - List 分区仅支持新增单个分区。

- `distribution_desc`：

   可以为新的分区单独设置分桶数量，但是不支持单独设置分桶方式。

- `"key"="value"`：

   可以为新的分区设置属性，具体说明见 [CREATE TABLE](CREATE_TABLE.md#properties)。

示例：

- Range 分区

  - 如果建表时指定分区列为 `event_day`，例如 `PARTITION BY RANGE(event_day)`，并且建表后需要新增一个分区，则可以执行：

    ```sql
    ALTER TABLE site_access ADD PARTITION p4 VALUES LESS THAN ("2020-04-30");
    ```

  - 如果建表时指定分区列为 `datekey`，例如 `PARTITION BY RANGE (datekey)`，并且建表后需要批量新增多个分区，则可以执行：

    ```sql
    ALTER TABLE site_access
        ADD PARTITIONS START ("2021-01-05") END ("2021-01-10") EVERY (INTERVAL 1 DAY);
    ```

- List 分区

  - 如果建表时指定单个分区列，例如 `PARTITION BY LIST (city)`，并且建表后需要新增一个分区，则可以执行：

    ```sql
    ALTER TABLE t_recharge_detail2
    ADD PARTITION pCalifornia VALUES IN ("Los Angeles","San Francisco","San Diego");
    ```

  - 如果建表时指定多个分区列，例如 `PARTITION BY LIST (dt,city)`，并且建表后需要新增一个分区，则可以执行：

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

#### 删除分区 (DROP PARTITION(S))

删除单个分区：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP PARTITION [ IF EXISTS ] <partition_name> [ FORCE ]
```

批量删除分区（自 v3.3.1 起支持）：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP [ TEMPORARY ] PARTITIONS [ IF EXISTS ]  { partition_name_list | multi_range_partitions } [ FORCE ] 

partion_name_list ::= ( <partition_name> [, ... ] )

multi_range_partitions ::=
    { START ("<start_date_value>") END ("<end_date_value>") EVERY ( INTERVAL <N> <time_unit> )
    | START ("<start_integer_value>") END ("<end_integer_value>") EVERY ( <granularity> ) } -- 即使 START、END 所指定的分区列值为整数，也需要使用英文引号包裹，而 EVERY 子句中的分区增量值不用英文引号包裹。
```

关于 `multi_range_partitions` 的说明：

- `multi_range_partitions` 仅适用于 Range 分区。
- 其中涉及的参数与 [增加分区 ADD PARTITION(S)](#增加分区-add-partitions) 中的相同。
- 仅支持基于单个分区键的分区。

:::note

- 分区表需要至少要保留一个分区。
- 如果未指定 FORCE 关键字，您可以通过 [RECOVER](../backup_restore/RECOVER.md) 语句恢复一定时间范围内（默认 1 天）删除的分区。
- 如果指定了 FORCE 关键字，则系统不会检查该分区是否存在未完成的事务，分区将直接被删除并且不能被恢复，一般不建议执行此操作。

:::

#### 增加临时分区 (ADD TEMPORARY PARTITION)

详细使用信息，请查阅[临时分区](../../../table_design/data_distribution/Temporary_partition.md)。

语法：

```SQL
ALTER TABLE [<db_name>.]<tbl_name>
ADD TEMPORARY PARTITION [IF NOT EXISTS] <partition_name>
partition_desc ["key"="value"]
[DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]];
```

#### 使用临时分区替换原分区

语法：

```SQL
ALTER TABLE [<db_name>.]<tbl_name>
REPLACE PARTITION <partition_name> 
partition_desc ["key"="value"]
WITH TEMPORARY PARTITION
partition_desc ["key"="value"]
[PROPERTIES ("key"="value", ...)]
```

#### 删除临时分区 (DROP TEMPORARY PARTITION)

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP TEMPORARY PARTITION <partition_name>;
```

#### 修改分区属性 (MODIFY PARTITION)

**语法：**

```sql
ALTER TABLE [<db_name>.]<tbl_name>
    MODIFY PARTITION { <partition_name> | partition_name_list | (*) }
        SET ("key" = "value", ...);
```

**使用说明：**

- 当前支持修改分区的下列属性：
  - storage_medium
  - storage_cooldown_ttl 或 storage_cooldown_time
  - replication_num

- 对于单分区表，分区名同表名。对于多分区表，如果需要修改所有分区的属性，则使用 `(*)` 更加方便。
- 执行 `SHOW PARTITIONS FROM <tbl_name>` 查看修改后分区属性。

### 修改分桶方式和分桶数量（自 3.2 版本起）

语法：

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

示例：

假设原表为明细表，分桶方式为 Hash 分桶，分桶数量为自动设置。

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

-- 插入多天的数据
INSERT INTO details (event_time, event_type, user_id, device_code, channel) VALUES
-- 11 月 26 日的数据
('2023-11-26 08:00:00', 1, 101, 12345, 2),
('2023-11-26 09:15:00', 2, 102, 54321, 3),
('2023-11-26 10:30:00', 1, 103, 98765, 1),
-- 11 月 27 日的数据
('2023-11-27 08:30:00', 1, 104, 11111, 2),
('2023-11-27 09:45:00', 2, 105, 22222, 3),
('2023-11-27 11:00:00', 1, 106, 33333, 1),
-- 11 月 28 日的数据
('2023-11-28 08:00:00', 1, 107, 44444, 2),
('2023-11-28 09:15:00', 2, 108, 55555, 3),
('2023-11-28 10:30:00', 1, 109, 66666, 1);
```

#### 仅修改分桶方式

> **注意**
>
> - 修改分桶方式针对整个表的所有分区生效，不能仅仅针对某个分区生效。
> - 虽然仅修改分桶方式，没有修改分桶数量，但是您仍然需要在语句中说明分桶数量 `BUCKETS <num>`，如果不指定，则表示由 StarRocks 自动设置分桶数量。

- 将原先的 Hash 分桶修改为 Random 分桶，并且分桶数量仍然由 StarRocks 自动设置。

  ```SQL
  ALTER TABLE details DISTRIBUTED BY RANDOM;
  ```

- 将 Hash 分桶时所使用的分桶键从原先的 `event_time, event_type` 修改为 `user_id, event_time`。并且分桶数量仍然由 StarRocks 自动设置。

  ```SQL
  ALTER TABLE details DISTRIBUTED BY HASH(user_id, event_time);
  ```

#### 仅修改分桶数量

> **注意**
>
> 虽然仅修改分桶数量，没有修改分桶方式，但是您仍然需要在语句中说明分桶方式，例如示例中的 `HASH(user_id)`。

- 将所有分区的分桶数量从原先的由 StarRocks 自动设置修改为 10。

  ```SQL
  ALTER TABLE details DISTRIBUTED BY HASH(user_id) BUCKETS 10;
  ```

- 将指定分区的分桶数量从原先的由 StarRocks 自动设置修改为 15。

  ```SQL
  ALTER TABLE details PARTITIONS (p20231127, p20231128) DISTRIBUTED BY HASH(user_id) BUCKETS 15;
  ```

  > **说明**
  >
  > 分区名称可以执行 `SHOW PARTITIONS FROM <table_name>;` 进行查看。

#### 同时修改分桶方式和分桶数量

> **注意**
>
> 同时修改分桶方式和分桶数量针对整个表的所有分区生效，不能仅仅针对某个分区生效。

- 分桶方式从原先的 Hash 分桶修改为 Random 分桶，并且分桶数量从原先的由 StarRocks 自动设置修改为 10。

   ```SQL
   ALTER TABLE details DISTRIBUTED BY RANDOM BUCKETS 10;
   ```

- 将 Hash 分桶时所使用的分桶键从原先的 `event_time, event_type` 修改为 `user_id, event_time`，并且分桶数量从原先的由 StarRocks 自动设置修改为 10。

  ```SQL
  ALTER TABLE details DISTRIBUTED BY HASH(user_id, event_time) BUCKETS 10;
  ```

### 修改列（增删列和修改列顺序）

下文中的 index 为物化索引。建表成功后表为 base 表 (base index)，基于 base 表可 [创建 rollup index](#创建-rollup-index-add-rollup)。

base index 和 rollup index 都是物化索引。下方语句在编写时如果没有指定 `rollup_index_name`，默认操作基表。

#### 向指定 index 的指定位置添加一列 (ADD COLUMN)

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
ADD COLUMN column_name column_type [KEY | agg_type] [DEFAULT "default_value"]
[AFTER column_name|FIRST]
[TO rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

使用说明：

- 聚合表如果增加 value 列，需要指定 agg_type。
- 非聚合表（如 DUPLICATE KEY）如果增加 key 列，需要指定 KEY 关键字。
- 不能在 rollup index 中增加 base index 中已经存在的列，如有需要，可以重新创建一个 rollup index。

#### 向指定 index 添加多列

语法：

- 添加多列:

  ```sql
  ALTER TABLE [<db_name>.]<tbl_name>
  ADD COLUMN (column_name1 column_type [KEY | agg_type] DEFAULT "default_value", ...)
  [TO rollup_index_name]
  [PROPERTIES ("key"="value", ...)]
  ```

- 添加多列的同时通过 `AFTER` 指定列的添加位置：

  ```sql
  ALTER TABLE [<db_name>.]<tbl_name>
  ADD COLUMN column_name1 column_type [KEY | agg_type] DEFAULT "default_value" AFTER column_name,
  ADD COLUMN column_name2 column_type [KEY | agg_type] DEFAULT "default_value" AFTER column_name
  [, ...]
  [TO rollup_index_name]
  [PROPERTIES ("key"="value", ...)]
  ```

注意：

1. 聚合表如果增加 value 列，需要指定 agg_type。
2. 非聚合表如果增加 key 列，需要指定 KEY 关键字。
3. 不能在 rollup index 中增加 base index 中已经存在的列，如有需要，可以重新创建一个 rollup index。

#### 增加生成列

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
ADD COLUMN col_name data_type [NULL] AS generation_expr [COMMENT 'string']
```

增加生成列并且指定其使用的表达式。[生成列](../generated_columns.md)用于预先计算并存储表达式的结果，可以加速包含复杂表达式的查询。自 v3.1，StarRocks 支持该功能。

#### 从指定 index 中删除一列 (DROP COLUMN)

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP COLUMN column_name
[FROM rollup_index_name];
```

注意：

1. 不能删除分区列。
2. 如果是从 base index 中删除列，则如果 rollup index 中包含该列，也会被删除。

#### 修改指定 index 的列类型以及列位置 (MODIFY COLUMN)

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
MODIFY COLUMN column_name column_type [KEY | agg_type] [NULL | NOT NULL] [DEFAULT "default_value"]
[AFTER column_name|FIRST]
[FROM rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

注意：

1. 聚合表如果修改 value 列，需要指定 agg_type。
2. 非聚合类型如果修改 key 列，需要指定 KEY 关键字。
3. 只能修改列的类型，列的其他属性维持原样（即其他属性需在语句中按照原属性显式的写出，参见示例中 [column](#column) 部分第 8 个例子）。
4. 分区列不能做任何修改。
5. 目前支持以下类型的转换（精度损失由用户保证）：

    - TINYINT/SMALLINT/INT/BIGINT 转换成 TINYINT/SMALLINT/INT/BIGINT/DOUBLE。
    - TINYINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE/DECIMAL 转换成 VARCHAR。
    - VARCHAR 支持修改最大长度。
    - VARCHAR 转换成 TINYINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE。
    - VARCHAR 转换成 DATE (目前支持 "%Y-%m-%d"，"%y-%m-%d"， "%Y%m%d"，"%y%m%d"，"%Y/%m/%d，"%y/%m/%d " 六种格式化格式)
    DATETIME 转换成 DATE(仅保留年-月-日信息，例如: `2019-12-09 21:47:05` &lt;--&gt; `2019-12-09`)
    DATE 转换成 DATETIME(时分秒自动补零，例如: `2019-12-09` &lt;--&gt; `2019-12-09 00:00:00`)
    - FLOAT 转换成 DOUBLE。
    - INT 转换成 DATE (如果 INT 类型数据不合法则转换失败，原始数据不变。)

#### 修改排序键

自 3.0 版本起，支持修改主键表的排序键。自 3.3 版本起，支持修改明细表、聚合表和更新表的排序键。

明细表和主键表中排序键可以为任意列的排序组合，聚合表和更新表中排序键必须包含所有 key 列，但是列的顺序无需与 key 列保持一致。

语法：

```SQL
ALTER TABLE [<db_name>.]<table_name>
[ order_desc ]

order_desc ::=
    ORDER BY <column_name> [, <column_name> ...]
```

示例：修改主键表中的排序键

假设原表为主键表，排序键与主键耦合 `dt,order_id`。

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

解耦排序键和主键，修改排序键为 `dt,revenue,state`。

```SQL
ALTER TABLE orders ORDER BY (dt,revenue,state);
```

#### STRUCT 类型列增删字段（MODIFY COLUMN ADD/DROP FIELD）

自 v3.2.10 及 v3.3.2 起，StarRocks 支持向 STRUCT 类型列增删字段。该字段可以为嵌套 STRUCT 类型或存在于 ARRAY 类型中。

语法：

```sql
-- 增加字段
ALTER TABLE [<db_name>.]<tbl_name> MODIFY COLUMN <column_name>
ADD FIELD field_path field_desc

-- 删除字段
ALTER TABLE [<db_name>.]<tbl_name> MODIFY COLUMN <column_name>
DROP FIELD field_path

field_path ::= [ { <field_name>. | [*]. } [ ... ] ]<field_name>

  -- 请注意，此处的 `[*]` 整体是一个预定义符号，不可拆分，
  -- 用于在添加或删除嵌套在 ARRAY 类型中的 STRUCT 类型的字段时，代表 ARRAY 字段中的所有元素。
  -- 有关详细信息，请参阅以下 `field_path` 的参数说明和示例。

field_desc ::= <field_type> [ AFTER <prior_field_name> | FIRST ]
```

参数：

- `field_path`：需要增加或删除字段。可以是单独的字段名，表示第一层级的字段，例如 `new_field_name`。也可以是 Column Access Path，用以表示嵌套层级的字段，例如 `lv1_k1.lv2_k2.[*].new_field_name`。
- `[*]`：当 ARRAY 和 STRUCT 类型发生嵌套时，`[*]` 代表操作 ARRAY 字段中的所有元素，用于在 ARRAY 类型字段下嵌套的每个 STRUCT 类型字段中添加或删除子字段。
- `prior_field_name`：新增字段的前一个字段。与 AFTER 关键字合用，可以表示新加字段的顺序。如果指定 FIRST 关键字，表示新增第一个字段，则无需指定该参数。`prior_field_name` 的层级由 `field_path` 决定，无需手动指定（也即是 `new_field_name` 之前的部分 `level1_k1.level2_k2.[*]`）。

`field_path` 示例：

- 向嵌套 STRUCT 类型中添加或删除子字段。

  假设向列 `fx stuct<c1 int, c2 struct <v1 int, v2 int>>` 中的 `c2` 字段下增加新字段 `v3`, 对应的语法为：

  ```SQL
  ALTER TABLE tbl MODIFY COLUMN fx ADD FIELD c2.v3 INT
  ```

  操作后该列变为 `fx stuct<c1 int, c2 struct <v1 int, v2 int, v3 int>>`。

- 向 ARRAY 类型下嵌套的 STRUCT 类型中添加或删除子字段。

  假设有列 `fx struct<c1 int, c2 array<struct <v1 int, v2 int>>>`, `fx` 列中 `c2` 字段是 ARRAY 类型，其下嵌套 STRUCT 类型元素 `v1` 和 `v2` 两个字段。当向 `c2` 中的 STRUCT 类型元素添加一个新字段 `v3` 时，对应的语法为：

  ```SQL
  ALTER TABLE tbl MODIFY COLUMN fx ADD FIELD c2.[*].v3 INT
  ```

  操作后该列变为 `fx struct<c1 int, c2 array<struct <v1 int, v2 int, v3 int>>>`。

有关示例，参考 [示例 - Column -14](#column)。

:::note

- 目前仅支持存算一体集群。
- 对应表必须开启 `fast_schema_evolution`。
- 不支持向 MAP 类型中的 STRUCT 增删字段。
- 新增字段不支持指定默认值，Nullable 等属性。默认为 Nullable，默认值为 null。
- 使用该功能后，不支持直接将集群降级至不具备该功能的版本。

:::

### 操作 rollup index 语法

#### 创建 rollup index (ADD ROLLUP)

**RollUp 表索引**: shortkey index 可加速数据查找，但 shortkey index 依赖维度列排列次序。如果使用非前缀的维度列构造查找谓词，用户可以为数据表创建若干 RollUp 表索引。 RollUp 表索引的数据组织和存储和数据表相同，但 RollUp 表拥有自身的 shortkey index。用户创建 RollUp 表索引时，可选择聚合的粒度，列的数量，维度列的次序。使频繁使用的查询条件能够命中相应的 RollUp 表索引。

语法：

```SQL
ALTER TABLE [<db_name>.]<tbl_name> 
ADD ROLLUP rollup_name (column_name1, column_name2, ...)
[FROM from_index_name]
[PROPERTIES ("key"="value", ...)];
```

PROPERTIES: 支持设置超时时间，默认超时时间为 1 天。

例子：

```SQL
ALTER TABLE [<db_name>.]<tbl_name> 
ADD ROLLUP r1(col1,col2) from r0;
```

#### 批量创建 rollup index

语法：

```SQL
ALTER TABLE [<db_name>.]<tbl_name>
ADD ROLLUP [rollup_name (column_name1, column_name2, ...)
[FROM from_index_name]
[PROPERTIES ("key"="value", ...)],...];
```

例子：

```SQL
ALTER TABLE [<db_name>.]<tbl_name>
ADD ROLLUP r1(col1,col2) from r0, r2(col3,col4) from r0;
```

> 注意
>
1. 如果没有指定 from_index_name，则默认从 base index 创建。
2. rollup 表中的列必须是 from_index 中已有的列。
3. 在 properties 中，可以指定存储格式。具体请参阅 [CREATE TABLE](CREATE_TABLE.md) 章节。

#### 删除 rollup index (DROP ROLLUP)

语法：

```SQL
ALTER TABLE [<db_name>.]<tbl_name>
DROP ROLLUP rollup_name [PROPERTIES ("key"="value", ...)];
```

例子：

```sql
ALTER TABLE [<db_name>.]<tbl_name> DROP ROLLUP r1;
```

#### 批量删除 rollup index

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP ROLLUP [rollup_name [PROPERTIES ("key"="value", ...)],...];
```

例子：

```sql
ALTER TABLE [<db_name>.]<tbl_name> DROP ROLLUP r1, r2;
```

注意：

不能删除 base index。

### bitmap index 修改

#### 创建 bitmap index (ADD INDEX)

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
ADD INDEX index_name (column [, ...],) [USING BITMAP] [COMMENT 'balabala'];
```

注意：

1. 目前仅支持修改 bitmap index。
2. bitmap index 仅在单列上创建。

#### 删除 bitmap index (DROP INDEX)

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP INDEX index_name;
```

### 修改表的属性

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
SET ("key" = "value")
```

参数说明：

- `key` 表示表属性的名称，`value` 表示该表属性的配置。

- 支持修改如下表属性：
  - `replication_num`
  - `default.replication_num`
  - `storage_cooldown_ttl`
  - `storage_cooldown_time`
  - [动态分区相关属性 properties](../../../table_design/data_distribution/dynamic_partitioning.md)，比如 `dynamic_partition.enable`
  - `enable_persistent_index`
  - `bloom_filter_columns`
  - `colocate_with`
  - `bucket_size`（自 3.2 版本支持）
  - `base_compaction_forbidden_time_ranges`（自 v3.2.13 版本支持）


:::note

- 大多数情况下，一次只能修改一个属性。只有当多个属性的前缀相同时，才能同时修改多个属性。目前仅支持 `dynamic_partition.` 和 `binlog.`。
- 修改表的属性也可以合并到 schema change 操作中来修改，见[示例](#示例)部分。

:::

### Swap 将两个表原子替换

语法：

```sql
ALTER TABLE [<db_name>.]<tbl_name>
SWAP WITH <tbl_name>;
```

:::note
- StarRocks 会在 Swap 过程中验证 OLAP 表之间的唯一键和外键约束，以确保被交换的两个表的约束一致。如果检测到不一致，将返回错误信息。如果未检测到不一致，将自动 Swap 唯一键和外键约束。
- 依赖于被 Swap 表的物化视图将自动设置为 Inactive，其唯一键和外键约束将被移除，变为不可用。
:::

### 手动 Compaction（3.1 版本起）

StarRocks 通过 Compaction 机制将导入的不同数据版本进行合并，将小文件合并成大文件，有效提升了查询性能。

3.1 版本之前，支持通过两种方式来做 Compaction：

- 系统自动在后台执行 Compaction。Compaction 的粒度是 BE 级，由后台自动执行，用户无法控制具体的数据库或者表。
- 用户通过 HTTP 接口指定 Tablet 来执行 Compaction。

3.1 版本之后，增加了一个 SQL 接口，用户可以通过执行 SQL 命令来手动进行 Compaction，可以指定表、单个或多个分区进行 Compaction。

存算分离集群自 v3.3.0 起支持该功能。

> **说明**
>
> 自 v3.2.13 版本起支持通过 [`base_compaction_forbidden_time_ranges`](./CREATE_TABLE.md#禁止-base-compaction) 属性在特定时段禁止 Base Compaction。

语法：

```sql
-- 对整张表做 compaction。
ALTER TABLE <tbl_name> COMPACT

-- 指定一个分区进行 compaction。
ALTER TABLE <tbl_name> COMPACT <partition_name>

-- 指定多个分区进行 compaction。
ALTER TABLE <tbl_name> COMPACT (<partition1_name>[,<partition2_name>,...])

-- 对多个分区进行 cumulative compaction。
ALTER TABLE <tbl_name> CUMULATIVE COMPACT (<partition1_name>[,<partition2_name>,...])

-- 对多个分区进行 base compaction。
ALTER TABLE <tbl_name> BASE COMPACT (<partition1_name>[,<partition2_name>,...])
```

执行完 Compaction 后，您可以通过查询 `information_schema` 数据库下的 `be_compactions` 表来查看 Compaction 后的数据版本变化 （`SELECT * FROM information_schema.be_compactions;`）。

## 示例

### Table

1. 修改表的默认副本数量，新建分区副本数量默认使用此值。

    ```sql
    ALTER TABLE example_db.my_table
    SET ("default.replication_num" = "2");
    ```

2. 修改单分区表的实际副本数量(只限单分区表)。

    ```sql
    ALTER TABLE example_db.my_table
    SET ("replication_num" = "3");
    ```

3. 修改数据在多副本间的写入和同步方式。

    ```sql
    ALTER TABLE example_db.my_table
    SET ("replicated_storage" = "false");
    ```

    以上示例表示将多副本的写入和同步方式设置为 leaderless replication，即数据同时写入到多个副本，不区分主从副本。更多信息，参见 [CREATE TABLE](CREATE_TABLE.md) 的 `replicated_storage` 参数描述。

### Partition

1. 增加分区，现有分区 [MIN, 2013-01-01)，增加分区 [2013-01-01, 2014-01-01)，使用默认分桶方式。

    ```sql
    ALTER TABLE example_db.my_table
    ADD PARTITION p1 VALUES LESS THAN ("2014-01-01");
    ```

2. 增加分区，使用新的分桶数。

    ```sql
    ALTER TABLE example_db.my_table
    ADD PARTITION p1 VALUES LESS THAN ("2015-01-01")
    DISTRIBUTED BY HASH(k1) BUCKETS 20;
    ```

3. 增加分区，使用新的副本数。

    ```sql
    ALTER TABLE example_db.my_table
    ADD PARTITION p1 VALUES LESS THAN ("2015-01-01")
    ("replication_num"="1");
    ```

4. 修改分区副本数。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY PARTITION p1 SET("replication_num"="1");
    ```

5. 批量修改指定分区。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY PARTITION (p1, p2, p4) SET("replication_num"="1");
    ```

6. 批量修改所有分区。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY PARTITION (*) SET("storage_medium"="HDD");
    ```

7. 删除分区。

    ```sql
    ALTER TABLE example_db.my_table
    DROP PARTITION p1;
    ```

8. 增加一个指定上下界的分区。

    ```sql
    ALTER TABLE example_db.my_table
    ADD PARTITION p1 VALUES [("2014-01-01"), ("2014-02-01"));
    ```

### Rollup index

1. 创建 index `example_rollup_index`，基于 base index（k1, k2, k3, v1, v2），列式存储。

    ```sql
    ALTER TABLE example_db.my_table
    ADD ROLLUP example_rollup_index(k1, k3, v1, v2)
    PROPERTIES("storage_type"="column");
    ```

2. 创建 index `example_rollup_index2`，基于 example_rollup_index（k1, k3, v1, v2）。

    ```sql
    ALTER TABLE example_db.my_table
    ADD ROLLUP example_rollup_index2 (k1, v1)
    FROM example_rollup_index;
    ```

3. 创建 index `example_rollup_index3`，基于 base index (k1, k2, k3, v1), 自定义 rollup 超时时间一小时。

    ```sql
    ALTER TABLE example_db.my_table
    ADD ROLLUP example_rollup_index3(k1, k3, v1)
    PROPERTIES("storage_type"="column", "timeout" = "3600");
    ```

4. 删除 index `example_rollup_index2`。

    ```sql
    ALTER TABLE example_db.my_table
    DROP ROLLUP example_rollup_index2;
    ```

### Column

1. 向 `example_rollup_index` 的 `col1` 后添加一个 key 列 `new_col`（非聚合表）。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT KEY DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

2. 向 `example_rollup_index` 的 `col1` 后添加一个 value 列 `new_col`（非聚合表）。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

3. 向 `example_rollup_index` 的 `col1` 后添加一个 key 列 `new_col`（聚合表）。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

4. 向 `example_rollup_index` 的 `col1` 后添加一个 value 列 `new_col`（SUM 聚合类型）（聚合表）。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT SUM DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

5. 向 `example_rollup_index` 添加多列（聚合表）。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN (col1 INT DEFAULT "1", col2 FLOAT SUM DEFAULT "2.3")
    TO example_rollup_index;
    ```

6. 向 `example_rollup_index` 添加多列并通过 `AFTER` 指定列的添加位置。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN col1 INT DEFAULT "1" AFTER `k1`,
    ADD COLUMN col2 FLOAT SUM AFTER `v2`,
    TO example_rollup_index;
    ```

7. 从 `example_rollup_index` 删除一列。

    ```sql
    ALTER TABLE example_db.my_table
    DROP COLUMN col2
    FROM example_rollup_index;
    ```

8. 修改 base index 的 `col1` 列的类型为 BIGINT，并移动到 `col2` 列后面。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY COLUMN col1 BIGINT DEFAULT "1" AFTER col2;
    ```

9. 修改 base index 的 `val1` 列最大长度。原 `val1` 为 (`val1 VARCHAR(32) REPLACE DEFAULT "abc"`)。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY COLUMN val1 VARCHAR(64) REPLACE DEFAULT "abc";
    ```

10. 重新排序 `example_rollup_index` 中的列（设原列顺序为：k1, k2, k3, v1, v2）。

    ```sql
    ALTER TABLE example_db.my_table
    ORDER BY (k3,k1,k2,v2,v1)
    FROM example_rollup_index;
    ```

11. 同时执行两种操作。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN v2 INT MAX DEFAULT "0" AFTER k2 TO example_rollup_index,
    ORDER BY (k3,k1,k2,v2,v1) FROM example_rollup_index;
    ```

12. 修改表的 bloom filter 列。

    ```sql
    ALTER TABLE example_db.my_table
    SET ("bloom_filter_columns"="k1,k2,k3");
    ```

    也可以合并到上面的修改列操作中（注意多子句的语法有少许区别）

    ```sql
    ALTER TABLE example_db.my_table
    DROP COLUMN col2
    PROPERTIES ("bloom_filter_columns"="k1,k2,k3");
    ```

13. 批量修改字段数据类型。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY COLUMN k1 VARCHAR(100) KEY NOT NULL,
    MODIFY COLUMN v2 DOUBLE DEFAULT "1" AFTER v1;
    ```

14. STRUCT 类型增删字段。

    **准备工作**：建表并导入数据

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

    - 向 STRUCT 类型列添加新字段。

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

    - 向嵌套 STRUCT 类型添加字段。

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

    - 向 Array 类型中的 STRUCT 类型添加字段。

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

    - 删除 STRUCT 类型列中的字段。

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

    - 删除嵌套 STRUCT 类型中的字段。

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

    - 删除 Array 类型中的 STRUCT 类型的字段。

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

1. 修改表的 Colocate 属性。

    ```sql
    ALTER TABLE example_db.my_table
    SET ("colocate_with" = "t1");
    ```

2. 修改表的动态分区属性(支持未添加动态分区属性的表添加动态分区属性)。

    ```sql
    ALTER TABLE example_db.my_table
    SET ("dynamic_partition.enable" = "false");
    ```

    如果需要在未添加动态分区属性的表中添加动态分区属性，则需要指定所有的动态分区属性。

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

1. 将表 `table1` 的名称修改为 `table2`。

    ```sql
    ALTER TABLE table1 RENAME table2;
    ```

2. 将表 `example_table` 中名为 `rollup1` 的 rollup index 修改为 `rollup2`。

    ```sql
    ALTER TABLE example_table RENAME ROLLUP rollup1 rollup2;
    ```

3. 将表 `example_table` 中名为 `p1` 的 partition 修改为 `p2`。

    ```sql
    ALTER TABLE example_table RENAME PARTITION p1 p2;
    ```

### Bitmap index

1. 在 `table1` 上为 `siteid` 创建 bitmap index。

    ```sql
    ALTER TABLE table1
    ADD INDEX index_name (siteid) [USING BITMAP] COMMENT 'balabala';
    ```

2. 删除 `table1` 上的 `siteid` 列的 bitmap index。

    ```sql
    ALTER TABLE table1 DROP INDEX index_name;
    ```

### Swap

将 `table1` 与 `table2` 原子替换。

```sql
ALTER TABLE table1 SWAP WITH table2;
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

## 相关参考

- [CREATE TABLE](CREATE_TABLE.md)
- [SHOW CREATE TABLE](SHOW_CREATE_TABLE.md)
- [SHOW TABLES](SHOW_TABLES.md)
- [SHOW ALTER TABLE](SHOW_ALTER.md)
- [DROP TABLE](DROP_TABLE.md)

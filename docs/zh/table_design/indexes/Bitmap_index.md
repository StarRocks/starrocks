---
displayed_sidebar: "Chinese"
keywords: ['suoyin']
---

# Bitmap 索引

本文介绍了如何创建和管理 bitmap（位图）索引，以及 Bitmap 索引的使用案例。

Bitmap 索引是一种使用 bitmap 的特殊数据库索引。bitmap 即为一个 bit 数组，一个 bit 的取值有两种：0 或 1。每一个 bit 对应数据表中的一行，并根据该行的取值情况来决定 bit 的取值是 0 还是 1。

Bitmap 索引能够提高指定列的查询效率。如果一个查询条件命中列，StarRocks 即可使用[前缀索引](./Prefix_index_sort_key.md)提高查询效率，快速返回查询结果。但是前缀索引的长度有限，如果想要提高一个非前缀索引列的查询效率，即可以为这一列创建 Bitmap 索引。

Bitmap 索引一般适用于高基数列，基于列构建的 Bitmap 索引的选择性高，并且使用 Bitmap 索引后能筛选出较少数据行的场景。
​
在 StarRocks 中使用 SSB 100G 测试数据集验证 Bitmap 索引加速查询的效果，测试结果如下：

- 只有为高基数列创建 Bitmap 索引，查询性能才会有比较明显的提升（在此测试数据集中，基数达到 100000 数量级可以看到比较明显的性能提升）。
- 高基数列可以是高基数的单列，也可以是高基数的多列组合。
- 为低基数列创建 Bitmap 索引，查询性能基本没有提升甚至会下降。

## 优势

- Bitmap 索引所占的存储空间通常只有索引数据的一小部分，与其他索引技术相比，更节省存储空间。
- 支持为多个列创建 Bitmap 索引，提高多列查询的效率，具体参见[多列查询](#多列查询)。

## 使用说明

- Bitmap 索引适用于可使用等值条件 (`=`) 查询或 [NOT] IN 范围查询的列。
- 主键表和明细表中所有列都可以创建 Bitmap 索引；聚合表和更新表中，只有维度列（即 Key 列）支持创建 bitmap 索引。
- 支持为如下类型的列创建 Bitmap 索引：
  - 日期类型：DATE、DATETIME。
  - 数值类型：TINYINT、SMALLINT、INT、BIGINT、LARGEINT、DECIMAL 和 BOOLEAN。
  - 字符串类型：CHAR、STRING 和 VARCHAR。
  - 其他类型：HLL。
- 如要了解一个查询是否命中了 Bitmap 索引，可查看该查询的 Profile 中的 `BitmapIndexFilterRows` 字段。关于如何查看 Profile，参见[分析查询](../../administration/Query_planning.md#查看分析-profile)。

## 创建索引

- 建表时创建 Bitmap 索引。

    ```SQL
    CREATE TABLE d0.table_hash
    (
        k1 TINYINT,
        k2 DECIMAL(10, 2) DEFAULT "10.5",
        v1 CHAR(10) REPLACE,
        v2 INT SUM,
        INDEX index_name (column_name) [USING BITMAP] [COMMENT '']
    )
    ENGINE = olap
    AGGREGATE KEY(k1, k2)
    DISTRIBUTED BY HASH(k1)
    PROPERTIES ("storage_type" = "column");
    ```

    其中有关索引部分参数说明如下：

    | **参数**    | **必选** | **说明**                                                     |
    | ----------- | -------- | ------------------------------------------------------------ |
    | index_name  | 是       | Bitmap 索引名称。命名要求参见[系统限制](../../reference/System_limit.md)。在同一张表中不能创建名称相同的索引。   |
    | column_name | 是       | 创建 Bitmap 索引的列名。您可以指定多个列名，即在建表时可同时为多个列创建 Bitmap 索引。|
    | COMMENT     | 否       | 索引备注。                                                   |

    您可以指定多条 `INDEX index_name (column_name) [USING BITMAP] [COMMENT '']` 命令同时为多个列创建 bitmap 索引，且多条命令之间用逗号（,）隔开。
    关于建表的其他参数说明，参见 [CREATE TABLE](../../sql-reference/sql-statements/data-definition/CREATE_TABLE.md)。

- 建表后使用 CREATE INDEX 创建 Bitmap 索引。详细参数说明和示例，参见 [CREATE INDEX](../../sql-reference/sql-statements/data-definition/CREATE_INDEX.md)。

    ```SQL
    CREATE INDEX index_name ON table_name (column_name) [USING BITMAP] [COMMENT ''];
    ```

## 创建进度

创建 Bitmap 索引为**异步**过程，执行索引创建语句后可通过 [SHOW ALTER TABLE](../../sql-reference/sql-statements/data-manipulation/SHOW_ALTER.md) 命令查看索引创建进度，当返回值中 `State` 字段显示为 `FINISHED` 时，即为创建成功。

```SQL
SHOW ALTER TABLE COLUMN [FROM db_name];
```

> 说明：当前每张表只允许同时进行一个 Schema Change 任务，在一个 Bitmap 索引未异步创建完成前，无法进行新 Bitmap 索引的创建。

## 查看索引

查看指定表的所有 Bitmap 索引。详细参数和返回结果说明，参见 [SHOW INDEX](../../sql-reference/sql-statements/data-manipulation//SHOW_INDEX.md)。

```SQL
SHOW { INDEX[ES] | KEY[S] } FROM [db_name.]table_name [FROM db_name];
```

> **说明**
>
> 创建 Bitmap 索引为异步过程，使用如上语句只能查看到已经创建完成的索引。

## 删除索引

删除指定表的 Bitmap 索引。详细参数说明和示例，参见 [DROP INDEX](../../sql-reference/sql-statements/data-definition/DROP_INDEX.md)。

```SQL
DROP INDEX index_name ON [db_name.]table_name;
```

## 使用案例

以 SSB Flat Table 性能测试的宽表 [`lineorder_flat`](../../benchmarking/SSB_Benchmarking.md#lineorder_flat-默认建表)  为例。

### 单列查询

例如，要提高 `s_address` 列的查询效率，该列的基数较高，约为 200000，则为该列创建 Bitmap 索引，基于该列查询时性能可以明显提升。

```SQL
CREATE INDEX bitmap_s_address ON lineorder_flat (s_address) USING BITMAP COMMENT 'bitmap_s_address';
```

如上语句执行后，Bitmap 生成的过程如下： 

1. 构建字典：字典的 key 列为 `s_address` 列中每个不同的值， value 列为 INT 类型的编码值。
2. 生成 Bitmap 索引：Bitmap 索引的 key 列为字典中 INT 类型的编码值。value 列是位图，表示该值在各个数据行中是否存在。一个位图的长度等于数据表的行数，位图中每一位表示一个数据行，1表示该行具有该值，0表示该行不具有该值。

如果想要统计 `lineorder_flat` 表中 列`s_address` 的值为 `1JJiCepSFCVX0UEAXYD` 的数量，可执行如下语句。

```SQL
select count(*) from lineorder_flat where s_address = '1JJiCepSFCVX0UEAXYD';
```

语句执行后，StarRocks 会查找字典，找到 `1JJiCepSFCVX0UEAXYD` 对应 INT 类型的编码值。然后再去查找 Bitmap 索引，查找其对应的位图，来确定符合条件的数据行。

### 多列查询

例如，要提高 `p_brand` 和 `c_city` 多列组合的查询效率，该多列组合的基数较高，约为 250*1000，则可为这两列分别创建 Bitmap 索引。

- `p_brand`

    ```SQL
    CREATE INDEX bitmap_p_brand ON lineorder_flat (p_brand) USING BITMAP COMMENT 'bitmap_p_brand';
    ```

- `c_city`

    ```SQL
    CREATE INDEX bitmap_c_city ON lineorder_flat (c_city) USING BITMAP COMMENT 'bitmap_c_city';
    ```

如上两个语句执行后，StarRocks 会为 `p_brand` 和 `c_city` 列分别构建字典，然后再根据字典生成 Bitmap 索引。

如果想要统计`lineorder_flat` 表中满足 `p_brand = 'MFGR#2239'` 和 `c_city = 'CHINA 8'`条件的数量，可执行如下语句。

```SQL
select count(*) from lineorder_flat where p_brand = 'MFGR#2239' and c_city = 'CHINA    8';
```

语句执行后，StarRocks 会同时查找分别基于 `p_brand` 和 `c_city` 列构建的字典， `MFGR#2239` 和 `CHINA    8` 对应 INT 类型的编码值。然后再去查找 Bitmap 索引，查找其对应的位图。

因为查询语句中 `p_brand = 'MFGR#2239'` 和 `c_city = 'CHINA    8'` 这两个条件是 `AND` 关系，所以 StarRocks 会对两个位图进行位运算 ，得到最终结果。根据最终结果，确定符合条件的数据行。

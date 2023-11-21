---
displayed_sidebar: "Chinese"
---

# 排序键和前缀索引

在建表时，您可以指定一个或多个列构成排序键 (Sort Key)。表中的行会根据排序键进行排序以后再落入磁盘存储。查询数据时，您可以使用排序列指定过滤条件，StarRocks 不需要扫描全表即可快速找到需要处理的数据，降低搜索的复杂度，从而加速查询。

同时，为减少内存开销，StarRocks 在排序键的基础上又引入了前缀索引 (Prefix Index)。前缀索引是一种稀疏索引。表中每 1024 行数据构成一个逻辑数据块 (Data Block)。每个逻辑数据块在前缀索引表中存储一个索引项，索引项的长度不超过 36 字节，其内容为数据块中第一行数据的排序列组成的前缀，在查找前缀索引表时可以帮助确定该行数据所在逻辑数据块的起始行号。前缀索引的大小会比数据量少 1024 倍，因此会全量缓存在内存中，在实际查找的过程中可以有效加速查询。

## 排序原理

在明细模型中，排序列就是通过 `DUPLICATE KEY` 关键字指定的列。

在聚合模型中，排序列就是通过 `AGGREGATE KEY` 关键字指定的列。

在更新模型中，排序列就是通过 `UNIQUE KEY` 关键字指定的列。

自 3.0 版本起，主键模型解耦了主键列和排序列，排序列通过 `ORDER BY` 关键字指定，主键列通过 `PRIMARY KEY` 关键字指定。

在明细模型、聚合模型、更新模型中定义排序列时，需要注意以下几点：

- 排序列必须从定义的第一列开始、并且是连续的。

- 在定义各列时，计划作为排序列的列必须定义在其他普通列之前。

- 排序列的顺序必须与表定义的列顺序一致。

例如，建表语句中声明要创建 `site_id`、`city_code`、`user_id` 和 `pv` 四列。这种情况下，正确的排序列组合和错误的排序列组合举例如下：

- 正确的排序列
  - `site_id` 和 `city_code`
  - `site_id`、`city_code` 和 `user_id`

- 错误的排序列
  - `city_code` 和 `site_id`
  - `city_code` 和 `user_id`
  - `site_id`、`city_code` 和 `pv`

下面通过示例来说明如何创建使用各个数据模型的表，以下建表语句适用于至少部署三个 BE 节点的集群环境下。

### 明细模型

创建一个名为 `site_access_duplicate` 的明细模型表，包含 `site_id`、`city_code`、`user_id` 和 `pv` 四列，其中 `site_id` 和 `city_code` 为排序列。

建表语句如下：

```SQL
CREATE TABLE site_access_duplicate
(
    site_id INT DEFAULT '10',
    city_code SMALLINT,
    user_id INT,
    pv BIGINT DEFAULT '0'
)
DUPLICATE KEY(site_id, city_code)
DISTRIBUTED BY HASH(site_id);
```

> **注意**
>
> 自 2.5.7 版本起，StarRocks 支持在建表和新增分区时自动设置分桶数量 (BUCKETS)，您无需手动设置分桶数量。更多信息，请参见 [确定分桶数量](./Data_distribution.md#确定分桶数量)。

### 聚合模型

创建一个名为 `site_access_aggregate` 的聚合模型表，包含 `site_id`、`city_code`、`user_id` 和 `pv` 四列，其中 `site_id` 和 `city_code` 为排序列。

建表语句如下：

```SQL
CREATE TABLE site_access_aggregate
(
    site_id INT DEFAULT '10',
    city_code SMALLINT,
    user_id BITMAP BITMAP_UNION,
    pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(site_id, city_code)
DISTRIBUTED BY HASH(site_id);
```

>**注意**
>
> 聚合模型表中，如果某列未指定 `agg_type`，则该列为 Key 列；如果某列指定了 `agg_type`，则该列为 Value 列。参见 [CREATE TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md)。上述示例指定排序列为 `site_id` 和 `city_code`，因此必须给 `user_id` 和 `pv` 列分别指定 `agg_type`。

### 更新模型

创建一个名为 `site_access_unique` 的更新模型表，包含 `site_id`、`city_code`、`user_id` 和 `pv` 四列，其中 `site_id` 和 `city_code` 为排序列。

建表语句如下：

```SQL
CREATE TABLE site_access_unique
(
    site_id INT DEFAULT '10',
    city_code SMALLINT,
    user_id INT,
    pv BIGINT DEFAULT '0'
)
UNIQUE KEY(site_id, city_code)
DISTRIBUTED BY HASH(site_id);
```

### 主键模型

创建一个名为 `site_access_primary` 的主键模型表，包含 `site_id`、`city_code`、`user_id` 和 `pv` 四列，其中 `site_id` 为主键列，`site_id` 和 `city_code` 为排序列。

建表语句如下：

```SQL
CREATE TABLE site_access_primary
(
    site_id INT DEFAULT '10',
    city_code SMALLINT,
    user_id INT,
    pv BIGINT DEFAULT '0'
)
PRIMARY KEY(site_id)
DISTRIBUTED BY HASH(site_id)
ORDER BY(site_id,city_code);
```

## 排序效果

以上述建表语句为例，排序效果可以分为三种情况：

- 如果查询条件只包含 `site_id` 和 `city_code` 两列，如下所示，则可以大幅减少查询过程中需要扫描的数据行：

  ```Plain
  select sum(pv) from site_access_duplicate where site_id = 123 and city_code = 2;
  ```

- 如果查询条件只包含 `site_id` 一列，如下所示，可以定位到只包含 `site_id` 的数据行：

  ```Plain
  select sum(pv) from site_access_duplicate where site_id = 123;
  ```

- 如果查询条件只包含 `city_code` 一列，如下所示，则需要扫描所有数据行，排序效果大打折扣：

  ```Plain
  select sum(pv) from site_access_duplicate where city_code = 2;
  ```

  > **说明**
  >
  > 这种情况下，排序列无法实现应有的排序效果。

在第一种情况下，为了定位到数据行的位置，需进行二分查找，以找到指定区间。如果数据行非常多，直接对 `site_id` 和 `city_code` 两列进行二分查找，需要把两列数据都加载到内存中，这样会消耗大量的内存空间。这时候您可以使用前缀索引来减少缓存的数据量、并有效加速查询。

另外，在实际业务场景中，如果指定的排序列非常多，也会占用大量内存。为了避免这种情况，StarRocks 对前缀索引做了如下限制:

- 前缀索引项的内容只能由数据块中第一行的排序列的前缀组成。

- 前缀索引列的数量不能超过 3。

- 前缀索引项的长度不能超过 36 字节。

- 前缀索引中不能包含 FLOAT 或 DOUBLE 类型的列。

- 前缀索引中 VARCHAR 类型的列只能出现一次，并且处在末尾位置。

- 当前缀索引的末尾列是 CHAR 或 VARCHAR 类型时，前缀索引项的长度不会超过 36 字节。

## 选择排序列

这里以 `site_access_duplicate` 表为例，介绍如何选择排序列。

- 经常作为查询条件的列，建议选为排序列。

- 当排序键涉及多个列的时候，建议把区分度高、且经常查询的列放在前面。

  区分度高的列是指取值个数较多、且持续增加的列。例如，在上述 `site_access_duplicate` 表中，因为城市的数目是固定的，所以 `city_code` 列中取值的个数是固定的，而 `site_id` 列中取值的个数要比 `city_code` 列中取值的个数大得多、并且还会不断地增加，所以 `site_id` 列的区分度就比 `city_code` 列要高不少。

- 排序键不应该包含过多的列。选择很多排序列并不有助于提升查询性能，而且会增大排序的开销，进而增加数据导入的开销。

综上所述，在为 `site_access_duplicate` 表选择排序列时，需要注意以下三点：

- 如果需要经常按 `site_id` 列加 `city_code` 列的组合进行查询，建议选择 `site_id` 列作为排序键的第一列。

- 如果需要经常按 `city_code` 列进行查询、偶尔按 `site_id` 列加 `city_code` 列的组合进行查询，建议选择 `city_code` 列作为排序键的第一列。

- 极端情况下，如果按 `site_id` 列加 `city_code` 列组合查询所占的比例与按 `city_code` 列单独查询所占的比例不相上下，可以创建一个以 `city_code` 列为第一列的 Rollup 表。Rollup 表会为 `city_code` 列再建一个排序索引 (Sort Index)。

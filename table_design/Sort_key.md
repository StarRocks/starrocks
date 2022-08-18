# 排序键和前缀索引

为加速查询，StarRocks 在内部组织和存储数据时，会把表中数据按照指定的一个或多个列进行排序。这些用于排序的列，称之为排序键 (Sort Key)。

为减少内存开销，StarRocks 在排序键的基础上又引入了前缀索引 (Prefix Index)。表中每 1024 行数据构成一个逻辑数据块 (Data Block)。每个逻辑数据块在前缀索引表中存储一项索引，内容为表的维度列的前缀，并且长度不超过 36 字节。前缀索引是一种稀疏索引，使用某行数据的维度列的前缀查找索引表，可以确定该行数据所在逻辑数据块的起始行号。前缀索引的大小会比数据量少 1024 倍，因此会全量缓存在内存中，在实际查找的过程中可以有效加速查询。

## 排序原理

在明细模型中，排序列就是指定的用于排序的列，即 Duplicate Key 指定的列。在聚合模型中，排序列就是用于聚合的列，即 Aggregate Key 指定的列。在更新模型中，排序列就是指定的满足唯一性约束的列，即 Unique Key 指定的列。示例 1 中，三个建表语句中的排序列都是 `site_id` 和 `city_code`。

示例 1：三种表模型分别对应的排序列

```SQL
CREATE TABLE site_access_duplicate
(
site_id INT DEFAULT '10',
city_code SMALLINT,
user_name VARCHAR(32) DEFAULT '',
pv BIGINT DEFAULT '0'
)
DUPLICATE KEY(site_id, city_code)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;

CREATE TABLE site_access_aggregate
(
site_id INT DEFAULT '10',
city_code SMALLINT,
pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(site_id, city_code)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;

CREATE TABLE site_access_unique
(
site_id INT DEFAULT '10',
city_code SMALLINT,
user_name VARCHAR(32) DEFAULT '',
pv BIGINT DEFAULT '0'
)
UNIQUE KEY(site_id, city_code)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;
```

在上述示例 1 中，各表的数据都按照 `site_id` 和 `city_code` 两列进行排序。注意以下两点：

- 建表语句中排序列必须定义在其他列之前。上述建表语句所创建的三个表的排序列可以是 `site_id` 和 `city_code`，也可以是 `site_id`、`city_code` 和 `user_name`，但不能是 `city_code` 和 `user_name`，也不能是 `site_id`、`city_code` 和 `pv`。

- 排序键中指定的排序列的顺序由建表语句中定义的列的顺序决定。建表语句中 `DUPLICATE KEY`、`UNIQUE KEY` 和 `AGGREGATE KEY` 所指定的排序列的顺序必须和建表语句中定义的列的顺序保持一致。以 `site_access_duplicate` 表为例，正确的和错误的建表语句如示例 2 所示。

示例 2：正确的建表语句和错误的建表语句

```SQL
-- 错误的建表语句
CREATE TABLE site_access_duplicate
(
site_id INT DEFAULT '10',
city_code SMALLINT,
user_name VARCHAR(32) DEFAULT '',
pv BIGINT DEFAULT '0'
)
DUPLICATE KEY(city_code, site_id)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;

-- 正确的建表语句
CREATE TABLE site_access_duplicate
(
    site_id INT DEFAULT '10',
    city_code SMALLINT,
    user_name VARCHAR(32) DEFAULT '',
    pv BIGINT DEFAULT '0'
)
DUPLICATE KEY(site_id, city_code)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;
```

## 排序效果

以上述示例 1 中的建表语句为例，排序效果可以分为三种情况：

- 如果查询条件只包含 `site_id` 和 `city_code` 两列，如下所示，则可以大幅减少查询过程中需要扫描的数据行：

  ```plain text
    select sum(pv) from site_access_duplicate where site_id = 123 and city_code = 2;
  ```

- 如果查询条件只包含 `site_id` 一列，如下所示，可以定位到只包含 `site_id` 的数据行

  ```plain text
    select sum(pv) from site_access_duplicate where site_id = 123;
  ```

- 如果查询条件只包含 `city_code` 一列，如下所示，则需要扫描所有数据行，排序效果大打折扣：

  ```plain text
    select sum(pv) from site_access_duplicate where city_code = 2;
  ```

在第一种情况下，为了定位到数据行的位置，需进行二分查找，以找到指定区间。如果数据行非常多，直接对 `site_id` 和 `city_code` 两列进行二分查找，需要把两列数据都加载到内存中，这样会消耗大量的内存空间。这时候您可以使用前缀索引来减少缓存的数据量、并有效加速查询。另外，在实际业务场景中，如果指定的排序列非常多，也会占用大量内存，为了避免这种情况，StarRocks 对前缀索引做了如下限制:

- 前缀索引列只能是排序键的前缀。

- 前缀索引列的数量不能超过 3。

- 前缀索引的长度不能超过 36 字节。

- 前缀索引中不能包含 FLOAT 或 DOUBLE 类型的列。

- 前缀索引中 VARCHAR 类型的列只能出现一次，并且处在末尾位置。

- 当前缀索引的末尾列是 CHAR 或 VARCHAR 类型时，前缀索引的长度不会超过 36 字节。

## 选择排序列

从上面的介绍可以看出，如果在查询 `site_access_duplicate` 表时只选择 `city_code` 列作为查询条件，排序列相当于失去了功效。因此，排序列的选择是和查询模式息息相关的。经常作为查询条件的列，建议选为排序列。

当排序键涉及多个列的时候，建议把区分度高、且经常查询的列放在前面。比如，在上述 `site_access_duplicate` 表中，因为城市的数目是固定的，所以 `city_code` 列中取值的个数是固定的，而 `site_id` 列中取值的个数要比 `city_code` 列中取值的个数大得多、并且还会不断地增加，所以 `site_id` 列的区分度就比 `city_code` 列要高不少。

还是以上述 `site_access_duplicate` 表为例，注意以下三点：

- 如果需要经常按 `site_id` 列加 `city_code` 列的组合进行查询，建议选择 `site_id` 列作为排序键的第一列。

- 如果需要经常按 `city_code` 列进行查询、偶尔按 `site_id` 列加 `city_code` 列的组合进行查询，建议选择 `city_code` 列作为排序键的第一列。

- 极端情况下，如果按 `site_id` 列加 `city_code` 列组合查询所占的比例与按 `city_code` 列单独查询所占的比例不相上下，可以创建一个以 `city_code` 列为第一列的 Rollup 表。Rollup 表会为 `city_code` 列再建一个排序索引 (Sort Index)。

## 使用说明

由于 StarRocks 的前缀索引大小固定不能超过 36 字节，所以不会发生内存膨胀的问题。实际使用过程中，需要注意以下四点：

- 排序键中所包含的排序列必须是从第一列开始、并且是连续的。

- 排序键中排序列的顺序是由建表语句中列的顺序决定的。

- 排序键不应该包含过多的列。如果排序键包含的列过多，那么排序的开销会导致数据导入的开销增加。

- 在大多数情况下，选择表的前几列作为排序列，也能很准确地定位到数据行所在的区间，选择很多排序列并不有助于提升查询性能。

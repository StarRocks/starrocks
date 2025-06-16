---
displayed_sidebar: docs
---

import Beta from '../../../_assets/commonMarkdown/_beta.mdx'

# CREATE DICTIONARY

<Beta />

基于原始对象创建字典对象。字典对象以哈希表的形式组织原始对象的键值映射，并缓存于所有 BE 节点的内存中。可以将其视为一个缓存表。

**优势**

- **更丰富的字典对象原始对象**：使用 `dictionary_get()` 查询字典对象时，原始对象可以是任意类型的表、异步物化视图或逻辑视图。然而，使用 `dict_mapping()` 查询字典表时，字典表只能是主键表。
- **查询速度快**：由于字典对象是一个哈希表，并且完全缓存于所有 BE 节点的内存中，查询字典对象以获取映射是通过在内存中查找哈希表实现的。因此，查询速度非常快。
- **支持多值列**：在内部，字典对象将多个值列编码为一个单一的 STRUCT 类型列。对于基于键的查询，多个值会一起返回。因此，字典对象可以作为一个维度表，其中每个键（通常是唯一标识符）对应多个值（描述性属性）。
- **确保一致的快照读取**：在同一事务中获得的字典快照是一致的，确保在同一查询或导入过程中，字典对象的查询结果不会改变。

## 语法

```SQL
CREATE DICTIONARY <dictionary_object_name> USING <dictionary_source>
(
    column_name KEY, [..., column_name KEY,]
    column_name VALUE[, ..., column_name VALUE]
)
[PROPERTIES ("key"="value", ...)];
```

## 参数

- `dictionary_object_name`：字典对象的名称。字典对象在全局范围内有效，不属于特定数据库。
- `dictionary_source`：字典对象所基于的原始对象的名称。原始对象可以是任意类型的表、异步物化视图或逻辑视图。
- 字典对象中的列定义：为了保持字典表中的键值映射，需要在字典对象的列中使用 `KEY` 和 `VALUE` 关键字来指定键及其映射值。
  - 字典对象中的列名 `column_name` 必须与字典表中的一致。
  - 字典对象中键和值列的数据类型限制为布尔型、整数型、字符串型和日期型。
  - 原始对象中的键列必须确保唯一性。
- 字典对象的相关属性 (`PROPERTIES`)：
  - `dictionary_warm_up`：将数据缓存到每个 BE 节点上的字典对象的方法。有效值：`TRUE`（默认）或 `FALSE`。如果参数设置为 `TRUE`，则在创建字典对象后，数据会自动缓存到字典对象中；如果参数设置为 `FALSE`，则需要手动刷新字典对象以缓存数据。
  - `dictionary_memory_limit`：字典对象在每个 BE 节点上可以占用的最大内存。单位：字节。默认值：2,000,000,000 字节（2 GB）。
  - `dictionary_refresh_interval`：定期刷新字典对象的间隔。单位：秒。默认值：`0`。值 `<=0` 表示不自动刷新。
  - `dictionary_read_latest`：是否仅查询最新的字典对象，主要影响刷新期间查询的字典对象。有效值：`TRUE` 或 `FALSE`（默认）。如果参数设置为 `TRUE`，则在刷新期间无法查询字典对象，因为最新的字典对象仍在刷新中。如果参数设置为 `FALSE`，则在刷新期间可以查询之前成功缓存的字典对象。
  - `dictionary_ignore_failed_refresh`：刷新失败时是否自动回滚到上次成功缓存的字典对象。有效值：`TRUE` 或 `FALSE`（默认）。如果参数设置为 `TRUE`，则在刷新失败时自动回滚到上次成功缓存的字典对象。如果参数设置为 `FALSE`，则在刷新失败时字典对象状态设置为 `CANCELLED`。

## 使用注意事项

- 字典对象完全缓存于每个 BE 节点的内存中，因此会消耗相对较多的内存。
- 即使原始对象被删除，基于其创建的字典对象仍然存在。需要手动 DROP 字典对象。

## 示例

**示例 1：创建一个简单的字典对象以替换原始字典表。**

以以下字典表为例并插入测试数据。

```Plain
MySQL > CREATE TABLE dict (
    order_uuid STRING,
    order_id_int BIGINT AUTO_INCREMENT 
)
PRIMARY KEY (order_uuid)
DISTRIBUTED BY HASH (order_uuid);
Query OK, 0 rows affected (0.02 sec)
MySQL > INSERT INTO dict (order_uuid) VALUES ('a1'), ('a2'), ('a3');
Query OK, 3 rows affected (0.12 sec)
{'label':'insert_9e60b0e4-89fa-11ee-a41f-b22a2c00f66b', 'status':'VISIBLE', 'txnId':'15029'}
MySQL > SELECT * FROM dict;
+------------+--------------+
| order_uuid | order_id_int |
+------------+--------------+
| a1         |            1 |
| a2         |            2 |
| a3         |            3 |
+------------+--------------+
3 rows in set (0.01 sec)
```

基于此字典表中的映射创建字典对象。

```Plain
MySQL > CREATE DICTIONARY dict_obj USING dict
    (order_uuid KEY,
     order_id_int VALUE);
Query OK, 0 rows affected (0.00 sec)
```

对于将来查询字典表中的映射，可以直接查询字典对象而不是字典表。例如，查询键 `a1` 映射的值。

```Plain
MySQL > SELECT dictionary_get("dict_obj", "a1");
+--------------------+
| DICTIONARY_GET     |
+--------------------+
| {"order_id_int":1} |
+--------------------+
1 row in set (0.01 sec)
```

**示例 2：创建一个字典对象以替换原始维度表**

以以下维度表为例并插入测试数据。

```Plain
MySQL > CREATE TABLE ProductDimension (
    ProductKey BIGINT AUTO_INCREMENT,
    ProductName VARCHAR(100) NOT NULL,
    Category VARCHAR(50),
    SubCategory VARCHAR(50),
    Brand VARCHAR(50),
    Color VARCHAR(20),
    Size VARCHAR(20)
)
PRIMARY KEY (ProductKey)
DISTRIBUTED BY HASH (ProductKey);
MySQL > INSERT INTO ProductDimension (ProductName, Category, SubCategory, Brand, Color, Size)
VALUES
    ('T-Shirt', 'Apparel', 'Shirts', 'BrandA', 'Red', 'M'),
    ('Jeans', 'Apparel', 'Pants', 'BrandB', 'Blue', 'L'),
    ('Running Shoes', 'Footwear', 'Athletic', 'BrandC', 'Black', '10'),
    ('Jacket', 'Apparel', 'Outerwear', 'BrandA', 'Green', 'XL'),
    ('Baseball Cap', 'Accessories', 'Hats', 'BrandD', 'White', 'OneSize');
Query OK, 5 rows affected (0.48 sec)
{'label':'insert_e938481f-181e-11ef-a6a9-00163e19e14e', 'status':'VISIBLE', 'txnId':'50'}
MySQL > SELECT * FROM ProductDimension;
+------------+---------------+-------------+-------------+--------+-------+---------+
| ProductKey | ProductName   | Category    | SubCategory | Brand  | Color | Size    |
+------------+---------------+-------------+-------------+--------+-------+---------+
|          1 | T-Shirt       | Apparel     | Shirts      | BrandA | Red   | M       |
|          2 | Jeans         | Apparel     | Pants       | BrandB | Blue  | L       |
|          3 | Running Shoes | Footwear    | Athletic    | BrandC | Black | 10      |
|          4 | Jacket        | Apparel     | Outerwear   | BrandA | Green | XL      |
|          5 | Baseball Cap  | Accessories | Hats        | BrandD | White | OneSize |
+------------+---------------+-------------+-------------+--------+-------+---------+
5 rows in set (0.02 sec)
```

创建一个字典对象以替换原始维度表。

```Plain
MySQL > CREATE DICTIONARY dimension_obj USING ProductDimension 
    (ProductKey KEY,
     ProductName VALUE,
     Category VALUE,
     SubCategory VALUE,
     Brand VALUE,
     Color VALUE,
     Size VALUE);
Query OK, 0 rows affected (0.00 sec)
```

对于将来查询维度值，可以直接查询字典对象而不是维度表以获取维度值。例如，查询键 `1` 映射的值。

```Plain
MySQL > SELECT dictionary_get("dict_obj", "a1");
+--------------------+
| DICTIONARY_GET     |
+--------------------+
| {"order_id_int":1} |
+--------------------+
1 row in set (0.01 sec)
```
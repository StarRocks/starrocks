---
displayed_sidebar: docs
---

# CREATE  DICTIONARY



基于原始对象创建字典对象。字典对象会以哈希表的形式组织**原始对象中的键值对映射关系**，并缓存在所有 BE 节点的内存中，可以视为一张缓存表。

**优势**

- **字典对象所基于的原始对象更加丰富**：使用 `dictionary_get()` 查询字典对象，字典对象所基于的原始对象可以为所有表类型、异步物化视图和逻辑视图。然而使用 `dict_mapping()` 查询字典表时仅支持字典表为主键表。
- **查询字典对象的速度快**：由于字典对象是哈希表，并且全量缓存在所有 BE 的内存中，查询字典对象获得映射关系的操作是通过查找内存中哈希表完成的，因此查询字典对象速度非常快。
- **支持多个 value 列**：字典对象内部将多个 value 列编码为一个 STRUCT 类型的列，后续基于 key 查询时多个 value  可以一起返回。因此字典对象可以作为维度表，维度表中的每个 key（通常是一个唯一标识符）对应多个 value（描述性属性）。
- **确保一致的快照读取**：同一个事务中获取的字典快照是一致的。因此可以保证在同一个查询或者导入过程中，查询字典对象的结果是一致的，不会有变化。

## 语法

```SQL
CREATE DICTIONARY <dictionary_object_name> USING <dictionary_source>
(
    column_name KEY, [..., column_name KEY,]
    column_name VALUE[, ..., column_name VALUE]
)
[PROPERTIES ("key"="value", ...)];
```

## 参数说明

- `dictionary_object_name`：字典对象的名称。字典对象全局级别生效，不从属于某个数据库。
- `dictionary_source`：字典对象所基于原始对象的名称。原始对象可以为所有表类型、异步物化视图和逻辑视图。
- 字典对象中列的定义：为了保存字典表中维护好的键值对映射关系，您需要在字典对象的列中使用 `KEY` 和 `VALUE` 关键字指定键和其映射的值。并且需要注意：
  - 字典对象中的列名 `column_name` 需要和字典表中的列名保持一致。
  - 字典对象中 key 和 value 列的数据类型仅限于布尔类型、整数类型、字符串类型和日期类型。
  - 原始对象中必须确保 key 列的唯一性。
- 字典对象相关属性 `PROPERTIES`：
  - `dictionary_warm_up`：缓存数据至各个 BE 节点中字典对象的触发方式，取值：`TRUE` （默认）或 `FALSE` 。如果为 `TRUE`，则创建字典对象后，自动缓存数据至字典对象；如果为 `FALSE`，则需要您手动刷新字典对象，则才能缓存数据至字典对象。
  - `dictionary_memory_limit`：各个 BE 节点上字典对象可占用的最大内存，单位：Byte，默认为 2,000,000,000 Bytes（2 GB）。
  - `dictionary_refresh_interval`：周期性刷新字典对象的时间间隔，单位：秒，默认为 `0`，取值为 `<=0` 时表示不会自动刷新。
  - `dictionary_read_latest`：是否只查询最新的字典对象，主要影响刷新字典对象时所查询的字典对象，取值：`TRUE` 或 `FALSE`（默认）。如果设置为 `TRUE`，在刷新时无法查询字典对象，因为最新的字典对象还在刷新中。如果为设置为 `FALSE`，则在刷新时可以查询上一次成功缓存字典对象。
  - `dictionary_ignore_failed_refresh`：刷新失败是否自动回滚为前一次成功缓存的字典对象，取值：`TRUE` 或 `FALSE`（默认）。如果设置为 `TRUE`，在刷新失败时，则自动回滚为前一次成功缓存的字典对象。如果为 `FALSE`，在刷新失败时，则将字典对象状态设置为 `CANCELLED`。

## 使用说明

- 字典对象全量缓存在各个 BE 的内存中，比较消耗内存。
- 即使原始对象删除后，如果已经基于原始对象创建字典对象，则该字典对象依旧存在。需要您手动 DROP 字典对象。

## 示例

**示例一：创建一张简单的字典对象，替代原先的字典表。**

以如下字典表为例，并插入测试数据。

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
| a3         |            3 |
| a2         |            2 |
+------------+--------------+
3 rows in set (0.01 sec)
```

基于该字典表中的映射关系创建字典对象。

```Plain
MySQL > CREATE DICTIONARY dict_obj USING dict
    (order_uuid KEY,
     order_id_int VALUE);
Query OK, 0 rows affected (0.00 sec)
```

后续查询字典表中的映射关系时，无需查询字典表，直接查询字典对象即可获得映射值。例如查询 key  `a1` 映射的 value。

```Plain
MySQL > SELECT dictionary_get("dict_obj", "a1");
+--------------------+
| DICTIONARY_GET     |
+--------------------+
| {"order_id_int":1} |
+--------------------+
1 row in set (0.01 sec)
```

**示例二：创建字典对象，替代原先的维度表。**

以如下维度表为例，并插入测试数据。

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
|          4 | Jacket        | Apparel     | Outerwear   | BrandA | Green | XL      |
|          5 | Baseball Cap  | Accessories | Hats        | BrandD | White | OneSize |
|          2 | Jeans         | Apparel     | Pants       | BrandB | Blue  | L       |
|          3 | Running Shoes | Footwear    | Athletic    | BrandC | Black | 10      |
+------------+---------------+-------------+-------------+--------+-------+---------+
5 rows in set (0.02 sec)
```

创建字典对象替代原先的维度表。 

```Plain
MySQL > CREATE DICTIONARY dimension_obj USING ProductDimension 
    (ProductKey KEY,
     ProductName VALUE,
     Category value,
     SubCategory value,
     Brand value,
     Color value,
     Size value);
```

后续查询维度值时，无需查询维度表，直接查询字典对象即可获得维度值。例如查询 key  `1` 映射的 value。

```Plain
MySQL > SELECT dictionary_get("dimension_obj", "1");
+--------------------+
| DICTIONARY_GET     |
+--------------------+
| {"order_id_int":1} |
+--------------------+
1 row in set (0.01 sec)
```
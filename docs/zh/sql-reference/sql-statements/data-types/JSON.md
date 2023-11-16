---
displayed_sidebar: "Chinese"
---

# JSON

自 2.2.0 版本起，StarRocks 支持 JSON。本文介绍 JSON 的基本概念，以及 StarRocks 如何创建 JSON 类型的列、导入和查询 JSON 数据，通过 JSON 函数及运算符构造和处理 JSON 数据。

## 什么是 JSON

JSON 是一种轻量级的数据交换格式，JSON 类型的数据是一种半结构化的数据，支持树形结构。JSON 数据层次清晰，结构灵活易于阅读和处理，广泛应用于数据存储和分析场景。JSON 支持的数据类型为数字类型（NUMBER）、字符串类型（STRING）、布尔类型（BOOLEAN）、数组类型（ARRAY）、对象类型（OBJECT），以及 NULL 值。

JSON 的更多介绍，请参见 [JSON 官网](http://www.json.org/?spm=a2c63.p38356.0.0.50756b9fVEfwCd)，JSON 数据的输入和输出语法，请参见 JSON 规范 [RFC 7159](https://tools.ietf.org/html/rfc7159?spm=a2c63.p38356.0.0.14d26b9fcp7fcf#page-4) 。

StarRocks 支持存储和高效查询分析 JSON 数据。StarRocks 采用二进制格式编码来存储 JSON 数据，而不是直接存储所输入文本，因此在数据计算查询时，降低解析成本，从而提升查询效率。

## 使用 JSON 数据

### 创建 JSON 类型的列

建表时，通过关键字 `JSON`，指定列 `j` 为 JSON 类型。

```SQL
CREATE TABLE `tj` (
    `id` INT(11) NOT NULL COMMENT "",
    `j`  JSON NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
    "replication_num" = "3",
    "storage_format" = "DEFAULT"
);
```

### 导入数据并存储为 JSON 类型

StarRocks 支持如下三种方式导入数据并存储为 JSON 类型。

- 方式一：通过 `INSERT INTO` 将数据写入至 JSON 类型的列（例如列 `j`）。

```SQL
INSERT INTO tj (id, j) VALUES (1, parse_json('{"a": 1, "b": true}'));
INSERT INTO tj (id, j) VALUES (2, parse_json('{"a": 2, "b": false}'));
INSERT INTO tj (id, j) VALUES (3, parse_json('{"a": 3, "b": true}'));
INSERT INTO tj (id, j) VALUES (4, json_object('a', 4, 'b', false)); 
```

> PARSE_JSON 函数能够基于字符串类型的数据构造出 JSON 类型的数据。JSON_OBJECT 函数能够构造出 JSON 对象类型的数据，可以将现有的表转成 JSON 类型。更多说明，请参见 [PARSE_JSON](../../sql-functions/json-functions/json-constructor-functions/parse_json.md) 和 [JSON_OBJECT](../../sql-functions/json-functions/json-constructor-functions/json_object.md)。

- 方式二：通过 Stream Load 的方式导入 JSON 文件并存储为 JSON 类型。导入方法请参见 [导入 JSON 数据](../../../loading/StreamLoad.md)。
  - 如果需要将 JSON 文件中根节点的 JSON 对象导入并存储为 JSON 类型，可设置 `jsonpaths` 为 `$`。
  - 如果需要将 JSON 文件中一个 JSON 对象的值 (value) 导入并存储为 JSON 类型，可设置 `jsonpaths` 为 `$.a`（a 代表 key）。更多 JSON 路径表达式，参见 [JSON path](../../sql-functions/json-functions/overview-of-json-functions-and-operators.md#json-path)。
  
- 方式三：通过 Broker Load 的方式导入 Parquet 文件并存储为 JSON 类型。导入方式，请参见 [Broker Load](../data-manipulation/BROKER_LOAD.md)。

导入时支持数据类型转换如下：

| Parquet 文件中的数据类型                                     | 转换后的 JSON 数据类型 |
| ------------------------------------------------------------ | -------------------- |
| 整数类型（INT8、INT16、INT32、INT64、UINT8、UINT16、UINT32、UINT64） | JSON 数字型          |
| 浮点类型（FLOAT、DOUBLE）                                    | JSON 数字型          |
| BOOLEAN                                                      | JSON 布尔型          |
| STRING                                                       | JSON 字符串型        |
| MAP                                                          | JSON 对象型          |
| STRUCT                                                       | JSON 对象型          |
| LIST                                                         | JSON 数组型          |
| UNION、TIMESTAMP 等其他类型                                  | 暂未支持             |

### 查询和处理 JSON 类型的数据

StarRocks 支持查询和处理 JSON 类型的数据，并且支持使用 JSON 函数和运算符。

本示例以表 tj 进行说明。

```Plain Text
mysql> select * from tj;
+------+----------------------+
| id   |          j           |
+------+----------------------+
| 1    | {"a": 1, "b": true}  |
| 2    | {"a": 2, "b": false} |
| 3    | {"a": 3, "b": true}  |
| 4    | {"a": 4, "b": false} |
+------+----------------------+
```

示例一：按照过滤条件 `id=1`，筛选出 JSON 类型的列中满足条件的数据。

```Plain Text
mysql> select * from tj where id = 1;
+------+---------------------+
| id   |           j         |
+------+---------------------+
| 1    | {"a": 1, "b": true} |
+------+---------------------+
```

示例二：根据 JSON 类型的列进行过滤，过滤出表中满足条件的数据。

> 以下示例中 `j->'a'` 返回的是 JSON 类型的数据，您可以使用第一个示例进行对比，该示例对数据进行了隐式转换；也可以使用 CAST 函数将 JSON 类型数据构造为 INT，然后进行对比。

```Plain Text
mysql> select * from tj where j->'a' = 1;
+------+---------------------+
| id   | j                   |
+------+---------------------+
|    1 | {"a": 1, "b": true} |
+------+---------------------+

mysql> select * from tj where cast(j->'a' as INT) = 1; 
+------+---------------------+
|   id |         j           |
+------+---------------------+
|   1  | {"a": 1, "b": true} |
+------+---------------------+
```

示例三：根据 JSON 类型的列进行过滤（您可以使用 CAST 函数将 JSON 类型的列构造为 BOOLEAN 类型），过滤出表中满足条件的数据。

```Plain Text
mysql> select * from tj where cast(j->'b' as boolean);
+------+---------------------+
|  id  |          j          |
+------+---------------------+
| 1    | {"a": 1, "b": true} |
| 3    | {"a": 3, "b": true} |
+------+---------------------+
```

示例四：根据 JSON 类型的列进行过滤（您可以使用 CAST 函数将 JSON 类型的列构造为 BOOLEAN 类型），过滤出 JSON 类型的列满足条件的数据，并进行数值运算。

```Plain Text
mysql> select cast(j->'a' as int) from tj where cast(j->'b' as boolean);
+-----------------------+
|  CAST(j->'a' AS INT)  |
+-----------------------+
|          3            |
|          1            |
+-----------------------+

mysql> select sum(cast(j->'a' as int)) from tj where cast(j->'b' as boolean);
+----------------------------+
| sum(CAST(j->'a' AS INT))  |
+----------------------------+
|              4             |
+----------------------------+
```

示例五：按照 JSON 类型的列进行排序。

```Plain Text
mysql> select * from tj
       where j->'a' <= parse_json('3')
       order by cast(j->'a' as int);
+------+----------------------+
| id   | j                    |
+------+----------------------+
|    1 | {"a": 1, "b": true}  |
|    2 | {"a": 2, "b": false} |
|    3 | {"a": 3, "b": true}  |
|    4 | {"a": 4, "b": false} |
+------+----------------------+
```

## JSON 函数和运算符

JSON 函数和运算符可以用于构造和处理 JSON 数据。具体说明，请参见 [JSON 函数和运算符](../../sql-functions/json-functions/overview-of-json-functions-and-operators.md)。

## 限制和注意事项

- 当前 JSON 类型数据支持的最大长度为 16 MB。
- ORDER BY、GROUP BY、JOIN 子句不支持引用 JSON 类型的列。如果需要引用，您可以提前使用 CAST 函数，将 JSON 类型的列转为其他 SQL 类型。具体转换方式，请参见 [JSON 类型转换](../../sql-functions/json-functions/json-query-and-processing-functions/cast.md)。
- 支持 JSON 类型的列存在于明细模型、主键模型、更新模型的表中，但不支持存在于聚合模型的表中。
- 暂不支持 JSON 类型的列作为明细模型、主键模型、更新模型表的分区键、分桶键、维度列，并且不支持用于 JOIN、GROUP BY、ORDER BY 子句。
- StarRocks 支持使用 `<`，`<=`，`>`，`>=`， `=`，`!=` 运算符查询 JSON 数据，不支持使用 IN 运算符。

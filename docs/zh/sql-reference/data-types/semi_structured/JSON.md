---
displayed_sidebar: docs
---

# JSON

自 2.2.0 版本起，StarRocks 支持 JSON。本文介绍 JSON 的基本概念，以及 StarRocks 如何创建 JSON 类型的列、导入和查询 JSON 数据，通过 JSON 函数及运算符构造和处理 JSON 数据。

## 什么是 JSON

JSON 是一种轻量级的数据交换格式，JSON 类型的数据是一种半结构化的数据，支持树形结构。JSON 数据层次清晰，结构灵活易于阅读和处理，广泛应用于数据存储和分析场景。JSON 支持的数据类型为数字类型（NUMBER）、字符串类型（STRING）、布尔类型（BOOLEAN）、数组类型（ARRAY）、对象类型（OBJECT），以及 NULL 值。

JSON 的更多介绍，请参见 [JSON 官网](https://www.json.org/json-en.html)，JSON 数据的输入和输出语法，请参见 JSON 规范 [RFC 7159](https://tools.ietf.org/html/rfc7159?spm=a2c63.p38356.0.0.14d26b9fcp7fcf#page-4) 。

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

StarRocks 支持如下方式导入数据并存储为 JSON 类型。

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
  
- 方式三：通过 Broker Load 的方式导入 Parquet 文件并存储为 JSON 类型。导入方式，请参见 [Broker Load](../../sql-statements/loading_unloading/BROKER_LOAD.md)。

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

- 方式四：通过 [Routine Load](../../../loading/RoutineLoad.md#导入-json-数据) 持续消费 Kafka 中的 JSON 格式数据，并导入至 StarRocks 中。

### 查询和处理 JSON 类型的数据

StarRocks 支持查询和处理 JSON 类型的数据，并且支持使用 JSON 函数和运算符。

本示例以表 `tj` 进行说明。

```SQL
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

```SQL
mysql> select * from tj where id = 1;
+------+---------------------+
| id   |           j         |
+------+---------------------+
| 1    | {"a": 1, "b": true} |
+------+---------------------+
```

示例二：根据 JSON 类型的列进行过滤，过滤出表中满足条件的数据。

> 以下示例中 `j->'a'` 返回的是 JSON 类型的数据，您可以使用第一个示例进行对比，该示例对数据进行了隐式转换；也可以使用 CAST 函数将 JSON 类型数据构造为 INT，然后进行对比。

```SQL
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

```SQL
mysql> select * from tj where cast(j->'b' as boolean);
+------+---------------------+
|  id  |          j          |
+------+---------------------+
| 1    | {"a": 1, "b": true} |
| 3    | {"a": 3, "b": true} |
+------+---------------------+
```

示例四：根据 JSON 类型的列进行过滤（您可以使用 CAST 函数将 JSON 类型的列构造为 BOOLEAN 类型），过滤出 JSON 类型的列满足条件的数据，并进行数值运算。

```SQL
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

```SQL
mysql> select * from tj
       where j->'a' <= 3
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

## JSON Array 

JSON 可以包含嵌套数据，例如 Array 中可以嵌套 Object、Array 或其他 JSON 数据类型。StarRocks 提供了丰富的函数和操作符来处理这些复杂的嵌套 JSON 数据结构。以下将介绍如何处理包含数组的 JSON 数据。

假设 events 表中有一个 JSON 字段 event_data，其内容如下：
```
{
  "user": "Alice",
  "actions": [
    {"type": "click", "timestamp": "2024-03-17T10:00:00Z", "quantity": 1},
    {"type": "view", "timestamp": "2024-03-17T10:05:00Z", "quantity": 2},
    {"type": "purchase", "timestamp": "2024-03-17T10:10:00Z", "quantity": 3}
  ]
}
```

以下示例将展示几种常见的 JSON 数组分析场景：

1. 提取数组元素：从 actions 数组中提取特定字段，如 type、timestamp 等，并进行投影操作
2. 数组展开：使用 json_each 函数将嵌套的 JSON 数组展开成多行多列的表格结构，便于后续分析
3. 数组计算：结合 Array Functions 对数组元素进行过滤、转换和聚合计算，如统计特定类型的操作次数

### 1. 提取 JSON 数组的元素

如果要提取 JSON Array 中的某个嵌套元素，可以使用如下语法：
- 其返回类型仍然是 JSON Array，可以使用 CAST 表达式进行类型转换
```
MySQL > SELECT json_query(event_data, '$.actions[*].type') as json_array FROM events;
+-------------------------------+
| json_array                    |
+-------------------------------+
| ["click", "view", "purchase"] |
+-------------------------------+

MySQL > SELECT cast(json_query(event_data, '$.actions[*].type') as array<string>) array_string FROM events;
+-----------------------------+
| array_string                |
+-----------------------------+
| ["click","view","purchase"] |
+-----------------------------+
```

### 2. 使用 json_each 函数展开
StarRocks 提供 `json_each` 函数进行 JSON 数组的展开，使其转换为多行数据。例如：
```
MySQL > select value from events, json_each(event_data->'actions');
+--------------------------------------------------------------------------+
| value                                                                    |
+--------------------------------------------------------------------------+
| {"quantity": 1, "timestamp": "2024-03-17T10:00:00Z", "type": "click"}    |
| {"quantity": 2, "timestamp": "2024-03-17T10:05:00Z", "type": "view"}     |
| {"quantity": 3, "timestamp": "2024-03-17T10:10:00Z", "type": "purchase"} |
+--------------------------------------------------------------------------+
```

如果需要将 type 和 timestamp 字段分别提取：
```
MySQL > select value->'timestamp', value->'type' from events, json_each(event_data->'actions');
+------------------------+---------------+
| value->'timestamp'     | value->'type' |
+------------------------+---------------+
| "2024-03-17T10:00:00Z" | "click"       |
| "2024-03-17T10:05:00Z" | "view"        |
| "2024-03-17T10:10:00Z" | "purchase"    |
+------------------------+---------------+
```

在此之后，JSON Array 的数据即变成我们熟悉的关系模型，可以使用常用的函数进行分析。

### 3. 使用 Array Functions 来进行筛选和计算
StarRocks 还支持 ARRAY 相关函数，与 JSON 函数结合使用可以实现更高效的查询。通过组合使用这些函数，您可以对 JSON 数组进行过滤、转换和聚合计算。以下示例展示了如何使用这些函数：
```
MySQL > 
WITH step1 AS (
 SELECT cast(event_data->'actions' as ARRAY<JSON>) as docs
   FROM events
)
SELECT array_filter(doc -> get_json_string(doc, 'type') = 'click', docs) as clicks
FROM step1
+---------------------------------------------------------------------------+
| clicks                                                                    |
+---------------------------------------------------------------------------+
| ['{"quantity": 1, "timestamp": "2024-03-17T10:00:00Z", "type": "click"}'] |
+---------------------------------------------------------------------------+
```

更进一步，可以结合其他的 ARRAY Function 对数组元素进行聚合计算：
```
MySQL > 
WITH step1 AS (
 SELECT cast(event_data->'actions' as ARRAY<JSON>) as docs
   FROM events
), step2 AS (
    SELECT array_filter(doc -> get_json_string(doc, 'type') = 'click', docs) as clicks
    FROM step1
)
SELECT array_sum(
            array_map(doc -> get_json_double(doc, 'quantity'), clicks)
            ) as click_amount
FROM step2
+--------------+
| click_amount |
+--------------+
| 1.0          |
+--------------+
```

## 限制和注意事项

- 当前 JSON 类型数据支持的最大长度为 16 MB。
- ORDER BY、GROUP BY、JOIN 子句不支持引用 JSON 类型的列。如果需要引用，您可以提前使用 CAST 函数，将 JSON 类型的列转为其他 SQL 类型。具体转换方式，请参见 [JSON 类型转换](../../sql-functions/json-functions/json-query-and-processing-functions/cast.md)。
- 支持 JSON 类型的列存在于明细表、主键表、更新表中，但不支持存在于聚合表中。
- 暂不支持 JSON 类型的列作为明细表、主键表、更新表的分区键、分桶键、维度列，并且不支持用于 JOIN、GROUP BY、ORDER BY 子句。
- StarRocks 支持使用 `<`，`<=`，`>`，`>=`， `=`，`!=` 运算符查询 JSON 数据，不支持使用 IN 运算符。

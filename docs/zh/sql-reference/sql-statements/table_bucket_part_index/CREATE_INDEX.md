---
displayed_sidebar: docs
keywords: ['suoyin']
---

# CREATE INDEX

## 功能

创建索引。

您可以创建以下索引：
- [Bitmap 索引](../../../table_design/indexes/Bitmap_index.md)
- [N-Gram bloom filter 索引](../../../table_design/indexes/Ngram_Bloom_Filter_Index.md)
- [全文倒排索引](../../../table_design/indexes/inverted_index.md)
- [向量索引](../../../table_design/indexes/vector_index.md)

有关创建这些索引的详细说明和示例，请参阅上述对应的教程。

:::tip

该操作需要对应表的 ALTER 权限。请参考 [GRANT](../account-management/GRANT.md) 为用户赋权。

:::

## 语法

```SQL
CREATE INDEX index_name ON table_name (column_name) 
[USING { BITMAP | NGRAMBF | GIN | VECTOR } ] 
[(index_property)] 
[COMMENT '<comment>']
```

## 参数说明

| **参数**    | **必选** | **说明**                                                     |
| ----------- | -------- | ------------------------------------------------------------ |
| index_name  | 是       | 索引名称，命名要求参见[系统限制](../../System_limit.md)。在同一张表中不能创建名称相同的索引。 |
| table_name  | 是       | 表名。                                                       |
| column_name | 是       | 创建索引的列名。执行一次该语句只能为某一列创建索引，且同一列只能创建一个索引。 |
| USING       | 否       | 要创建索引的类型。有效值：<ul><li>BITMAP (默认值)</li><li>NGRAMBF</li><li>GIN</li><li>VECTOR</li></ul> |
| index_property | 否    | 要创建的索引的属性。对于 `NGRAMBF`、`GIN` 和 `VECTOR`，您必须指定相应的属性。有关详细说明，请参阅相应的教程。 |
| COMMENT     | 否       | 索引备注。                                                   |

## 示例

例如有一张表 `sales_records`，其建表语句如下：

```SQL
CREATE TABLE sales_records
(
    record_id int,
    seller_id int,
    item_id int
)
DISTRIBUTED BY hash(record_id)
PROPERTIES (
    "replication_num" = "3"
);
```

为表 `sales_records` 中的 `item_id` 列创建 bitmap 索引，索引名称为 `index`。

```SQL
CREATE INDEX index ON sales_records (item_id) USING BITMAP COMMENT '';
```

或

```SQL
CREATE INDEX index ON sales_records (item_id);
```

## 相关操作

- 如要查看索引，参见 [SHOW INDEX](SHOW_INDEX.md)。
- 如要删除索引，参见 [DROP INDEX](DROP_INDEX.md)。

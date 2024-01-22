---
displayed_sidebar: "Chinese"
---

# CREATE INDEX

## 功能

创建索引，当前仅支持创建 bitmap 索引。有关 bitmap 索引的使用说明和适用场景，参见 [Bitmap 索引](../../../using_starrocks/Bitmap_index.md)。

## 语法

```SQL
CREATE INDEX index_name ON table_name (column_name) [USING BITMAP] [COMMENT'']
```

## 参数说明

| **参数**    | **必选** | **说明**                                                     |
| ----------- | -------- | ------------------------------------------------------------ |
| index_name  | 是       | 索引名称，命名要求如下：<ul><li>必须由字母(a-z或A-Z)、数字(0-9)或下划线(_)组成，且只能以字母开头。</li><li>总长度不能超过 64 个字符。</li></ul>在同一张表中不能创建名称相同的索引。 |
| table_name  | 是       | 表名。                                                       |
| column_name | 是       | 创建索引的列名。执行一次该语句只能为某一列创建索引，且同一列只能创建一个索引。 |
| COMMENT     | 否       | 索引备注。                                                   |

## 注意事项

- 主键表和明细表中所有列都可以创建 bitmap 索引；聚合表和更新表中，只有维度列（即 Key 列）支持创建 bitmap 索引。
- 不支持为 FLOAT、DOUBLE、BOOLEAN 和 DECIMAL 类型的列创建 bitmap 索引。

## 示例

例如有一张表`sales_records`，其建表语句如下：

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

为表`sales_records`中的`item_id`列创建 bitmap 索引，索引名称为`index3`。

```SQL
CREATE INDEX index3 ON sales_records (item_id) USING BITMAP COMMENT '';
```

或

```SQL
CREATE INDEX index3 ON sales_records (item_id);
```

## 相关操作

- 如要查看索引，参见 [SHOW INDEX](../Administration/SHOW_INDEX.md)。
- 如要删除索引，参见 [DROP INDEX](../data-definition/DROP_INDEX.md)。

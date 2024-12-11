---
displayed_sidebar: docs
keywords: ['suoyin']
---

# CREATE INDEX

## 功能

<<<<<<< HEAD
创建索引，当前仅支持创建 bitmap 索引。有关 bitmap 索引的使用说明和适用场景，参见 [Bitmap 索引](../../../using_starrocks/Bitmap_index.md)。

:::tip

该操作需要对应表的 ALTER 权限。请参考 [GRANT](../account-management/GRANT.md) 为用户赋权。
=======
创建索引。仅支持使用该语句创建 Bitmap 索引，有关 Bitmap 索引的使用说明和适用场景，参见 [Bitmap 索引](../../../table_design/indexes/Bitmap_index.md)。

:::tip

- 该操作需要对应表的 ALTER 权限。请参考 [GRANT](../account-management/GRANT.md) 为用户赋权。
- 一列只能创建一个 Bitmap index。如果某列已有 index，再次创建会返回失败。
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

:::

## 语法

```SQL
CREATE INDEX index_name ON table_name (column_name) [USING BITMAP] [COMMENT'']
```

## 参数说明

| **参数**    | **必选** | **说明**                                                     |
| ----------- | -------- | ------------------------------------------------------------ |
<<<<<<< HEAD
| index_name  | 是       | 索引名称，命名要求如下：<ul><li>必须由字母(a-z或A-Z)、数字(0-9)或下划线(_)组成，且只能以字母开头。</li><li>总长度不能超过 64 个字符。</li></ul>在同一张表中不能创建名称相同的索引。 |
=======
| index_name  | 是       | 索引名称，命名要求参见[系统限制](../../System_limit.md)。在同一张表中不能创建名称相同的索引。 |
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
| table_name  | 是       | 表名。                                                       |
| column_name | 是       | 创建索引的列名。执行一次该语句只能为某一列创建索引，且同一列只能创建一个索引。 |
| COMMENT     | 否       | 索引备注。                                                   |

<<<<<<< HEAD
## 注意事项

- 主键模型和明细模型中所有列都可以创建 bitmap 索引；聚合模型和更新模型中，只有维度列（即 Key 列）支持创建 bitmap 索引。
- 不支持为 FLOAT、DOUBLE、BOOLEAN 和 DECIMAL 类型的列创建 bitmap 索引。

## 示例

例如有一张表`sales_records`，其建表语句如下：
=======
## 示例

例如有一张表 `sales_records`，其建表语句如下：
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

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

<<<<<<< HEAD
为表 `sales_records` 中的 `item_id` 列创建 bitmap 索引，索引名称为`index3`。
=======
为表 `sales_records` 中的 `item_id` 列创建 bitmap 索引，索引名称为 `index3`。
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

```SQL
CREATE INDEX index3 ON sales_records (item_id) USING BITMAP COMMENT '';
```

或

```SQL
CREATE INDEX index3 ON sales_records (item_id);
```

## 相关操作

- 如要查看索引，参见 [SHOW INDEX](SHOW_INDEX.md)。
- 如要删除索引，参见 [DROP INDEX](DROP_INDEX.md)。

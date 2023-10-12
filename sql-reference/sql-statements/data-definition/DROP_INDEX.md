# DROP INDEX

## 功能

删除指定表的某个 bitmap 索引。创建 bitmap 索引会占用额外的存储空间，所以不用的索引建议删除。删除索引后，存储空间会立即释放。

## 语法

```SQL
DROP INDEX index_name ON [db_name.]table_name
```

## 参数说明

| **参数**   | **必选** | **说明**           |
| ---------- | -------- | ------------------ |
| index_name | 是       | 要删除的索引名称。 |
| table_name | 是       | 创建索引的表。     |
| db_name    | 否       | 表所属的数据库。   |

## 示例

例如为表`sales_records`中的`item_id`列创建位图索引，索引名称为`index3`。

```SQL
CREATE INDEX index3 ON sales_records (item_id);
```

删除表`sales_records`中的索引`index3`。

```SQL
DROP INDEX index3 ON sales_records;
```

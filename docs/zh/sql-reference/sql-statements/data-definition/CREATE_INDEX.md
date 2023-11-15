# CREATE INDEX

## 功能

该语句用于创建索引。

删除索引请参考 [DROP INDEX](../data-definition/DROP_INDEX.md) 章节。

## 语法

```sql
CREATE INDEX index_name ON table_name (column [, ...],) [USING BITMAP] [COMMENT'balabala'];
```

注：方括号 [] 中内容可省略不写。

注意：

1. 目前只支持 `Bitmap 索引`, 该索引适用的应用场景，数据模型及详细原理请参考 [Bitmap 索引](/table_design/Bitmap_index.md) 章节。
2. [Bitmap 索引](/table_design/Bitmap_index.md) 仅在单列上创建。

## 示例

1. 在 table1 上为 siteid 创建 bitmap 索引。

    ```sql
    CREATE INDEX index_name ON table1 (siteid) USING BITMAP COMMENT 'balabala';
    ```

## 关键字(keywords)

CREATE，INDEX

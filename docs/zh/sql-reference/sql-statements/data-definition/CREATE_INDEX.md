# CREATE INDEX

## description

该语句用于创建索引

语法：

```sql
CREATE INDEX index_name ON table_name (column [, ...],) [USING BITMAP] [COMMENT'balabala'];
```

注意：

<<<<<<< HEAD
1. 目前只支持bitmap 索引
2. BITMAP 索引仅在单列上创建
=======
1. 目前只支持 `Bitmap 索引`, 该索引适用的应用场景，数据模型及详细原理请参考 [Bitmap 索引](../../../table_design/Bitmap_index.md) 章节。
2. [Bitmap 索引](../../../table_design/Bitmap_index.md) 仅在单列上创建。
>>>>>>> a83aa885d ([Doc] fix links in 2.2 (#35221))

## example

1. 在table1 上为siteid 创建bitmap 索引

    ```sql
    CREATE INDEX index_name ON table1 (siteid) USING BITMAP COMMENT 'balabala';
    ```

## keyword

CREATE,INDEX

# CREATE INDEX

## description

该语句用于创建索引

语法：

```sql
CREATE INDEX index_name ON table_name (column [, ...],) [USING BITMAP] [COMMENT'balabala'];
```

注意：

1. 目前只支持bitmap 索引
2. BITMAP 索引仅在单列上创建

## example

1. 在table1 上为siteid 创建bitmap 索引

    ```sql
    CREATE INDEX index_name ON table1 (siteid) USING BITMAP COMMENT 'balabala';
    ```

## keyword

CREATE,INDEX

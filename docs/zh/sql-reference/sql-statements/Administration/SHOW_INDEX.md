---
displayed_sidebar: "Chinese"
---

# SHOW INDEX

## description

该语句用于展示一个表中索引的相关信息，目前只支持bitmap 索引

语法：

```sql
SHOW INDEX[ES] FROM [db_name.]table_name [FROM database];
或者
SHOW KEY[S] FROM [db_name.]table_name [FROM database];
```

## example

1. 展示指定 table_name 的下索引

    ```sql
    SHOW INDEX FROM example_db.table_name;
    ```

## keyword

SHOW,INDEX

---
displayed_sidebar: "Chinese"
---

# DROP VIEW

## 功能

该语句用于删除一个逻辑视图。

## 语法

```sql
DROP VIEW [IF EXISTS] [db_name.]view_name
```

## 示例

1. 如果存在，删除 example_db 上的逻辑视图 example_view。

    ```sql
    DROP VIEW IF EXISTS example_db.example_view;
    ```

## 相关操作

如要创建逻辑视图，请参考 [CREATE VIEW](../data-definition/CREATE_VIEW.md)。

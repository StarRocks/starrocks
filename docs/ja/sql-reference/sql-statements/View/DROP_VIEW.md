---
displayed_sidebar: docs
---

# ビューの削除

## 説明

このステートメントは、ビューを削除するために使用されます。

## 構文

```sql
DROP VIEW [IF EXISTS]
[db_name.]view_name
```

## 例

1. 存在する場合、example_db 上のビュー example_view を削除します。

    ```sql
    DROP VIEW IF EXISTS example_db.example_view;
    ```
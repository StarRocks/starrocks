---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "DROP VIEW deletes a logical view from StarRocks."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
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
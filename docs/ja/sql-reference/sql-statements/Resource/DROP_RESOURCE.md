---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "DROP RESOURCE removes an existing resource from StarRocks."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# DROP RESOURCE

## 説明

このステートメントは、既存のリソースを削除するために使用されます。リソースを削除できるのは、root または superuser のみです。

構文:

```sql
DROP RESOURCE 'resource_name'
```

## 例

1. Spark リソース spark0 を削除します。

    ```SQL
    DROP RESOURCE 'spark0';
    ```

2. Hive リソース hive0 を削除します。

    ```SQL
    DROP RESOURCE 'hive0';
    ```
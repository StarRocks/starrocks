---
displayed_sidebar: docs
---

# DROP RESOURCE

## 説明

このステートメントは、既存のリソースを削除するために使用されます。リソースを削除できるのは、root または superuser のみです。

構文:

```sql
DROP RESOURCE 'resource_name'
```

## 例

1. spark0 という名前の Spark リソースを削除します。

    ```SQL
    DROP RESOURCE 'spark0';
    ```

2. hive0 という名前の Hive リソースを削除します。

    ```SQL
    DROP RESOURCE 'hive0';
    ```
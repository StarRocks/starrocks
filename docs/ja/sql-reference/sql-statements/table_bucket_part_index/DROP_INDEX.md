---
displayed_sidebar: docs
---

# DROP INDEX

## 説明

このステートメントは、テーブル上の指定されたインデックスを削除するために使用されます。現在、このバージョンではビットマップインデックスのみがサポートされています。

:::tip

この操作には、対象テーブルに対する ALTER 権限が必要です。この権限を付与するには、[GRANT](../account-management/GRANT.md) の指示に従ってください。

:::

構文:

```sql
DROP INDEX index_name ON [db_name.]table_name
```
---
displayed_sidebar: docs
---

# DROP INDEX

## 説明

このステートメントは、テーブル上の指定されたインデックスを削除するために使用されます。現在、このバージョンではビットマップインデックスのみがサポートされています。

構文:

```sql
DROP INDEX index_name ON [db_name.]table_name
```
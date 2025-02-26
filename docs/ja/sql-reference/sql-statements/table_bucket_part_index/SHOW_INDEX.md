---
displayed_sidebar: docs
---

# SHOW INDEX

## 説明

このステートメントは、テーブル内のインデックスに関連する情報を表示するために使用されます。現在、ビットマップインデックスのみをサポートしています。

:::tip

この操作には特権は必要ありません。

:::

## 構文

```sql
SHOW INDEX[ES] FROM [db_name.]table_name [FROM database]
Or
SHOW KEY[S] FROM [db_name.]table_name [FROM database]
```

## 例

1. 指定された table_name のすべてのインデックスを表示します:

    ```sql
    SHOW INDEX FROM example_db.table_name;
    ```
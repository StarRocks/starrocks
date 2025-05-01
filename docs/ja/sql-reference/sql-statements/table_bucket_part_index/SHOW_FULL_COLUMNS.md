---
displayed_sidebar: docs
---

# SHOW FULL COLUMNS

## 説明

このステートメントは、指定されたテーブルのカラムの内容を表示するために使用されます。

:::tip

この操作には権限は必要ありません。

:::

## 構文

```sql
SHOW FULL COLUMNS FROM <tbl_name>
```

## 例

1. 指定されたテーブルからカラムの内容を表示します。

    ```sql
    SHOW FULL COLUMNS FROM tbl;
    ```
---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "SHOW FULL COLUMNS displays column content information for a specified table."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# SHOW FULL COLUMNS

## 説明

このステートメントは、指定されたテーブルの列の内容を表示するために使用されます。

:::tip

この操作には特権は必要ありません。

:::

## 構文

```sql
SHOW FULL COLUMNS FROM <tbl_name>
```

## 例

1. 指定されたテーブルの列の内容を表示します。

    ```sql
    SHOW FULL COLUMNS FROM tbl;
    ```
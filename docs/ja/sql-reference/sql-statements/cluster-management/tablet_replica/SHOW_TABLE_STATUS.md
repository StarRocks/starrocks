---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "SHOW TABLE STATUS は、テーブルのいくつかの情報を表示するために使用されます。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# SHOW TABLE STATUS

## 説明

このステートメントは、テーブルのいくつかの情報を表示するために使用されます。

:::tip

この操作には特権は必要ありません。

:::

## 構文

```sql
SHOW TABLE STATUS
[FROM db] [LIKE "pattern"]
```

> Note
>
> このステートメントは主に MySQL 構文と互換性があります。現在のところ、Comment などのいくつかの情報のみを表示します。

## 例

1. 現在のデータベース内のすべてのテーブル情報を表示します。

    ```SQL
    SHOW TABLE STATUS;
    ```

2. 名前に example を含む、指定されたデータベース内のすべてのテーブル情報を表示します。

    ```SQL
    SHOW TABLE STATUS FROM db LIKE "%example%";
    ```
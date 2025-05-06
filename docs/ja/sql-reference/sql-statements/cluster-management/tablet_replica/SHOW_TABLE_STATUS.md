---
displayed_sidebar: docs
---

# SHOW TABLE STATUS

## Description

このステートメントは、テーブル内のいくつかの情報を表示するために使用されます。

:::tip

この操作には特権は必要ありません。

:::

## Syntax

```sql
SHOW TABLE STATUS
[FROM db] [LIKE "pattern"]
```

> Note
>
> このステートメントは主に MySQL の構文と互換性があります。現在のところ、Comment などのいくつかの情報のみを表示します。

## Examples

1. 現在のデータベース内のすべてのテーブル情報を表示します。

    ```SQL
    SHOW TABLE STATUS;
    ```

2. 名前に example を含む、指定されたデータベース内のすべてのテーブル情報を表示します。

    ```SQL
    SHOW TABLE STATUS FROM db LIKE "%example%";
    ```
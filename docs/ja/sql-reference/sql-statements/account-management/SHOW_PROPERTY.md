---
displayed_sidebar: docs
---

# SHOW PROPERTY

## 説明

このステートメントは、ユーザーのプロパティを表示するために使用されます。

構文:

```sql
SHOW PROPERTY [FOR user] [LIKE key]
```

## 例

1. jack ユーザーのプロパティを表示する

    ```sql
    SHOW PROPERTY FOR 'jack'
    ```

2. Jack ユーザーによってインポートされたクラスターに関連するプロパティを表示する

    ```sql
    SHOW PROPERTY FOR 'jack' LIKE '%load_cluster%'
    ```
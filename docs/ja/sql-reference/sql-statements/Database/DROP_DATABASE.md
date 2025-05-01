---
displayed_sidebar: docs
---

# DROP DATABASE

## 説明

StarRocksでデータベースを削除します。

> **注意**
>
> この操作には、対象データベースに対するDROP権限が必要です。

## 構文

```sql
DROP DATABASE [IF EXISTS] <db_name> [FORCE]
```

注意:

1. DROP DATABASEを実行した後、しばらくしてからRECOVER文を使用して削除されたデータベースを復元できます。詳細はRECOVER文を参照してください。
2. DROP DATABASE FORCEが実行されると、データベースは直接削除され、データベース内に未完了のアクティビティがあるかどうかを確認せずに復元することはできません。一般的にこの操作は推奨されません。

## 例

1. データベース db_test を削除します。

    ```sql
    DROP DATABASE db_test;
    ```

## 参考

- [CREATE DATABASE](CREATE_DATABASE.md)
- [SHOW CREATE DATABASE](SHOW_CREATE_DATABASE.md)
- [USE](USE.md)
- [DESC](../table_bucket_part_index/DESCRIBE.md)
- [SHOW DATABASES](SHOW_DATABASES.md)
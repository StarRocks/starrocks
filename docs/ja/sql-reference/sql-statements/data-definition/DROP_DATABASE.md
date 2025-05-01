---
displayed_sidebar: docs
---

# DROP DATABASE

## 説明

StarRocks でデータベースを削除します。

## 構文

```sql
DROP DATABASE [IF EXISTS] <db_name> [FORCE]
```

注意:

1. DROP DATABASE を実行した後、しばらくしてから RECOVER ステートメントを使用して削除されたデータベースを復元できます。詳細は RECOVER ステートメントを参照してください。
2. DROP DATABASE FORCE が実行されると、データベースは直接削除され、データベース内に未完了のアクティビティがあるかどうかを確認せずに復元できなくなります。一般的にこの操作は推奨されません。

## 例

1. データベース db_test を削除します。

    ```sql
    DROP DATABASE db_test;
    ```

## 参照

- [CREATE DATABASE](../data-definition/CREATE_DATABASE.md)
- [SHOW CREATE DATABASE](../data-manipulation/SHOW_CREATE_DATABASE.md)
- [USE](../data-definition/USE.md)
- [DESC](../Utility/DESCRIBE.md)
- [SHOW DATABASES](../data-manipulation/SHOW_DATABASES.md)
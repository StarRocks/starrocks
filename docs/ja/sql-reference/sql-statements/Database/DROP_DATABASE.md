---
displayed_sidebar: docs
---

# DROP DATABASE

## 説明

StarRocks でデータベースを削除します。

> **注意**
>
> この操作を行うには、対象データベースに対する DROP 権限が必要です。

## 構文

```sql
DROP DATABASE [IF EXISTS] <db_name> [FORCE]
```

次の点に注意してください:

- DROP DATABASE を実行してデータベースを削除した後、指定された保持期間内（デフォルトの保持期間は 1 日）に [RECOVER](../backup_restore/RECOVER.md) ステートメントを使用して削除されたデータベースを復元できますが、データベースと共に削除されたパイプ（v3.2 以降でサポート）は復元できません。
- `DROP DATABASE FORCE` を実行してデータベースを削除すると、未完了のアクティビティがあるかどうかのチェックなしにデータベースが直接削除され、復元できません。一般的に、この操作は推奨されません。
- データベースを削除すると、そのデータベースに属するすべてのパイプ（v3.2 以降でサポート）もデータベースと共に削除されます。

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
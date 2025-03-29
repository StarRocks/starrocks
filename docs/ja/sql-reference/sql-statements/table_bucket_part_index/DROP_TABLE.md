---
displayed_sidebar: docs
---

# DROP TABLE

## 説明

このステートメントは、テーブルを削除するために使用されます。

## 構文

```sql
DROP [TEMPORARY] TABLE [IF EXISTS] [db_name.]table_name [FORCE]
```

注意:

- テーブルが削除された後、指定された期間内（デフォルトでは1日）に[RECOVER](../backup_restore/RECOVER.md)ステートメントを使用してテーブルを復元できます。
- DROP Table FORCE が実行されると、データベース内の未完了のアクティビティを確認せずにテーブルが直接削除され、復元できなくなります。一般的に、この操作は推奨されません。
- 一度削除された一時テーブルは、RECOVER を使用して復元することはできません。

## 例

1. テーブルを削除します。

    ```sql
    DROP TABLE my_table;
    ```

2. 存在する場合、指定されたデータベースのテーブルを削除します。

    ```sql
    DROP TABLE IF EXISTS example_db.my_table;
    ```

3. テーブルを強制的に削除し、ディスク上のデータをクリアします。

    ```sql
    DROP TABLE my_table FORCE;
    ```

## 参照

- [CREATE TABLE](CREATE_TABLE.md)
- [SHOW TABLES](SHOW_TABLES.md)
- [SHOW CREATE TABLE](SHOW_CREATE_TABLE.md)
- [ALTER TABLE](ALTER_TABLE.md)
- [SHOW ALTER TABLE](SHOW_ALTER.md)
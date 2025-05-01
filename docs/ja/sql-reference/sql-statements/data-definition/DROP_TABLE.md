---
displayed_sidebar: docs
---

# DROP TABLE

## 説明

このステートメントはテーブルを削除するために使用されます。

## 構文

```sql
DROP TABLE [IF EXISTS] [db_name.]table_name [FORCE]
```

注意:

- DROP TABLE ステートメントを使用してテーブルが削除されてから24時間以内であれば、[RECOVER](../data-definition/RECOVER.md) ステートメントを使用してテーブルを復元できます。
- DROP TABLE FORCE が実行されると、データベース内の未完了のアクティビティを確認せずにテーブルが直接削除され、復元できなくなります。一般的にこの操作は推奨されません。

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
- [SHOW TABLES](../data-manipulation/SHOW_TABLES.md)
- [SHOW CREATE TABLE](../data-manipulation/SHOW_CREATE_TABLE.md)
- [ALTER TABLE](ALTER_TABLE.md)
- [SHOW ALTER TABLE](../data-manipulation/SHOW_ALTER.md)
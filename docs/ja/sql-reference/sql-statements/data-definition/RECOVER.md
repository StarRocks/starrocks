---
displayed_sidebar: docs
---

# RECOVER

## 説明

このステートメントは、削除されたデータベース、テーブル、またはパーティションを復元するために使用されます。

構文:

1. データベースを復元

    ```sql
    RECOVER DATABASE <db_name>
    ```

2. テーブルを復元

    ```sql
    RECOVER TABLE [<db_name>.]<table_name>
    ```

3. パーティションを復元

    ```sql
    RECOVER PARTITION partition_name FROM [<db_name>.]<table_name>
    ```

注意:

1. 以前に削除されたメタ情報のみを復元できます。デフォルトの時間は1日です。（fe.conf のパラメーター設定 catalog_trash_expire_second を変更することで変更可能です。）
2. メタ情報が削除され、同一のメタ情報が作成された場合、以前のものは復元されません。

## 例

1. example_db という名前のデータベースを復元

    ```sql
    RECOVER DATABASE example_db;
    ```

2. example_tbl という名前のテーブルを復元

    ```sql
    RECOVER TABLE example_db.example_tbl;
    ```

3. example_tbl テーブルの p1 という名前のパーティションを復元

    ```sql
    RECOVER PARTITION p1 FROM example_tbl;
    ```
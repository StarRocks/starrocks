---
displayed_sidebar: docs
---

# RECOVER

DROP コマンドを使用して削除されたデータベース、テーブル、またはパーティションを復元します。削除されたデータベース、テーブル、またはパーティションは、FE パラメータ `catalog_trash_expire_second`（デフォルトでは 1 日）で指定された期間内に復元できます。

[TRUNCATE TABLE](../table_bucket_part_index/TRUNCATE_TABLE.md) を使用して削除されたデータは復元できません。

## 構文

1. データベースを復元します。

    ```sql
    RECOVER DATABASE <db_name>
    ```

2. テーブルを復元します。

    ```sql
    RECOVER TABLE [<db_name>.]<table_name>
    ```

3. パーティションを復元します。

    ```sql
    RECOVER PARTITION <partition_name> FROM [<db_name>.]<table_name>
    ```

注意:

1. このコマンドは、ある程度前に削除されたメタデータ（デフォルトでは 1 日）しか復元できません。期間を変更するには、FE パラメータ `catalog_trash_expire_second` を調整してください。
2. メタデータが削除され、同一のメタデータが作成された場合、以前のものは復元されません。

## 例

1. データベース `example_db` を復元します。

    ```sql
    RECOVER DATABASE example_db;
    ```

2. テーブル `example_tbl` を復元します。

    ```sql
    RECOVER TABLE example_db.example_tbl;
    ```

3. テーブル `example_tbl` のパーティション `p1` を復元します。

    ```sql
    RECOVER PARTITION p1 FROM example_tbl;
    ```
---
displayed_sidebar: docs
---

# ALTER DATABASE

## 説明

指定されたデータベースのプロパティを設定します。

:::tip

この操作には、対象データベースに対する ALTER 権限が必要です。この権限を付与するには、[GRANT](../account-management/GRANT.md) の指示に従ってください。

:::

## 構文

1. データベースのデータクォータを B/K/KB/M/MB/G/GB/T/TB/P/PB で設定します。

    ```sql
    ALTER DATABASE <db_name> SET DATA QUOTA <quota>;
    ```

2. データベースの名前を変更します。

    ```sql
    ALTER DATABASE <db_name> RENAME <new_db_name>;
    ```

3. データベースのレプリカクォータを設定します。

    ```sql
    ALTER DATABASE <db_name> SET REPLICA QUOTA <quota>;
    ```

注意:

```plain text
- データベースの名前を変更した後、必要に応じて REVOKE と GRANT コマンドを使用して対応するユーザー権限を変更してください。
- データベースのデフォルトのデータクォータとデフォルトのレプリカクォータは 2^63-1 です。
```

## 例

1. データベースのデータクォータを設定します。

    ```SQL
    ALTER DATABASE example_db SET DATA QUOTA 10995116277760B;
    -- 上記の単位はバイトで、以下の文と同等です。
    ALTER DATABASE example_db SET DATA QUOTA 10T;
    ALTER DATABASE example_db SET DATA QUOTA 100G;
    ALTER DATABASE example_db SET DATA QUOTA 200M;
    ```

2. データベース `example_db` の名前を `example_db2` に変更します。

    ```SQL
    ALTER DATABASE example_db RENAME example_db2;
    ```

3. データベースのレプリカクォータを設定します。

    ```SQL
    ALTER DATABASE example_db SET REPLICA QUOTA 102400;
    ```

## 参考

- [CREATE DATABASE](CREATE_DATABASE.md)
- [USE](USE.md)
- [SHOW DATABASES](SHOW_DATABASES.md)
- [DESC](../table_bucket_part_index/DESCRIBE.md)
- [DROP DATABASE](DROP_DATABASE.md)
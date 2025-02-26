---
displayed_sidebar: docs
---

# CREATE DATABASE

## 説明

このステートメントはデータベースを作成するために使用されます。

:::tip

この操作には、対象の catalog に対する CREATE DATABASE 権限が必要です。この権限を付与するには、[GRANT](../account-management/GRANT.md) の指示に従ってください。

:::

## 構文

```sql
CREATE DATABASE [IF NOT EXISTS] <db_name>
[PROPERTIES ("key"="value", ...)]
```

## パラメータ

`db_name`: 作成するデータベースの名前。命名規則については、[System limits](../../System_limit.md) を参照してください。

**PROPERTIES (オプション)**

`storage_volume`: 共有データクラスタ内でデータベースを保存するために使用されるストレージボリュームの名前を指定します。

## 例

1. データベース `db_test` を作成します。

   ```sql
   CREATE DATABASE db_test;
   ```

2. ストレージボリューム `s3_storage_volume` を使用してクラウドネイティブデータベース `cloud_db` を作成します。

   ```sql
   CREATE DATABASE cloud_db
   PROPERTIES ("storage_volume"="s3_storage_volume");
   ```

## 参照

- [SHOW CREATE DATABASE](SHOW_CREATE_DATABASE.md)
- [USE](USE.md)
- [SHOW DATABASES](SHOW_DATABASES.md)
- [DESC](../table_bucket_part_index/DESCRIBE.md)
- [DROP DATABASE](DROP_DATABASE.md)
---
displayed_sidebar: docs
---

# ADMIN SHOW REPLICA STATUS

## 説明

このステートメントは、テーブルまたはパーティションのレプリカのステータスを表示するために使用されます。

:::tip

この操作には、SYSTEM レベルの OPERATE 権限が必要です。この権限を付与するには、[GRANT](../../account-management/GRANT.md) の指示に従ってください。

:::

## 構文

```sql
ADMIN SHOW REPLICA STATUS FROM [db_name.]tbl_name [PARTITION (p1, ...)]
[where_clause]
```

```sql
where_clause:
WHERE STATUS [!]= "replica_status"
```

```plain text
replica_status:
OK:            レプリカは正常です
DEAD:          レプリカのバックエンドが利用できません
VERSION_ERROR: レプリカのデータバージョンが欠落しています
SCHEMA_ERROR:  レプリカのスキーマハッシュが正しくありません
MISSING:       レプリカが存在しません
```

## 例

1. テーブルのすべてのレプリカのステータスを表示します。

    ```sql
    ADMIN SHOW REPLICA STATUS FROM db1.tbl1;
    ```

2. VERSION_ERROR のステータスを持つパーティションのレプリカを表示します。

    ```sql
    ADMIN SHOW REPLICA STATUS FROM tbl1 PARTITION (p1, p2)
    WHERE STATUS = "VERSION_ERROR";
    ```

3. テーブルのすべての異常なレプリカを表示します。

    ```sql
    ADMIN SHOW REPLICA STATUS FROM tbl1
    WHERE STATUS != "OK";
    ```
---
displayed_sidebar: docs
---

# ADMIN CANCEL REPAIR

## 説明

指定されたテーブルまたはパーティションに対する修復操作の優先スケジュールをキャンセルします。このステートメントは、指定されたテーブルまたはパーティションのシャーディングレプリカを高優先度で修復しないことを示すだけです。これらのコピーは、デフォルトのスケジューリングによって修復され続けます。

ADMIN CANCEL REPAIR は、共有なしクラスタ内のネイティブテーブルにのみ適用されます。

詳細な手順については、[レプリカの管理](../../../../administration/management/resource_management/Replica.md) を参照してください。

:::tip

この操作には、SYSTEM レベルの OPERATE 権限が必要です。この権限を付与するには、[GRANT](../../account-management/GRANT.md) の指示に従ってください。

:::

## 構文

```sql
ADMIN CANCEL REPAIR TABLE table_name[ PARTITION (p1,...)]
```

## 例

1. ネイティブテーブル `tbl1` のパーティション `p1` に対する優先度の高い修復スケジュールをキャンセルします。

    ```sql
    ADMIN CANCEL REPAIR TABLE tbl PARTITION(p1);
    ```

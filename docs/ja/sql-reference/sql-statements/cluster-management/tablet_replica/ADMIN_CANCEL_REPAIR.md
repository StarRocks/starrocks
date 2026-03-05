---
displayed_sidebar: docs
---

# ADMIN CANCEL REPAIR

## 説明

<<<<<<< HEAD
このステートメントは、指定されたテーブルまたはパーティションの修復を高優先度でキャンセルするために使用されます。
=======
指定されたテーブルまたはパーティションに対する修復操作の優先スケジュールをキャンセルします。このステートメントは、指定されたテーブルまたはパーティションのシャーディングレプリカを高優先度で修復しないことを示すだけです。これらのコピーは、デフォルトのスケジューリングによって修復され続けます。

ADMIN CANCEL REPAIR は、共有なしクラスタ内のネイティブテーブルにのみ適用されます。
>>>>>>> 9d6586f1d4 ([Doc] Remove Problematic Links (#69829))

:::tip

この操作には、SYSTEM レベルの OPERATE 権限が必要です。この権限を付与するには、[GRANT](../../account-management/GRANT.md) の指示に従ってください。

:::

## 構文

```sql
ADMIN CANCEL REPAIR TABLE table_name[ PARTITION (p1,...)]
```

Note
>
> このステートメントは、指定されたテーブルまたはパーティションのシャーディングレプリカを高優先度で修復しないことを示すだけです。これらのコピーは、デフォルトのスケジューリングによって修復され続けます。

## 例

1. 高優先度の修復をキャンセル

    ```sql
    ADMIN CANCEL REPAIR TABLE tbl PARTITION(p1);
    ```
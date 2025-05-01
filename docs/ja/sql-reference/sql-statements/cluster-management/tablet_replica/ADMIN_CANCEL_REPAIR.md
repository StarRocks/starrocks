---
displayed_sidebar: docs
---

# ADMIN CANCEL REPAIR

## 説明

このステートメントは、指定されたテーブルやパーティションの修復を高優先度でキャンセルするために使用されます。

:::tip

この操作には、SYSTEM レベルの OPERATE 権限が必要です。この権限を付与するには、 [GRANT](../../account-management/GRANT.md) の指示に従ってください。

:::

## 構文

```sql
ADMIN CANCEL REPAIR TABLE table_name[ PARTITION (p1,...)]
```

Note
>
> このステートメントは、指定されたテーブルやパーティションのシャーディングレプリカを高優先度で修復しないことを示すだけです。デフォルトのスケジューリングによって、これらのコピーは引き続き修復されます。

## 例

1. 高優先度の修復をキャンセル

    ```sql
    ADMIN CANCEL REPAIR TABLE tbl PARTITION(p1);
    ```
---
displayed_sidebar: docs
---

# ADMIN CANCEL REPAIR

## 説明

このステートメントは、指定されたテーブルまたはパーティションの修復を高優先度でキャンセルするために使用されます。

## 構文

```sql
ADMIN CANCEL REPAIR TABLE table_name[ PARTITION (p1,...)]
```

注意
>
> このステートメントは、システムが指定されたテーブルまたはパーティションのシャーディングレプリカを高優先度で修復しないことを示すだけです。これらのコピーは、デフォルトのスケジューリングによって引き続き修復されます。

## 例

1. 高優先度修復のキャンセル

    ```sql
    ADMIN CANCEL REPAIR TABLE tbl PARTITION(p1);
    ```
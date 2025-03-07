---
displayed_sidebar: docs
---

# ADMIN SHOW REPLICA DISTRIBUTION

## 説明

このステートメントは、テーブルまたはパーティションのレプリカの分布状況を表示するために使用されます。

:::tip

この操作には、SYSTEM レベルの OPERATE 権限が必要です。この権限を付与するには、[GRANT](../../account-management/GRANT.md) の指示に従ってください。

:::

## 構文

```sql
ADMIN SHOW REPLICA DISTRIBUTION FROM [db_name.]tbl_name [PARTITION (p1, ...)]
```

注意:

結果の Graph 列には、レプリカの分布比率がグラフィカルに表示されます。

## 例

1. テーブルのレプリカ分布を表示する

    ```sql
    ADMIN SHOW REPLICA DISTRIBUTION FROM tbl1;
    ```

2. テーブル内のパーティションのレプリカ分布を表示する

    ```sql
    ADMIN SHOW REPLICA DISTRIBUTION FROM db1.tbl1 PARTITION(p1, p2);
    ```
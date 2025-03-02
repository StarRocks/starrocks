---
displayed_sidebar: docs
---

# ADMIN CHECK TABLET

## 説明

このステートメントは、一連の tablet をチェックするために使用されます。

:::tip

この操作には、SYSTEM レベルの OPERATE 権限が必要です。[GRANT](../../account-management/GRANT.md) の指示に従って、この権限を付与できます。

:::

## 構文

```sql
ADMIN CHECK TABLE (tablet_id1, tablet_id2, ...)
PROPERTIES("type" = "...")
```

注意:

1. tablet id と PROPERTIES の "type" プロパティは指定する必要があります。

2. 現在、"type" は以下のみをサポートしています:

   Consistency: tablet のレプリカの一貫性をチェックします。このコマンドは非同期です。送信後、StarRocks は対応する tablet 間の一貫性をチェックし始めます。最終結果は、SHOW PROC "/statistic" の結果の InconsistentTabletNum 列に表示されます。

## 例

1. 指定された一連の tablet 上のレプリカの一貫性をチェックする

    ```sql
    ADMIN CHECK TABLET (10000, 10001)
    PROPERTIES("type" = "consistency");
    ```
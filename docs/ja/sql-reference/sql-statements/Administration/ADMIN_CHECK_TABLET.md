---
displayed_sidebar: docs
---

# ADMIN CHECK TABLET

## 説明

このステートメントは、一連の tablet をチェックするために使用されます。

構文:

```sql
ADMIN CHECK TABLE (tablet_id1, tablet_id2, ...)
PROPERTIES("type" = "...")
```

注意:

1. tablet id と PROPERTIES の "type" プロパティは指定する必要があります。

2. 現在、"type" は以下のみをサポートしています:

   Consistency: tablet のレプリカの整合性をチェックします。このコマンドは非同期です。送信後、StarRocks は対応する tablet 間の整合性をチェックし始めます。最終結果は、SHOW PROC "/statistic" の結果にある InconsistentTabletNum 列に表示されます。

## 例

1. 指定された一連の tablet のレプリカの整合性をチェックする

    ```sql
    ADMIN CHECK TABLET (10000, 10001)
    PROPERTIES("type" = "consistency");
    ```
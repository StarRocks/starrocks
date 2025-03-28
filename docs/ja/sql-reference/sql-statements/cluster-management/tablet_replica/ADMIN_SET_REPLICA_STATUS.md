---
displayed_sidebar: docs
---

# ADMIN SET REPLICA STATUS

## 説明

このステートメントは、指定されたレプリカのステータスを設定するために使用されます。

このコマンドは現在、一部のレプリカのステータスを手動で BAD または OK に設定し、システムがこれらのレプリカを自動的に修復できるようにするためにのみ使用されます。

:::tip

この操作には、SYSTEM レベルの OPERATE 権限が必要です。[GRANT](../../account-management/GRANT.md) の指示に従って、この権限を付与できます。

:::

## 構文

```sql
ADMIN SET REPLICA STATUS
PROPERTIES ("key" = "value", ...)
```

現在サポートされている属性は以下の通りです:

"table_id": 必須。Tablet Id を指定します。

"backend_id": 必須。Backend Id を指定します。

"status": 必須。ステータスを指定します。現在サポートされているのは "bad" と "ok" のみです。

指定されたレプリカが存在しないか、そのステータスが不良の場合、そのレプリカは無視されます。

注意:

Bad ステータスに設定されたレプリカはすぐに削除される可能性があるため、注意して進めてください。

## 例

1. tablet 10003 の BE 10001 上のレプリカステータスを bad に設定します。

    ```sql
    ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "10003", "backend_id" = "10001", "status" = "bad");
    ```

2. tablet 10003 の BE 10001 上のレプリカステータスを ok に設定します。

    ```sql
    ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "10003", "backend_id" = "10001", "status" = "ok");
    ```
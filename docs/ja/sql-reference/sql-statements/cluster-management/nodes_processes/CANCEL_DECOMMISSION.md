---
displayed_sidebar: docs
---

# CANCEL DECOMMISSION

## 説明

このステートメントは、ノードの停止解除に使用されます。

:::tip

この操作を実行する権限は `cluster_admin` ロールのみが持っています。この権限を付与するには、 [GRANT](../../account-management/GRANT.md) の指示に従ってください。

:::

構文:

```sql
CANCEL DECOMMISSION BACKEND "<host>:<heartbeat_port>"[,"<host>:<heartbeat_port>"...]
```

## 例

1. 2つのノードの停止解除をキャンセルします。

    ```sql
    CANCEL DECOMMISSION BACKEND "host1:port", "host2:port";
    ```
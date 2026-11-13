---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "ノードのデコミッションを取り消します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# CANCEL DECOMMISSION

## 説明

このステートメントは、ノードのデコミッションを取り消すために使用されます。

:::tip

この操作を実行する権限は `cluster_admin` ロールのみが持っています。この権限を付与するには、[GRANT](../../account-management/GRANT.md) の指示に従ってください。

:::

構文:

```sql
CANCEL DECOMMISSION BACKEND "<host>:<heartbeat_port>"[,"<host>:<heartbeat_port>"...]
```

## 例

1. 2つのノードのデコミッションを取り消します。

    ```sql
    CANCEL DECOMMISSION BACKEND "host1:port", "host2:port";
    ```
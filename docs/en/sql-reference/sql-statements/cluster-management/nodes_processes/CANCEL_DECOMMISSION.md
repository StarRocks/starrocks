---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "CANCEL DECOMMISSION is used to undo a node decommission."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# CANCEL DECOMMISSION

CANCEL DECOMMISSION is used to undo a node decommission.

:::tip

Only the `cluster_admin` role has the privilege to perform this operation. You can follow the instructions in [GRANT](../../account-management/GRANT.md) to grant this privilege.

:::

Syntax:

```sql
CANCEL DECOMMISSION BACKEND "<host>:<heartbeat_port>"[,"<host>:<heartbeat_port>"...]
```

## Examples

1. Cancel decommission of two nodes.

    ```sql
    CANCEL DECOMMISSION BACKEND "host1:port", "host2:port";
    ```

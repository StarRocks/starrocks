---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "SHOW BROKER views the information of all brokers that have been added to the StarRocks cluster."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# SHOW BROKER

SHOW BROKER is used to view a broker that currently exists.

:::tip

Only users with the SYSTEM-level OPERATE privilege or the `cluster_admin` role can perform this operation.

:::

## Syntax

```sql
SHOW BROKER
```

Note:

1. LastStartTime represents the latest BE start-up time.
2. LastHeartbeat represents the latest heartbeat.
3. Alive indicates whether the node survives.
4. ErrMsg is used to display error messages when the heartbeat fails.

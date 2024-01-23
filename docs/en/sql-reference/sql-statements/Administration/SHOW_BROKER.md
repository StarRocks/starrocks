---
displayed_sidebar: "English"
---

# SHOW BROKER

## Description

This statement is used to view broker that currently exists.

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

---
displayed_sidebar: docs
---

# SHOW FRONTENDS

## Description

This statement is used to view FE nodes.

:::tip

Only users with the SYSTEM-level OPERATE privilege or the `cluster_admin` role can perform this operation.

:::

## Syntax

```sql
SHOW FRONTENDS
```

Note:

1. Name represents the name of FE node in BDBJE.
2. Join being true means this node has already joined the cluster. But it does not mean that the node is still in the cluster, as it may be missing.
3. Alive indicates whether the node survives.
4. ReplayedJournalId represents the maximum metadata log ID that the node has currently replayed.
5. LastHeartbeat is the latest heartbeat.
6. IsHelper indicates whether the node is a helper node from BDBJE.
7. ErrMsg is used to display error messages when the heartbeat fails.

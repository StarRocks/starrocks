---
displayed_sidebar: docs
---

# ADD BACKEND BLACKLIST

## Description

Adds a BE node to the BE Blacklist. You can manually add BE nodes to the blacklist to forbid the usage of the nodes in query execution, thereby avoiding frequent query failures or other unexpected behaviors caused by failed connections to the BE nodes.

This feature is supported from v3.3.0 onwards. For more information, see [Manage BE Blacklist](../../../../administration/management/BE_blacklist.md).

:::note

Only users with the SYSTEM-level BLACKLIST privilege can perform this operation.

:::

By default, StarRocks can automatically manage the BE Blacklist, adding the BE nodes that have lost connection to the blacklist and removing them from the blacklist when the connection is reestablished. However, StarRocks will not remove the BE node from the blacklist if the node is manually blacklisted.

## Syntax

```SQL
ADD BACKEND BLACKLIST <be_id>[, ...]
```

## Parameters

`be_id`: ID of the BE node to be blacklisted. You can obtain the BE ID by executing [SHOW BACKENDS](SHOW_BACKENDS.md).

## Examples

```SQL
-- Obtain BE ID.
SHOW BACKENDS\G
*************************** 1. row ***************************
            BackendId: 10001
                   IP: xxx.xx.xx.xxx
                   ...
-- Add BE to the blacklist.
ADD BACKEND BLACKLIST 10001;
```

## Relevant SQLs

- [DELETE BACKEND BLACKLIST](./DELETE_BACKEND_BLACKLIST.md)
- [SHOW BACKEND BLACKLIST](./SHOW_BACKEND_BLACKLIST.md)
- [SHOW BACKENDS](SHOW_BACKENDS.md)


---
displayed_sidebar: docs
---

# SYNC

Synchronizes the data consistency among different sessions.

Currently, StarRocks can only guarantee consistency within the same session, that is, only the session that initiated the data write operation can read the latest data immediately after the operation succeeds. If you want to read the latest data immediately from other sessions, you must synchronize the data consistency using the SYNC statement. Generally, there will be a millisecond-level latency among sessions if you do not execute this statement.

:::tip

This operation does not require privileges.

:::

## Syntax

```SQL
SYNC
```

## Examples

```Plain
mysql> SYNC;
Query OK, 0 rows affected (0.00 sec)
```

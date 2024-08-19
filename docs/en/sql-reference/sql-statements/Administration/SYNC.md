---
displayed_sidebar: "English"
---

# SYNC

## Description

Currently StarRocks is not a strongly consistent system and can only guarantee session consistency, that is, the data write operation initiated in the same MySQL session can read the latest data at the next moment. If other sessions need to read the latest data, send a sync command before querying the data.

:::tip

This operation does not require privileges.

:::

## Syntax

```SQL
sync
```

## Examples

```Plain
mysql> sync;
Query OK, 0 rows affected (0.00 sec)
```

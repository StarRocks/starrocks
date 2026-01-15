---
displayed_sidebar: docs
---

# SHOW BACKEND/COMPUTE NODE BLACKLIST

Shows the BE/CN nodes in the BE and CN Blacklist.

BE Blacklist is supported from v3.3.0 onwards and CN Blacklist is supported from v4.0 onwards. For more information, see [Manage BE and CN Blacklist](../../../../administration/management/BE_blacklist.md).

:::note

Only users with the SYSTEM-level BLACKLIST privilege can perform this operation.

:::

## Syntax

```SQL
SHOW { BACKEND | COMPUTE NODE } BLACKLIST
```

## Return value

| **Return**                   | **Description**                                              |
| ---------------------------- | ------------------------------------------------------------ |
| AddBlackListType             | How the BE/CN node was added to the blacklist. `MANUAL` indicates it is manually blacklisted by the user. `AUTO` indicates it is automatically blacklisted by StarRocks. |
| LostConnectionTime           | For the `MANUAL` type, it indicates the time when the BE/CN node was manually added to the blacklist.<br />For the `AUTO` type, it indicates the time when the last successful connection was established. |
| LostConnectionNumberInPeriod | The number of disconnections detected within `CheckTimePeriod(s)`. |
| CheckTimePeriod(s)           | The interval at which StarRocks checks the connection status of the blacklisted BE/CN nodes. Its value is evaluated to the value you specified for the FE configuration item `black_host_history_sec`. Unit: Seconds. |

## Examples

```SQL
-- View the BE Blacklist.
SHOW BACKEND BLACKLIST;
+-----------+------------------+---------------------+------------------------------+--------------------+
| BackendId | AddBlackListType | LostConnectionTime  | LostConnectionNumberInPeriod | CheckTimePeriod(s) |
+-----------+------------------+---------------------+------------------------------+--------------------+
| 10001     | MANUAL           | 2024-04-28 11:52:09 | 0                            | 5                  |
+-----------+------------------+---------------------+------------------------------+--------------------+

-- View the CN Blacklist.
SHOW COMPUTE NODE BLACKLIST;
+---------------+------------------+---------------------+------------------------------+--------------------+
| ComputeNodeId | AddBlackListType | LostConnectionTime  | LostConnectionNumberInPeriod | CheckTimePeriod(s) |
+---------------+------------------+---------------------+------------------------------+--------------------+
| 10005         | MANUAL           | 2025-08-18 10:47:51 | 0                            | 5                  |
+---------------+------------------+---------------------+------------------------------+--------------------+
```

## Relevant SQLs

- [ADD BACKEND/COMPUTE NODE BLACKLIST](./ADD_BACKEND_BLACKLIST.md)
- [DELETE BACKEND/COMPUTE NODE BLACKLIST](./DELETE_BACKEND_BLACKLIST.md)
- [SHOW BACKENDS](SHOW_BACKENDS.md)
- [SHOW COMPUTE NODES](./SHOW_COMPUTE_NODES.md)

---
displayed_sidebar: docs
---

# SHOW BACKEND BLACKLIST

## Description

Shows the BE nodes in the BE Blacklist.

This feature is supported from v3.3.0 onwards. For more information, see [Manage BE Blacklist](../../../../administration/management/BE_blacklist.md).

:::note

Only users with the SYSTEM-level BLACKLIST privilege can perform this operation.

:::

## Syntax

```SQL
SHOW BACKEND BLACKLIST
```

## Return value

| **Return**                   | **Description**                                              |
| ---------------------------- | ------------------------------------------------------------ |
| AddBlackListType             | How the BE node was added to the blacklist. `MANUAL` indicates it is manually blacklisted by the user. `AUTO` indicates it is automatically blacklisted by StarRocks. |
| LostConnectionTime           | For the `MANUAL` type, it indicates the time when the BE node was manually added to the blacklist.<br />For the `AUTO` type, it indicates the time when the last successful connection was established. |
| LostConnectionNumberInPeriod | The number of disconnections detected within `CheckTimePeriod(s)`. |
| CheckTimePeriod(s)           | The interval at which StarRocks checks the connection status of the blacklisted BE nodes. Its value is evaluated to the value you specified for the FE configuration item `black_host_history_sec`. Unit: Seconds. |

## Examples

```SQL
SHOW BACKEND BLACKLIST;
+-----------+------------------+---------------------+------------------------------+--------------------+
| BackendId | AddBlackListType | LostConnectionTime  | LostConnectionNumberInPeriod | CheckTimePeriod(s) |
+-----------+------------------+---------------------+------------------------------+--------------------+
| 10001     | MANUAL           | 2024-04-28 11:52:09 | 0                            | 5                  |
+-----------+------------------+---------------------+------------------------------+--------------------+
```

## Relevant SQLs

- [ADD BACKEND BLACKLIST](./ADD_BACKEND_BLACKLIST.md)
- [DELETE BACKEND BLACKLIST](./DELETE_BACKEND_BLACKLIST.md)
- [SHOW BACKENDS](SHOW_BACKENDS.md)


---
displayed_sidebar: "English"
---

# SHOW USAGE RESOURCE GROUPS

## Description

Shows the usage information of resource groups. This feature is supported from v3.1.4 onwards.

:::tip

This operation does not require privileges.

:::

## Syntax

```SQL
SHOW USAGE RESOURCE GROUPS
```

## Return

- `Name`: The name of the resource group.
- `Id`: The ID of the resource group.
- `Backend`: The BE's IP or FQDN.
- `BEInUseCpuCores`: The number of CPU cores currently in use by this resource group on this BE. This value is an approximate estimate.
- `BEInUseMemBytes`: The number of memory bytes currently in use by this resource group on this BE.
- `BERunningQueries`: The number of queries from this resource group that are still running on this BE.

## Usage notes

- BEs periodically report this resource usage information to the Leader FE at the interval specified in `report_resource_usage_interval_ms`, which is by default set to 1 second.
- The results will only show rows where at least one of `BEInUseCpuCores`/`BEInUseMemBytes`/`BERunningQueries` is a positive number. In other words, the information is displayed only when a resource group is actively using some resources on a BE.

## Example

```Plain
MySQL [(none)]> SHOW USAGE RESOURCE GROUPS;
+------------+----+-----------+-----------------+-----------------+------------------+
| Name       | Id | Backend   | BEInUseCpuCores | BEInUseMemBytes | BERunningQueries |
+------------+----+-----------+-----------------+-----------------+------------------+
| default_wg | 0  | 127.0.0.1 | 0.100           | 1               | 5                |
+------------+----+-----------+-----------------+-----------------+------------------+
| default_wg | 0  | 127.0.0.2 | 0.200           | 2               | 6                |
+------------+----+-----------+-----------------+-----------------+------------------+
| wg1        | 0  | 127.0.0.1 | 0.300           | 3               | 7                |
+------------+----+-----------+-----------------+-----------------+------------------+
| wg2        | 0  | 127.0.0.1 | 0.400           | 4               | 8                |
+------------+----+-----------+-----------------+-----------------+------------------+
```

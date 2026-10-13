---
displayed_sidebar: docs
description: "SHOW RESOURCE GROUP shows the information of resource groups."
---

# SHOW RESOURCE GROUP

SHOW RESOURCE GROUP shows the information of resource groups.

:::tip

This operation does not require privileges.

:::

## Syntax

```SQL
SHOW { RESOURCE GROUPS [ALL] | RESOURCE GROUP <resource_group_name> }
```

## Parameters

| **Parameter**       | **Description**                                              |
| ------------------- | ------------------------------------------------------------ |
| RESOURCE GROUPS     | Show the resource groups that match the current user.        |
| ALL                 | Specify this parameter to show all resource groups in the cluster. |
| RESOURCE GROUP      | Show the specified resource group.                           |
| resource_group_name | Name of the resource group to be shown.                      |

## Return

| **Return**                 | **Description**                                                                                                                    |
|----------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| name                       | Name of the resource group.                                                                                                        |
| id                         | ID of the resource group.                                                                                                          |
| cpu_weight                 | CPU scheduling weight of this resource group on a BE node.                                                                         |
| exclusive_cpu_cores        | CPU hard isolation parameter for this resource group.                                                                              |
| mem_limit                  | Memory limit of the resource group.                                                                                                |
| big_query_cpu_second_limit | Big query upper time limit of the resource group.                                                                                  |
| big_query_scan_rows_limit  | Big query scan row limit of the resource group.                                                                                    |
| big_query_mem_limit        | Big query memory limit of the resource group.                                                                                      |
| concurrency_limit          | Concurrency limit of the resource group.                                                                                           |
| spill_mem_limit_threshold  | Memory usage threshold that triggers spilling to disk.                                                                             |
| mem_used_pct_limit         | Memory usage percentage limit of the resource group. Requires `enable_group_level_query_queue` to be enabled.                      |
| classifiers                | Classifiers that are associated with the resource group. `id` is the ID of the classifier, and `weight` is the degree of matching. |

## Examples

Example 1: Shows all resource groups in the cluster.

```Plain
mysql> SHOW RESOURCE GROUPS ALL;
+---------------+------+------------+---------------------+-----------+----------------------------+---------------------------+---------------------+-------------------+---------------------------+---------------------+--------------------+
| name          | id   | cpu_weight | exclusive_cpu_cores | mem_limit | big_query_cpu_second_limit | big_query_scan_rows_limit | big_query_mem_limit | concurrency_limit | spill_mem_limit_threshold | mem_used_pct_limit  | classifiers        |
+---------------+------+------------+---------------------+-----------+----------------------------+---------------------------+---------------------+-------------------+---------------------------+---------------------+--------------------+
| default_mv_wg | 3    | 1          | 0                   | 80.0%     | 0                          | 0                         | 0                   | null              | 80%                       | null                | (id=0, weight=0.0) |
| default_wg    | 2    | 32         | 0                   | 100.0%    | 0                          | 0                         | 0                   | null              | 100%                      | null                | (id=0, weight=0.0) |
+---------------+------+------------+---------------------+-----------+----------------------------+---------------------------+---------------------+-------------------+---------------------------+---------------------+--------------------+
```

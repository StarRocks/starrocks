---
displayed_sidebar: "English"
---

# SHOW RESOURCE GROUP

## Description

Shows the information of resource groups.

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

| **Return**                 | **Description**                                              |
| -------------------------- | ------------------------------------------------------------ |
| name                       | Name of the resource group.                                  |
| id                         | ID of the resource group.                                    |
| cpu_core_limit             | CPU core limit of the resource group.                        |
| mem_limit                  | Memory limit of the resource group.                          |
| big_query_cpu_second_limit | Big query upper time limit of the resource group.            |
| big_query_scan_rows_limit  | Big query scan row limit of the resource group.              |
| big_query_mem_limit        | Big query memory limit of the resource group.                |
| concurrency_limit          | Concurrency limit of the resource group.                     |
| type                       | Type of resource group.                                      |
| classifiers                | Classifiers that are associated with the resource group. `id` is the ID of the classifier, and `weight` is the degree of matching. |

## Examples

Example 1: Shows all resource groups in the cluster.

```Plain
mysql> SHOW RESOURCE GROUPS ALL;
+-------+--------+----------------+-----------+----------------------------+---------------------------+---------------------+-------------------+--------+------------------------------------------------------------------------------------------------------------------+
| name  | id     | cpu_core_limit | mem_limit | big_query_cpu_second_limit | big_query_scan_rows_limit | big_query_mem_limit | concurrency_limit | type   | classifiers                                                                                                      |
+-------+--------+----------------+-----------+----------------------------+---------------------------+---------------------+-------------------+--------+------------------------------------------------------------------------------------------------------------------+
| rg1   | 625126 | 10             | 20.0%     | 100                        | 100000                    | 1073741824          | null              | NORMAL | (id=625127, weight=4.459375, user=rg1_user1, role=rg1_role1, query_type in (SELECT), source_ip=172.26.xxx.xx/24) |
| rg1   | 625126 | 10             | 20.0%     | 100                        | 100000                    | 1073741824          | null              | NORMAL | (id=625128, weight=3.459375, user=rg1_user2, query_type in (SELECT), source_ip=172.26.xxx.xx/24)                 |
| rg1   | 625126 | 10             | 20.0%     | 100                        | 100000                    | 1073741824          | null              | NORMAL | (id=625129, weight=2.359375, user=rg1_user3, source_ip=172.26.xxx.xx/24)                                         |
| rg1   | 625126 | 10             | 20.0%     | 100                        | 100000                    | 1073741824          | null              | NORMAL | (id=625130, weight=1.0, user=rg1_user4)                                                                          |
| rg1   | 625126 | 10             | 20.0%     | 100                        | 100000                    | 1073741824          | null              | NORMAL | (id=625131, weight=10.0, db='db1')                                                                                |
+-------+--------+----------------+-----------+----------------------------+---------------------------+---------------------+-------------------+--------+------------------------------------------------------------------------------------------------------------------+
```

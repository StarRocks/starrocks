---
displayed_sidebar: docs
---

# CREATE RESOURCE GROUP

## Description

Creates a resource group.

For more information, see [Resource group](../../../../administration/management/resource_management/resource_group.md).

:::tip

This operation requires the SYSTEM-level CREATE RESOURCE GROUP privilege. You can follow the instructions in [GRANT](../../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```SQL
CREATE RESOURCE GROUP resource_group_name 
TO CLASSIFIER1, CLASSIFIER2, ...
WITH resource_limit
```

## Parameters

- `resource_group_name`: The name of the resource group to be created.

- `CLASSIFIER`: Classifiers used to filter the queries on which resource limits are imposed. You must specify a classifier using `"key"="value"` pairs. You can set multiple classifiers for a resource group.

  Parameters for classifiers are listed below:

    | **Parameter** | **Required** | **Description**                                              |
    | ------------- | ------------ | ------------------------------------------------------------ |
    | user          | No           | The name of user.                                            |
    | role          | No           | The role of user.                                            |
    | query_type    | No           | The type of query. `SELECT` and `INSERT` (from v2.5) are supported. When INSERT tasks hit a resource group with `query_type` as `insert`, the BE node reserves the specified CPU resources for the tasks.   |
    | source_ip     | No           | The CIDR block from which the query is initiated.            |
    | db            | No           | The database which the query accesses. It can be specified by strings separated by commas (,). |
    | plan_cpu_cost_range | No     | The estimated CPU cost range of the query. Its value has an equivalent meaning with the field `PlanCpuCost` in  **fe.audit.log**, and has no unit. The format is `[DOUBLE, DOUBLE)`. The default value is NULL, indicating no such restriction. This parameter is supported from v3.1.4 onwards.                  |
    | plan_mem_cost_range | No     | The estimated memory cost range of the query. Its value has an equivalent meaning with the field `PlanMemCost` in  **fe.audit.log**, and has no unit. The format is `[DOUBLE, DOUBLE)`. The default value is NULL, indicating no such restriction. This parameter is supported from v3.1.4 onwards.               |

- `resource_limit`: Resource limits to be imposed on the resource group. You must specify resource limits using `"key"="value"` pairs. You can set multiple resource limits for a resource group.

  Parameters for resource limits are listed below:

    | **Parameter**              | **Required** | **Description**                                              |
    | -------------------------- | ------------ | ------------------------------------------------------------ |
    | cpu_core_limit             | No           | The soft limit for the number of CPU cores that can be allocated to the resource group on the BE. In actual business scenarios, CPU cores that are allocated to the resource group proportionally scale based on the availability of CPU cores on the BE. Valid values: any non-zero positive integer. |
    | mem_limit                  | No           | The percentage of memory that can be used for queries in the total memory that is provided by the BE. Unit: %. Valid values: (0, 1). |
    | concurrency_limit          | No           | The upper limit of concurrent queries in a resource group. It is used to avoid system overload caused by too many concurrent queries. |
    | max_cpu_cores              | No           | The CPU core limit for this resource group on a single BE node. It takes effect only when it is set to greater than `0`. Range: [0, `avg_be_cpu_cores`], where `avg_be_cpu_cores` represents the average number of CPU cores across all BE nodes. Default: 0. |
    | big_query_cpu_second_limit | No           | The upper time limit of CPU occupation for a big query. Concurrent queries add up the time. The unit is second. |
    | big_query_scan_rows_limit  | No           | The upper limit of row counts that can be scanned by a big query. |
    | big_query_mem_limit        | No           | The upper limit of memory usage of a big query. The unit is byte. |
    | type                       | No           | The type of resource group. Valid values: <br />`short_query`: When queries from the `short_query` resource group are running, the BE node reserves the CPU cores defined in `short_query.cpu_core_limit`. CPU cores for all `normal` resource groups are limited to "the total CPU cores - `short_query.cpu_core_limit`". <br />`normal`: When no query from the `short_query` resource group is running, the CPU core limit above is not imposed on the `normal` resource groups. <br />Note that you can create only ONE `short_query` resource group in a cluster. |

## Example

Example 1: Creates the resource group `rg1` based on multiple classifiers.

```SQL
CREATE RESOURCE GROUP rg1
TO 
    (user='rg1_user1', role='rg1_role1', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user2', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user3', source_ip='192.168.x.x/24'),
    (user='rg1_user4'),
    (db='db1')
WITH ('cpu_core_limit' = '10',
      'mem_limit' = '20%',
      'type' = 'normal',
      'big_query_cpu_second_limit' = '100',
      'big_query_scan_rows_limit' = '100000',
      'big_query_mem_limit' = '1073741824'
);
```

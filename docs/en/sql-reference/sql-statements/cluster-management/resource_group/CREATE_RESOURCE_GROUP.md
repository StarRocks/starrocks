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
    | cpu_weight                 | No           | Required parameter for creating Shared resource groups. It specifies the CPU scheduling weight of a resource group on a single BE node, determining the relative share of CPU time allocated to tasks from this group. In actual business scenarios, CPU cores that are allocated to the resource group proportionally scale based on the availability of CPU cores on the BE. Value range: (0, `avg_be_cpu_cores`], where `avg_be_cpu_cores` is the average number of CPU cores across all BE nodes. The parameter is effective only when it is set to greater than 0. Only one of `cpu_weight` or `exclusive_cpu_cores` can be set to greater than 0.|
    | exclusive_cpu_cores        | No           | Required parameter for creating Exclusive resource groups (with CPU hard limit). It means to reserve `exclusive_cpu_cores` CPU cores exclusively for this resource group, making them unavailable to other groups, even when idle, and to limit the resource group to only using these reserved CPU cores, preventing it from using available CPU resources from other groups. Value range: (0, `min_be_cpu_cores - 1`], where `min_be_cpu_cores` is the minimum number of CPU cores across all BE nodes. It takes effect only when greater than 0. Only one of `cpu_weight` or `exclusive_cpu_cores` can be set to greater than 0.|
    | mem_limit                  | No           | The percentage of memory that can be used for queries in the total memory that is provided by the BE. Unit: %. Valid values: (0, 1). |
    | concurrency_limit          | No           | The upper limit of concurrent queries in a resource group. It is used to avoid system overload caused by too many concurrent queries. |
    | max_cpu_cores              | No           | The CPU core limit for this resource group on a single BE node. It takes effect only when it is set to greater than `0`. Range: [0, `avg_be_cpu_cores`], where `avg_be_cpu_cores` represents the average number of CPU cores across all BE nodes. Default: 0. |
    | big_query_cpu_second_limit | No           | The upper time limit of CPU occupation for a big query. Concurrent queries add up the time. The unit is second. |
    | big_query_scan_rows_limit  | No           | The upper limit of row counts that can be scanned by a big query. |
    | big_query_mem_limit        | No           | The upper limit of memory usage of a big query. The unit is byte. |

    > **NOTE**
    >
    > Before v3.3.5, StarRocks allowed setting the `type` of a resource group to `short_query`. However, the parameter `type` has been deprecated and replaced by `exclusive_cpu_cores`. For any existing resource groups of this type, the system will automatically convert them to an Exclusive resource group where the `exclusive_cpu_cores` value equals the `cpu_weight` after upgrading to v3.3.5.

## Example

Example 1: Creates a Shared resource group `rg1` based on multiple classifiers.

```SQL
CREATE RESOURCE GROUP rg1
TO 
    (user='rg1_user1', role='rg1_role1', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user2', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user3', source_ip='192.168.x.x/24'),
    (user='rg1_user4'),
    (db='db1')
WITH ('cpu_weight' = '10',
      'mem_limit' = '20%',
      'big_query_cpu_second_limit' = '100',
      'big_query_scan_rows_limit' = '100000',
      'big_query_mem_limit' = '1073741824'
);
```

Example 2: Creates an Exclusive resource group `rg2` based on multiple classifiers.

```SQL
CREATE RESOURCE GROUP rg2
TO 
    (user='rg1_user5', role='rg1_role5', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user6', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user7', source_ip='192.168.x.x/24'),
    (user='rg1_user8'),
    (db='db2')
WITH ('exclusive_cpu_cores' = '10',
      'mem_limit' = '20%',
      'type' = 'normal',
      'big_query_cpu_second_limit' = '100',
      'big_query_scan_rows_limit' = '100000',
      'big_query_mem_limit' = '1073741824'
);
```

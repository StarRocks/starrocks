# CREATE RESOURCE GROUP

## Description

Creates a resource group.

For more information, see [Resource group](../../../administration/resource_group.md).

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
    | plan_cpu_cost_range | No     | The estimated CPU cost range of the query.                   |
    | plan_mem_cost_range | No     | The estimated memory cost range of the query.                |

- `resource_limit`: Resource limits to be imposed on the resource group. You must specify resource limits using `"key"="value"` pairs. You can set multiple resource limits for a resource group.

  Parameters for resource limits are listed below:

    | **Parameter**              | **Required** | **Description**                                              |
    | -------------------------- | ------------ | ------------------------------------------------------------ |
    | cpu_core_limit             | No           | The soft limit for the number of CPU cores that can be allocated to the resource group on the BE. In actual business scenarios, CPU cores that are allocated to the resource group proportionally scale based on the availability of CPU cores on the BE. Valid values: any non-zero positive integer. |
    | mem_limit                  | No           | The percentage of memory that can be used for queries in the total memory that is provided by the BE. Unit: %. Valid values: (0, 1). |
    | concurrency_limit          | No           | The upper limit of concurrent queries in a resource group. It is used to avoid system overload caused by too many concurrent queries. |
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

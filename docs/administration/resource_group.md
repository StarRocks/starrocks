# [Preview] Resource group

This topic describes the resource group feature that StarRocks has started to provide since v2.2.

This feature allows StarRocks to limit resource consumption for queries and implement isolation and efficient use of resources among tenants in the same cluster.

With this feature, you can divide the computing resources of each backend (BE) into multiple resource groups and associate each resource group with one or more classifiers. When you run a query, StarRocks compares the conditions of each classifier with the information about the query to identify the classifier that best matches the query. Then, StarRocks allocates computing resources to the query based on the resource quotas of the resource group associated with the identified classifier.

## Terms

This section describes the terms that you must understand before you use the resource group feature.

### resource group

Each resource group is a set of computing resources from a specific BE. You can divide each BE of your cluster into multiple resource groups. When a query is assigned to a resource group, StarRocks allocates CPU and memory resources to the resource group based on the resource quotas that you specified for the resource group.

You can specify CPU and memory resource quotas for a resource group on a BE by using the following parameters:

- `cpu_core_limit`

  This parameter specifies the soft limit for the number of CPU cores that can be allocated to the resource group on the BE. Valid values: any non-zero positive integer.

  In actual business scenarios, CPU cores that are allocated to the resource group proportionally scale based on the availability of CPU cores on the BE.

  > Note:
  > For example, you configure three resource groups on a BE that provides 16 CPU cores: rg1, rg2, and rg3. The values of `cpu_core_limit` for the three resource groups are `2`, `6`, and `8`, respectively.
  > If all CPU cores of the BE are occupied, the number of CPU cores that can be allocated to each of the three resource groups are 2, 6, and 8, respectively, based on the following calculations:

  - > Number of CPU cores for rg1 = Total number of CPU cores on the BE × (2/16) = 2
  - > Number of CPU cores for rg2 = Total number of CPU cores on the BE × (6/16) = 6
  - > Number of CPU cores for rg3 = Total number of CPU cores on the BE × (8/16) = 8

  > If not all CPU cores of the BE are occupied, as when rg1 and rg2 are loaded but rg3 is not, the number of CPU cores that can be allocated to rg1 and rg2 are 4 and 12, respectively, based on the following calculations:

  - > Number of CPU cores for rg1 = Total number of CPU cores on the BE × (2/8) = 4
  - > Number of CPU cores for rg2 = Total number of CPU cores on the BE × (6/8) = 12

- `mem_limit`

  This parameter specifies the percentage of memory that can be used for queries in the total memory that is provided by the BE. Unit: %. Valid values: (0, 1).

  > Note: The amount of memory that can be used for queries is indicated by the `query_pool` parameter. For more information about the parameter, see [Memory management](Memory_management.md).

### classifier

Each classifier holds one or more conditions that can be matched to the properties of queries. StarRocks identifies the classifier that best matches each query based on the match conditions and assigns resources for running the query.

Classifiers support the following conditions:

- `user`: the name of the user.
- `role`: the role of the user.
- `query_type`: the type of the query. Only `SELECT` queries are supported.
- `source_ip`: the CIDR block from which the query is initiated.

A classifier matches a query only when one or all conditions of the classifier match the information about the query. If multiple classifiers match a query, StarRocks calculates the degree of matching between the query and each classifier and identifies the classifier with the highest degree of matching.

StarRocks calculates the degree of matching between a query and a classifier by using the following rules:

- If the classifier has the same value of `user` as the query, the degree of matching of the classifier increases by 1.
- If the classifier has the same value of `role` as the query, the degree of matching of the classifier increases by 1.
- If the classifier has the same value of `query_type` as the query, the degree of matching of the classifier increases by 1 plus the number obtained from the following calculation: 1/Number of `query_type` fields in the classifier.
- If the classifier has the same value of `source_ip` as the query, the degree of matching of the classifier increases by 1 plus the number obtained from the following calculation: (32 - `cidr_prefix`)/64.

If multiple classifiers match a query, the classifier with a larger number of conditions has a higher degree of matching.

```Plain_Text
-- Classifier B has more conditions than Classifier A. Therefore, Classifier B has a higher degree of matching than Classifier A.


classifier A (user='Alice')


classifier B (user='Alice', source_ip = '192.168.1.0/24')
```

If multiple matching classifiers have the same number of conditions, the classifier whose conditions are described more accurately has a higher degree of matching.

```Plain_Text
-- The CIDR block that is specified in Classifier B is smaller in range than Classifier A. Therefore, Classifier B has a higher degree of matching than Classifier A.


classifier A (user='Alice', source_ip = '192.168.1.0/16')


classifier B (user='Alice', source_ip = '192.168.1.0/24')





-- Classifier C has fewer query types specified in it than Classifier D. Therefore, Classifier has a higher degree of matching than Classifier D.


classifier C (user='Alice', query_type in ('select'))


classifier D (user='Alice', query_type in ('insert','select', 'ctas')）
```

## Isolate computing resources

You can isolate computing resources among queries by configuring resource groups and classifiers.

### Enable resource groups

Execute the following statement to enable resource groups:

```SQL
SET enable_resource_group = true;
```

If you want to globally enable resource groups, execute the following statement:

```SQL
SET GLOBAL enable_resource_group = true;
```

### Create resource groups and classifiers

Execute the following statement to create a resource group, associate the resource group with a classifier, and allocate computing resources to the resource group:

```SQL
CREATE RESOURCE GROUP <group_name> 


TO (user='string', role='string', query_type in ('select'), source_ip='cidr') --Create a classifier. If you create more than one classifier, separate the classifiers with commas (,).


WITH (


    "cpu_core_limit" = "INT",


    "mem_limit" = "m%",


    "type" = "normal" --The type of the resource group. Set the value to normal.


);
```

Example:

```SQL
CREATE RESOURCE GROUP rg1


to 


    (user='rg1_user1', role='rg1_role1', query_type in ('select'), source_ip='192.168.2.1/24'),


    (user='rg1_user2', query_type in ('select'), source_ip='192.168.3.1/24'),


    (user='rg1_user3', source_ip='192.168.4.1/24'),


    (user='rg1_user4')


with (


    'cpu_core_limit' = '10',


    'mem_limit' = '20%',


    'type' = 'normal'


);
```

### View resource groups and classifiers

Execute the following statement to query all resource groups and classifiers:

```SQL
SHOW RESOURCE GROUPS ALL;
```

Execute the following statement to query the resource groups and classifiers of the logged-in user:

```SQL
SHOW RESOURCE GROUPS;
```

Execute the following statement to query a specified resource group and its classifiers:

```SQL
SHOW RESOURCE GROUP <group_name>；
```

Example:

```SQL
mysql> SHOW RESOURCE GROUPS ALL;





+------+--------+--------------+----------+------------------+--------+------------------------------------------------------------------------------------------------------------------------+


| Name | Id     | CPUCoreLimit | MemLimit | ConcurrencyLimit | Type   | Classifiers                                                                                                            |


+------+--------+--------------+----------+------------------+--------+------------------------------------------------------------------------------------------------------------------------+


| rg1  | 300039 | 10           | 20.0%    | 11               | NORMAL | (id=300040, weight=4.409375, user=rg1_user1, role=rg1_role1, query_type in (SELECT), source_ip=192.168.2.1/24)         |


| rg1  | 300039 | 10           | 20.0%    | 11               | NORMAL | (id=300041, weight=3.459375, user=rg1_user2, query_type in (SELECT), source_ip=192.168.3.1/24)                         |


| rg1  | 300039 | 10           | 20.0%    | 11               | NORMAL | (id=300042, weight=2.359375, user=rg1_user3, source_ip=192.168.4.1/24)                                                 |


| rg1  | 300039 | 10           | 20.0%    | 11               | NORMAL | (id=300043, weight=1.0, user=rg1_user4)                                                                                |


+------+--------+--------------+----------+------------------+--------+------------------------------------------------------------------------------------------------------------------------+
```

> Note:
> In the preceding example, `weight` indicates the degree of matching.

### Manage resource groups and classifiers

You can modify the resource quotas for each resource group. You can also add or delete classifiers from resource groups.

Execute the following statement to modify the resource quotas for an existing resource group:

```SQL
ALTER RESOURCE GROUP <group_name> WITH (


    'cpu_core_limit' = '10',


    'mem_limit' = '20%',


    'type' = 'normal'


);
```

Example:

```SQL
-- Change the maximum number of CPU cores that can be allocated to rg1 to 20.


ALTER RESOURCE GROUP rg1 WITH (


    'cpu_core_limit' = '20'


);
```

Execute the following statement to add a classifier to a resource group:

```SQL
ALTER RESOURCE GROUP <group_name> ADD (user='string', role='string', query_type in ('select'), source_ip='cidr');
```

Execute the following statement to delete a classifier from a resource group:

```SQL
ALTER RESOURCE GROUP <group_name> DROP (CLASSIFER_ID_1, CLASSIFIER_ID_2, ...);
```

Execute the following statement to delete all classifiers of a resource group:

```SQL
ALTER RESOURCE GROUP <group_name> DROP ALL;
```

Example:

```SQL
-- Add a classifier to rg1.


ALTER RESOURCE GROUP rg1 ADD (user='root', query_type in ('select'));





-- Delete classifiers 300040, 300041, and 300041 from rg1.


ALTER RESOURCE GROUP rg1 DROP (300040, 300041, 300041);





-- Delete all classifiers of rg1.


ALTER RESOURCE GROUP rg1 DROP ALL;
```

Execute the following statement to delete a resource group:

```SQL
DROP RESOURCE GROUP <group_name>;
```

Example:

```SQL
-- Delete rg1.


DROP RESOURCE GROUP rg1;
```

## What to do next

After you configure resource groups, you can manage memory resources and queries. For more information, see the following topics:

- [Memory management](./Memory_management.md)

- [Query management](./Query_management.md)

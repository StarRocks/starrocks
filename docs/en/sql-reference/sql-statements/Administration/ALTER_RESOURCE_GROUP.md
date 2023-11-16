# ALTER RESOURCE GROUP

## Description

Alters the configuration of a resource group.

## Syntax

```SQL
ALTER RESOURCE GROUP resource_group_name
{  ADD CLASSIFIER1, CLASSIFIER2, ...
 | DROP (CLASSIFER_ID_1, CLASSIFIER_ID_2, ...)
 | DROP ALL
 | WITH resource_limit 
};
```

## Parameters

| **Parameter**       | **Description**                                              |
| ------------------- | ------------------------------------------------------------ |
| resource_group_name | Name of the resource group to be altered.                    |
| ADD                 | Add classifiers to the resource group. See CREATE RESOURCE GROUP - Parameters for more information on how to define a classifier. |
| DROP                | Drop classifiers from the resource group via classifier IDs. You can check the ID of a classifier via SHOW RESOURCE GROUP statement. |
| DROP ALL            | Drop all classifiers from the resource group.                |
| WITH                | Modify the resource limits of the resource group. See CREATE RESOURCE GROUP - Parameters for more information on how to set resource limits. |

## Examples

Example 1: Adds a new classifier to the resource group `rg1`.

```SQL
ALTER RESOURCE GROUP rg1 ADD (user='root', query_type in ('select'));
```

Example 2: Drops classifiers with ID `300040`, `300041`, and `300041` from the resource group `rg1`.

```SQL
ALTER RESOURCE GROUP rg1 DROP (300040, 300041, 300041);
```

Example 3: Drops all classifiers from the resource group `rg1`.

```SQL
ALTER RESOURCE GROUP rg1 DROP ALL;
```

Example 4: modifies the resource limits of the resource group `rg1`.

```SQL
ALTER RESOURCE GROUP rg1 WITH (
    'cpu_core_limit' = '20'
);
```

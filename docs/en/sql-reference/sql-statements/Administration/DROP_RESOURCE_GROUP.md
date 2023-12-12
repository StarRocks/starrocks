---
displayed_sidebar: "English"
---

# DROP RESOURCE GROUP

## Description

Drops the specified resource group.

:::tip

This operation requires the DROP privilege on the target resource group. You can follow the instructions in [GRANT](../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```SQL
DROP RESOURCE GROUP <resource_group_name>
```

## Parameters

| **Parameter**       | **Description**                           |
| ------------------- | ----------------------------------------- |
| resource_group_name | Name of the resource group to be dropped. |

## Example

Example 1: Drops the resource group `rg1`.

```SQL
DROP RESOURCE GROUP rg1;
```

---
displayed_sidebar: "English"
---

# ADMIN SHOW CONFIG

## Description

Displays the configuration of the current cluster (Currently, only FE configuration items can be displayed). For detailed description of these configuration items, see [Configuration](../../../administration/FE_configuration.md#fe-configuration-items).

If you want to set or modify a configuration item, use [ADMIN SET CONFIG](ADMIN_SET_CONFIG.md).

:::tip

This operation requires the SYSTEM-level OPERATE privilege. You can follow the instructions in [GRANT](../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```sql
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"]
```

Note:

Description of the return parameters:

```plain text
1. Key:        Configuration item name
2. Value:      Configuration item value
3. Type:       Configuration item type 
4. IsMutable:  Whether it can be set through the ADMIN SET CONFIG command
5. MasterOnly: Whether it only applies to leader FE
6. Comment:    Configuration item description 
```

## Examples

1. View the configuration of the current FE node.

    ```sql
    ADMIN SHOW FRONTEND CONFIG;
    ```

2. Search for the configuration of the current FE node by using the `like` predicate.  

    ```plain text
    mysql> ADMIN SHOW FRONTEND CONFIG LIKE '%check_java_version%';
    +--------------------+-------+---------+-----------+------------+---------+
    | Key                | Value | Type    | IsMutable | MasterOnly | Comment |
    +--------------------+-------+---------+-----------+------------+---------+
    | check_java_version | true  | boolean | false     | false      |         |
    +--------------------+-------+---------+-----------+------------+---------+
    1 row in set (0.00 sec)
    ```

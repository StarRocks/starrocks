---
displayed_sidebar: docs
---

# ADMIN SHOW BACKEND CONFIG

## Description

Displays the configuration of the backend nodes in the current cluster. This command allows you to view the
configuration items of all BE (Backend) nodes. For detailed descriptions of BE configuration items,
see [Configuration](../../../../administration/management/BE_configuration.md).

If you want to set or modify a configuration item, use [ADMIN SET CONFIG](ADMIN_SET_CONFIG.md).

:::tip

This operation requires the SYSTEM-level OPERATE privilege. You can follow the instructions
in [GRANT](../../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```sql
ADMIN SHOW BACKEND CONFIG [LIKE "pattern"]
```

Note:

Description of the return parameters:

```plain text
1. Host:       Backend host and port
2. Key:        Configuration item name
3. Value:      Configuration item value
4. Type:       Configuration item type 
5. IsMutable:  Whether it can be set through the ADMIN SET CONFIG command
```

## Examples

1. View the configuration of all backend nodes.

    ```sql
    ADMIN SHOW BACKEND CONFIG;
    ```

2. Search for the configuration of backend nodes by using the `like` predicate.

    ```plain text
    mysql> ADMIN SHOW BACKEND CONFIG LIKE 'mem_limit';
    +-------------------+-----------------------------------------------------+-----------+--------+-----------+
    | Host              | Key                                                 | Value     | Type   | IsMutable |
    +-------------------+-----------------------------------------------------+-----------+--------+-----------+
    | 127.0.0.1:9050    | default_mv_resource_group_spill_mem_limit_threshold | 0.8       | double | false     |
    | 127.0.0.1:9050    | local_exchange_buffer_mem_limit_per_driver          | 134217728 | int64  | false     |
    | 127.0.0.1:9050    | mem_limit                                           | 90%       | string | false     |
    | 127.0.0.1:9050    | mem_limited_chunk_queue_block_size                  | 8388608   | int64  | true      |
    | 127.0.0.1:9050    | query_pool_spill_mem_limit_threshold                | 1         | double | false     |
    +-------------------+-----------------------------------------------------+-----------+--------+-----------+
    5 rows in set (0,13 sec)
    ``` 
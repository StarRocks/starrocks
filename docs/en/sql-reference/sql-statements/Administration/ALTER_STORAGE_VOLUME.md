# ALTER STORAGE VOLUME

## Description

Alters the credential properties, comment, or status (`enabled`) of a storage volume. For more about the properties of a storage volume, see [CREATE STORAGE VOLUME](./CREATE_STORAGE_VOLUME.md). This feature is supported from v3.1.

> **CAUTION**
>
> - Only users with the ALTER privilege on a specific storage volume can perform this operation.
> - The `TYPE`, `LOCATIONS`, and other path-related properties of an existing storage volume cannot be altered. You can only alter its credential-related properties. If you changed the path-related configuration items, the databases and tables you created before the change become read-only, and you cannot load data into them.
> - When `enabled` is `false`, the corresponding storage volume cannot be referenced.

## Syntax

```SQL
ALTER STORAGE VOLUME [ IF EXISTS ] <storage_volume_name>
{ COMMENT '<comment_string>'
| SET ("key" = "value"[,...]) }
```

## Parameters

| **Parameter**       | **Description**                          |
| ------------------- | ---------------------------------------- |
| storage_volume_name | The name of the storage volume to alter. |
| COMMENT             | The comment on the storage volume.       |

For detailed information on the properties that can be altered or added, see [CREATE STORAGE VOLUME - PROPERTIES](./CREATE_STORAGE_VOLUME.md#properties).

## Examples

Example 1: Disable the storage volume `my_s3_volume`.

```Plain
MySQL > ALTER STORAGE VOLUME my_s3_volume
    -> SET ("enabled" = "false");
Query OK, 0 rows affected (0.01 sec)
```

Example 2: Alter the credential information of the storage volume `my_s3_volume`.

```Plain
MySQL > ALTER STORAGE VOLUME my_s3_volume
    -> SET (
    ->     "aws.s3.use_instance_profile" = "true"
    -> );
Query OK, 0 rows affected (0.00 sec)
```

## Relevant SQL statements

- [CREATE STORAGE VOLUME](./CREATE_STORAGE_VOLUME.md)
- [DROP STORAGE VOLUME](./DROP_STORAGE_VOLUME.md)
- [SET DEFAULT STORAGE VOLUME](./SET_DEFAULT_STORAGE_VOLUME.md)
- [DESC STORAGE VOLUME](./DESC_STORAGE_VOLUME.md)
- [SHOW STORAGE VOLUMES](./SHOW_STORAGE_VOLUMES.md)

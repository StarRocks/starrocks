# SET DEFAULT STORAGE VOLUME

## Description

Sets a storage volume as the default storage volume. After creating a storage volume for an external data source, you can set it as the default storage volume of your StarRocks cluster. This feature is supported from v3.1.

> **CAUTION**
>
> - Only users with the USAGE privilege on a specific storage volume can perform this operation.
> - The default storage volume cannot be dropped or disabled.
> - You must set the default storage volume for a shared-data StarRocks cluster because StarRocks stores the system statistics information in the default storage volume.

## Syntax

```SQL
SET <storage_volume_name> AS DEFAULT STORAGE VOLUME
```

## Parameters

| **Parameter**       | **Description**                                              |
| ------------------- | ------------------------------------------------------------ |
| storage_volume_name | The name of the storage volume to be set as the default storage volume. |

## Examples

Example 1: Set the storage volume `my_s3_volume` as the default storage volume.

```SQL
MySQL > SET my_s3_volume AS DEFAULT STORAGE VOLUME;
Query OK, 0 rows affected (0.01 sec)
```

## Relevant SQL statements

- [CREATE STORAGE VOLUME](./CREATE_STORAGE_VOLUME.md)
- [ALTER STORAGE VOLUME](./ALTER_STORAGE_VOLUME.md)
- [DROP STORAGE VOLUME](./DROP_STORAGE_VOLUME.md)
- [DESC STORAGE VOLUME](./DESC_STORAGE_VOLUME.md)
- [SHOW STORAGE VOLUMES](./SHOW_STORAGE_VOLUMES.md)

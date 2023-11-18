---
displayed_sidebar: "English"
---

# DROP STORAGE VOLUME

## Description

Drops a storage volume. Dropped storage volumes cannot be referenced anymore. This feature is supported from v3.1.

> **CAUTION**
>
> - Only users with the DROP privilege on a specific storage volume can perform this operation.
> - The default storage volume and the built-in storage volume `builtin_storage_volume` cannot be dropped. You can use [DESC STORAGE VOLUME](./DESC_STORAGE_VOLUME.md) to check whether a storage volume is the default storage volume.
> - Storage volumes that are referenced by existing databases or cloud-native tables cannot be dropped.

## Syntax

```SQL
DROP STORAGE VOLUME [ IF EXISTS ] <storage_volume_name>
```

## Parameters

| **Parameter**       | **Description**                         |
| ------------------- | --------------------------------------- |
| storage_volume_name | The name of the storage volume to drop. |

## Examples

Example 1: Drop the storage volume `my_s3_volume`.

```Plain
MySQL > DROP STORAGE VOLUME my_s3_volume;
Query OK, 0 rows affected (0.01 sec)
```

## Relevant SQL statements

- [CREATE STORAGE VOLUME](./CREATE_STORAGE_VOLUME.md)
- [ALTER STORAGE VOLUME](./ALTER_STORAGE_VOLUME.md)
- [SET DEFAULT STORAGE VOLUME](./SET_DEFAULT_STORAGE_VOLUME.md)
- [DESC STORAGE VOLUME](./DESC_STORAGE_VOLUME.md)
- [SHOW STORAGE VOLUMES](./SHOW_STORAGE_VOLUMES.md)

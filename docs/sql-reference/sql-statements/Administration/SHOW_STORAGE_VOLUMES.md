# SHOW STORAGE VOLUMES

## Description

Shows the storage volumes in your StarRocks cluster. This feature is supported from v3.1.

## Syntax

```SQL
SHOW STORAGE VOLUMES [ LIKE '<pattern>' ]
```

## Parameters

| **Parameter** | **Description**                                |
| ------------- | ---------------------------------------------- |
| pattern       | The pattern used to match the storage volumes. |

## Return value

| **Return**     | **Description**                 |
| -------------- | ------------------------------- |
| Storage Volume | The name of the storage volume. |

## Examples

Example 1: Show all storage volumes in the StarRocks cluster.

```Plain
MySQL > SHOW STORAGE VOLUMES;
+----------------+
| Storage Volume |
+----------------+
| my_s3_volume   |
+----------------+
1 row in set (0.01 sec)
```

## Relevant SQL statements

- [CREATE STORAGE VOLUME](./CREATE_STORAGE_VOLUME.md)
- [ALTER STORAGE VOLUME](./ALTER_STORAGE_VOLUME.md)
- [DROP STORAGE VOLUME](./DROP_STORAGE_VOLUME.md)
- [SET DEFAULT STORAGE VOLUME](./SET_DEFAULT_STORAGE_VOLUME.md)
- [DESC STORAGE VOLUME](./DESC_STORAGE_VOLUME.md)

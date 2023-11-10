---
displayed_sidebar: "Chinese"
---

# SET DEFAULT STORAGE VOLUME

## 功能

设置指定存储卷为默认存储卷。为远程存储系统创建了存储卷之后，您可以设置该存储卷为集群的默认存储卷。该功能自 v3.1 起支持。

> **注意**
>
> - 仅拥有指定存储卷 USAGE 权限的用户可以执行该操作。
> - 默认存储卷无法删除或禁用。
> - 您必须为 StarRocks 存算分离集群设置默认存储卷，因为 StarRocks 会将系统统计信息存储在默认存储卷中。

## 语法

```SQL
SET <storage_volume_name> AS DEFAULT STORAGE VOLUME
```

## 参数说明

| **参数**            | **说明**               |
| ------------------- | ---------------------- |
| storage_volume_name | 待设置的存储卷的名称。 |

## 示例

示例一：设置存储卷 `my_s3_volume` 为默认存储卷。

```SQL
MySQL > SET my_s3_volume AS DEFAULT STORAGE VOLUME;
Query OK, 0 rows affected (0.01 sec)
```

## 相关 SQL

- [CREATE STORAGE VOLUME](./CREATE_STORAGE_VOLUME.md)
- [ALTER STORAGE VOLUME](./ALTER_STORAGE_VOLUME.md)
- [DROP STORAGE VOLUME](./DROP_STORAGE_VOLUME.md)
- [DESC STORAGE VOLUME](./DESC_STORAGE_VOLUME.md)
- [SHOW STORAGE VOLUMES](./SHOW_STORAGE_VOLUMES.md)

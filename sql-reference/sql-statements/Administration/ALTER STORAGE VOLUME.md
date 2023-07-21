# ALTER STORAGE VOLUME

## 功能

更改存储卷的认证属性、注释或状态（`enabled`）。关于存储卷的属性，请参见[CREATE STORAGE VOLUME](./CREATE%20STORAGE%20VOLUME.md)。该功能自 v3.1 起支持。

> **注意**
>
> - 仅拥有特定存储卷 ALTER 权限的用户可以执行该操作。
> - 已有存储卷的 `TYPE` 、`LOCATIONS` 和其他存储路径相关的参数无法更改，仅能更改认证属性。如果您更改了存储路径相关的配置项，则在此之前创建的数据库和表将变为只读，您无法向其中导入数据。
> - `enabled` 为 `false` 的存储卷无法被引用。

## 语法

```SQL
ALTER STORAGE VOLUME [ IF EXISTS ] <storage_volume_name>
{ COMMENT '<comment_string>'
| SET ("key" = "value"[,...]) }
```

## 参数说明

| **参数**            | **说明**                         |
| ------------------- | -------------------------------- |
| storage_volume_name | 待更改或新增属性的存储卷的名称。 |
| COMMENT             | 存储卷的注释。                   |

有关可更改或新增 PROPERTIES 的详细信息，请参阅 [CREATE STORAGE VOLUME - PROPERTIES](./CREATE%20STORAGE%20VOLUME.md#properties)。

## 示例

示例一：禁用存储卷 `my_s3_volume`。

```Plain
MySQL > ALTER STORAGE VOLUME my_s3_volume
    -> SET ("enabled" = "false");
Query OK, 0 rows affected (0.01 sec)
```

示例二：修改存储卷 `my_s3_volume` 的认证信息。

```Plain
MySQL > ALTER STORAGE VOLUME my_s3_volume
    -> SET (
    ->     "aws.s3.use_instance_profile" = "true"
    -> );
Query OK, 0 rows affected (0.00 sec)
```

## 相关 SQL

- [CREATE STORAGE VOLUME](./CREATE%20STORAGE%20VOLUME.md)
- [DROP STORAGE VOLUME](./DROP%20STORAGE%20VOLUME.md)
- [SET DEFAULT STORAGE VOLUME](./SET%20DEFAULT%20STORAGE%20VOLUME.md)
- [DESC STORAGE VOLUME](./DESC%20STORAGE%20VOLUME.md)
- [SHOW STORAGE VOLUMES](./SHOW%20STORAGE%20VOLUMES.md)

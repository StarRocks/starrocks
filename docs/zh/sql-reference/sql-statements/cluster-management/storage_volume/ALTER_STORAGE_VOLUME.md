---
keywords: ['xiugai'] 
displayed_sidebar: docs
description: "ALTER STORAGE VOLUME modifies credential properties, comments, or status of a storage volume."
products: [oss, enterprise, cloud, anywhere]
---

# ALTER STORAGE VOLUME

## 功能

更改存储卷的认证属性、注释或状态（`enabled`）。关于存储卷的属性，请参见[CREATE STORAGE VOLUME](./CREATE_STORAGE_VOLUME.md)。该功能自 v3.1 起支持。

> **注意**
>
> - 仅拥有特定存储卷 ALTER 权限的用户可以执行该操作。
> - 已有存储卷的 `TYPE`、`LOCATIONS` 以及属性 `aws.s3.enable_partitioned_prefix` 和 `aws.s3.num_partitioned_prefix` 无法更改。StarRocks 不会阻止更改 Endpoint、Region 等其他属性，新值会作用于所有使用该存储卷的已有数据库和表。仅应在访问同一存储时更改这些属性，例如轮换认证信息。如果新值指向其他存储，已有数据库和表的数据将无法访问。
> - `enabled` 为 `false` 的存储卷无法被引用。
> - 在 shared_data 模式下，执行 `ALTER STORAGE VOLUME` 时默认会进行存储可访问性校验；该行为受 FE 配置项 `enable_storage_volume_access_check` 控制，可通过关闭该配置禁用校验。如果更新后的凭证或端点无法访问远端存储，语句会失败。

## 语法

```SQL
ALTER STORAGE VOLUME [ IF EXISTS ] <storage_volume_name>
{ COMMENT = '<comment_string>'
| SET ("key" = "value"[,...]) }
```

## 参数说明

| **参数**            | **说明**                         |
| ------------------- | -------------------------------- |
| storage_volume_name | 待更改或新增属性的存储卷的名称。 |
| COMMENT             | 存储卷的注释。                   |

有关可更改或新增 PROPERTIES 的详细信息，请参阅 [CREATE STORAGE VOLUME - PROPERTIES](./CREATE_STORAGE_VOLUME.md#properties)。

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

- [CREATE STORAGE VOLUME](./CREATE_STORAGE_VOLUME.md)
- [DROP STORAGE VOLUME](./DROP_STORAGE_VOLUME.md)
- [SET DEFAULT STORAGE VOLUME](./SET_DEFAULT_STORAGE_VOLUME.md)
- [DESC STORAGE VOLUME](./DESC_STORAGE_VOLUME.md)
- [SHOW STORAGE VOLUMES](./SHOW_STORAGE_VOLUMES.md)

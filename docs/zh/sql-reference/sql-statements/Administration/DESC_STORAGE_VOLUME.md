# DESC STORAGE VOLUME

## 功能

查看指定存储卷的信息。该功能自 v3.1 起支持。

> **注意**
>
> 仅拥有指定存储卷 USAGE 权限的用户可以执行该操作。

## 语法

```SQL
DESC[RIBE] STORAGE VOLUME <storage_volume_name>
```

## 参数说明

| **参数**            | **说明**               |
| ------------------- | ---------------------- |
| storage_volume_name | 待查看的存储卷的名称。 |

## 返回

| **返回**  | **说明**                                                |
| --------- | ------------------------------------------------------- |
| Name      | 存储卷的名称。                                          |
| Type      | 远程存储系统的类型。有效值：`S3`、`AZBLOB` 和 `HDFS`。 |
| IsDefault | 该存储卷是否为默认存储卷。                              |
| Location  | 远程存储系统的位置。                                    |
| Params    | 用于连接远程存储系统的认证信息。                        |
| Enabled   | 该存储卷是否已被启用。                                  |
| Comment   | 存储卷的注释。                                          |

## 示例

示例一：查看存储卷 `my_s3_volume` 的信息。

```Plain
MySQL > DESCRIBE STORAGE VOLUME my_s3_volume\G
*************************** 1. row ***************************
     Name: my_s3_volume
     Type: S3
IsDefault: false
 Location: s3://defaultbucket/test/
   Params: {"aws.s3.access_key":"xxxxxxxxxx","aws.s3.secret_key":"yyyyyyyyyy","aws.s3.endpoint":"https://s3.us-west-2.amazonaws.com","aws.s3.region":"us-west-2","aws.s3.use_instance_profile":"true","aws.s3.use_aws_sdk_default_behavior":"false"}
  Enabled: false
  Comment: 
1 row in set (0.00 sec)
```

## 相关 SQL

- [CREATE STORAGE VOLUME](./CREATE_STORAGE_VOLUME.md)
- [ALTER STORAGE VOLUME](./ALTER_STORAGE_VOLUME.md)
- [DROP STORAGE VOLUME](./DROP_STORAGE_VOLUME.md)
- [SET DEFAULT STORAGE VOLUME](./SET_DEFAULT_STORAGE_VOLUME.md)
- [SHOW STORAGE VOLUMES](./SHOW_STORAGE_VOLUMES.md)

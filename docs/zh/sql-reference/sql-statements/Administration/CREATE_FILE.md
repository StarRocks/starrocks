---
displayed_sidebar: "Chinese"
---

# CREATE FILE

CREATE FILE 语句用于创建文件。文件创建后会自动上传并持久化在 StarRocks 集群中。

:::tip

该操作需要 SYSTEM 级 FILE 权限。请参考 [GRANT](../account-management/GRANT.md) 为用户赋权。当一个文件归属于一个数据库时，对该数据库拥有访问权限的用户都可以使用该文件。

:::

## 基本概念

**文件**：是指创建并保存在 StarRocks 中的文件。每个文件都有一个全局唯一的标识符 (`FileId`)。一个文件由数据库名称 (`database`)、类别 (`catalog`) 和文件名 (`file_name`) 共同定位。

## 语法

```SQL
CREATE FILE "file_name" [IN database]
[properties]
```

## 参数说明

| **参数**   | **必填** | **描述**                                                     |
| ---------- | -------- | ------------------------------------------------------------ |
| file_name  | 是       | 文件名，可根据需求自定义。                                   |
| database   | 否       | 文件所属的数据库。如果没有指定该参数，则使用当前会话的数据库。 |
| properties | 是       | 文件属性，具体配置项见下表：`properties` 配置项。            |

**properties 配置项**

| **配置项** | **必填** | **描述**                                                     |
| ---------- | -------- | ------------------------------------------------------------ |
| url        | 是       | 文件的下载路径。当前仅支持无认证的 HTTP 下载路径。语句执行成功后，文件会被下载并持久化保存在 StarRocks 中。 |
| catalog    | 是       | 文件所属类别，可根据需求自定义。在某些命令中，会查找指定类别下的文件。比如在例行导入中，当数据源为 StarRocks 会查找 Apache Kafka® 类别下的文件。 |
| md5        | 否       | 消息摘要算法。如果指定该参数，StarRocks 会对下载后的文件进行校验。 |

## 示例

- 创建一个名为 **test.pem** 的文件，所属类别为 kafka。

```SQL
CREATE FILE "test.pem"
PROPERTIES
(
    "url" = "http://starrocks-public.oss-cn-xxxx.aliyuncs.com/key/test.pem",
    "catalog" = "kafka"
);
```

- 创建一个名为 **client.key** 的文件，所属类别为 my_catalog。

```SQL
CREATE FILE "client.key"
IN my_database
PROPERTIES
(
    "url" = "http://test.bj.bcebos.com/kafka-key/client.key",
    "catalog" = "my_catalog",
);
```

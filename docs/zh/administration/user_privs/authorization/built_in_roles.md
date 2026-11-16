---
displayed_sidebar: docs
description: "五个内置StarRocks角色的参考：db_admin、cluster_admin、user_admin、security_admin和public。"
sidebar_position: 25
sidebar_label: "内置角色"
products: [oss, enterprise, cloud, anywhere]
---


# StarRocks 支持的内置角色

在 StarRocks 集群中，有五个内置角色：

- `db_admin`
- `cluster_admin`
- `user_admin`
- `security_admin`
- `public`

每个 `admin` 角色都被授予不同的权限，以允许他们在其特定领域执行管理操作。默认情况下，`public` 角色没有任何权限，并被授予可以访问集群的每个用户。

有关下述权限的详细信息，请参见[权限项](./privilege_item.md)。

## `db_admin`

`db_admin` 是内置的数据库管理员。它拥有所有与数据相关的权限以及一些基本的运维权限。

- 专注于数据库和数据的管理
- 不可用于用户或集群管理
- 不可变角色

权限范围：

| 权限级别         | 权限项 |
| ---------------- | ------ |
| SYSTEM           | <ul><li>CREATE RESOURCE</li><li>PLUGIN</li><li>FILE</li><li>BLACKLIST</li><li>OPERATE</li><li>CREATE EXTERNAL CATALOG</li><li>REPOSITORY</li><li>CREATE RESOURCE GROUP</li><li>CREATE GLOBAL FUNCTION</li><li>CREATE STORAGE VOLUME</li><li>SECURITY</li></ul> |
| CATALOG          | <ul><li>USAGE</li><li>DROP</li><li>ALTER</li><li>CREATE DATABASE</li></ul> |
| DATABASE         | <ul><li>DROP</li><li>ALTER</li><li>CREATE TABLE</li><li>CREATE VIEW</li><li>CREATE MATERIALIZED VIEW</li><li>CREATE FUNCTION</li><li>CREATE PIPE</li><li>CREATE MASKING POLICY</li><li>CREATE ROW ACCESS POLICY</li></ul> |
| TABLE            | <ul><li>DROP</li><li>ALTER</li><li>INSERT</li><li>UPDATE</li><li>DELETE</li><li>SELECT</li><li>EXPORT</li></ul> |
| VIEW             | <ul><li>DROP</li><li>ALTER</li><li>SELECT</li></ul> |
| MATERIALIZED VIEW| <ul><li>DROP</li><li>ALTER</li><li>SELECT</li><li>REFRESH</li></ul> |
| RESOURCE         | <ul><li>USAGE</li><li>DROP</li><li>ALTER</li></ul> |
| RESOURCE GROUP   | <ul><li>DROP</li><li>ALTER</li></ul> |
| FUNCTION         | <ul><li>USAGE</li><li>DROP</li></ul> |
| GLOBAL FUNCTION  | <ul><li>USAGE</li><li>DROP</li></ul> |
| STORAGE VOLUME   | <ul><li>USAGE</li><li>DROP</li><li>ALTER</li></ul> |
| PIPE             | <ul><li>USAGE</li><li>DROP</li><li>ALTER</li></ul> |

## `cluster_admin`

`cluster_admin` 是内置的集群管理员。

- 专注于集群基础设施的管理
- 拥有节点管理的权限
- 不可变角色

权限范围：

| 权限级别         | 权限项         |
| ---------------- | -------------- |
| SYSTEM           | NODE           |

## `user_admin`

`user_admin` 是内置的用户管理员。它可用于管理用户、角色和授权。

- 专注于用户和权限的管理
- 能够创建、修改和删除用户
- 能够授予或撤销权限或角色
- 不可变角色

权限范围：

| 权限级别         | 权限项        |
| ---------------- | ------------- |
| SYSTEM           | GRANT         |

## `security_admin`

`security_admin` 是内置的安全管理员。它可用于管理安全集成和 Group Provider。

- 专注于系统安全的管理
- 能够管理与安全相关的配置和策略
- 不可变角色

权限范围：

| 权限级别         | 权限项        |
| ---------------- | ------------- |
| SYSTEM           | <ul><li>SECURITY</li><li>OPERATE</li></ul> |

## `public`

`public` 是授予每个可以访问集群的用户的内置角色。默认情况下，它没有任何权限。

- 自动授予并激活给所有集群用户
- 可变角色。如果您希望将权限或角色授予所有集群用户，可以将其授予此角色。
---
displayed_sidebar: docs
sidebar_position: 25
sidebar_label: "内置角色"
---

import DBAdmin from '../../../_assets/commonMarkdown/role_db_admin.mdx'
import ClusterAdmin from '../../../_assets/commonMarkdown/role_cluster_admin.mdx'

# StarRocks 支持的内置角色

在 StarRocks 集群中，有五个内置角色：

- `db_admin`
- `cluster_admin`
- `user_admin`
- `security_admin`
- `public`

每个 `admin` 角色都被授予不同的权限，以允许他们在其特定领域执行管理操作。默认情况下，`public` 角色没有任何权限，并被授予可以访问集群的每个用户。

有关下述权限的详细信息，请参见[权限项](./privilege_item.md)。

<DBAdmin />

<ClusterAdmin />

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

`security_admin` 是内置的安全管理员。它可用于管理安全集成和组提供者。

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
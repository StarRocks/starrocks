---
displayed_sidebar: docs
sidebar_position: 25
sidebar_label: "Built-in Roles"
---

import DBAdmin from '../../../_assets/commonMarkdown/role_db_admin.mdx'
import ClusterAdmin from '../../../_assets/commonMarkdown/role_cluster_admin.mdx'

# Built-in Roles supported by StarRocks

In a StarRocks cluster, there are FIVE built-in roles:

- `db_admin`
- `cluster_admin`
- `user_admin`
- `security_admin`
- `public`

Each of the `admin` roles is granted with different privileges to allow them to perform administrative operations on their specific domain. By default, the `public` role has no privileges and is granted to every user that can access the cluster.

For details of the privileges described below, see [Privilege Item](./privilege_item.md).

<DBAdmin />

<ClusterAdmin />

## `user_admin`

`user_admin` is the built-in user administrator. It can be used to manage users, roles, and authorization.

- Focused on management of users and privileges
- Able to create, alter, and drop users
- Able to grant or revoke privileges or roles
- Immutable role

Privilege scope:

| Privilege Level   | Privilege Item |
| ----------------- | -------------- |
| SYSTEM            | GRANT          |

## `security_admin`

`security_admin` is the built-in security administrator. It can be used to manage security integrations and group providers.

- Focused on management of system security
- Able to manage security-related configurations and strategies
- Immutable role

Privilege scope:

| Privilege Level   | Privilege Item |
| ----------------- | -------------- |
| SYSTEM            | <ul><li>SECURITY</li><li>OPERATE</li></ul> |

## `public`

`public` is the built-in role that is granted to every user that can access the cluster. By default, it has no privilege.

- Automatically granted and activated to all cluster users
- Mutable role. You can grant privileges or roles to this role if you want to grant them to all cluster users.

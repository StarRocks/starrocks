---
displayed_sidebar: docs
sidebar_position: 25
sidebar_label: "Built-in Roles"
products: [oss, enterprise, cloud, anywhere]
---


# Built-in Roles supported by StarRocks

In a StarRocks cluster, there are FIVE built-in roles:

- `db_admin`
- `cluster_admin`
- `user_admin`
- `security_admin`
- `public`

Each of the `admin` roles is granted with different privileges to allow them to perform administrative operations on their specific domain. By default, the `public` role has no privileges and is granted to every user that can access the cluster.

For details of the privileges described below, see [Privilege Item](./privilege_item.md).

## `db_admin`

`db_admin` is the built-in database administrator. It has all data-related privileges and some basic privileges on operations and maintenance.

- Focused on management of databases and data
- Unavailable for user or cluster management
- Immutable role

Privilege scope:

| Privilege Level   | Privilege Item |
| ----------------- | -------------- |
| SYSTEM            | <ul><li>CREATE RESOURCE</li><li>PLUGIN</li><li>FILE</li><li>BLACKLIST</li><li>OPERATE</li><li>CREATE EXTERNAL CATALOG</li><li>REPOSITORY</li><li>CREATE RESOURCE GROUP</li><li>CREATE GLOBAL FUNCTION</li><li>CREATE STORAGE VOLUME</li><li>SECURITY</li></ul> |
| CATALOG           | <ul><li>USAGE</li><li>DROP</li><li>ALTER</li><li>CREATE DATABASE</li></ul> |
| DATABASE          | <ul><li>DROP</li><li>ALTER</li><li>CREATE TABLE</li><li>CREATE VIEW</li><li>CREATE MATERIALIZED VIEW</li><li>CREATE FUNCTION</li><li>CREATE PIPE</li><li>CREATE MASKING POLICY</li><li>CREATE ROW ACCESS POLICY</li></ul> |
| TABLE             | <ul><li>DROP</li><li>ALTER</li><li>INSERT</li><li>UPDATE</li><li>DELETE</li><li>SELECT</li><li>EXPORT</li></ul> |
| VIEW              | <ul><li>DROP</li><li>ALTER</li><li>SELECT</li></ul> |
| MATERIALIZED VIEW | <ul><li>DROP</li><li>ALTER</li><li>SELECT</li><li>REFRESH</li></ul> |
| RESOURCE          | <ul><li>USAGE</li><li>DROP</li><li>ALTER</li></ul> |
| RESOURCE GROUP    | <ul><li>DROP</li><li>ALTER</li></ul> |
| FUNCTION          | <ul><li>USAGE</li><li>DROP</li></ul> |
| GLOBAL FUNCTION   | <ul><li>USAGE</li><li>DROP</li></ul> |
| STORAGE VOLUME    | <ul><li>USAGE</li><li>DROP</li><li>ALTER</li></ul> |
| PIPE              | <ul><li>USAGE</li><li>DROP</li><li>ALTER</li></ul> |

## `cluster_admin`

`cluster_admin` is the built-in cluster administrator.

- Focused on management of cluster infrastructure
- Granted with privileges on node management
- Immutable role

Privilege scope:

| Privilege Level   | Privilege Item |
| ----------------- | -------------- |
| SYSTEM            | NODE           |

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

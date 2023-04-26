# SET DEFAULT ROLE

## Description

Sets the roles that are activated by default when the user connects to the server.

This command is supported from v3.0.

## Syntax

```SQL
-- Set specified roles as default roles.
SET DEFAULT ROLE <role_name>[,<role_name>,..] TO <user_identity>;
-- Set all roles of the user, including roles that will be assigned to this user, as default roles. 
SET DEFAULT ROLE ALL TO <user_identity>;
-- No default role is set but the public role is still enabled after a user login. 
SET DEFAULT ROLE NONE TO <user_identity>; 
```

## Parameters

`role_name`: the role name

`user_identity`: the user identity

## Usage notes

Individual users can set default roles for themselves. `user_admin` can set default roles for other users. Before you perform this operation, make sure that the user has already been assigned these roles.

You can query the roles of a user using [SHOW GRANTS](SHOW%20GRANTS.md).

## Examples

Query the roles of the current user.

```SQL
SHOW GRANTS FOR test;
+--------------+---------+----------------------------------------------+
| UserIdentity | Catalog | Grants                                       |
+--------------+---------+----------------------------------------------+
| 'test'@'%'   | NULL    | GRANT 'db_admin', 'user_admin' TO 'test'@'%' |
+--------------+---------+----------------------------------------------+
```

Example 1: Set roles `db_admin` and `user_admin` as the default roles for the user `test`.

```SQL
SET DEFAULT ROLE db_admin TO test;
```

Example 2: Set all roles of the user `test`, including roles that will be assigned to this user as default roles.

```SQL
SET DEFAULT ROLE ALL TO test;
```

Example 3: Clear all the default roles of the user `test`.

```SQL
SET DEFAULT ROLE NONE TO test;
```

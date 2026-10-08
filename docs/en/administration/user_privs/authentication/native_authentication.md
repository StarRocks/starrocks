---
displayed_sidebar: docs
sidebar_position: 10
description: "Create and manage users using the native authentication within StarRocks through SQL commands."
---

# Native Authentication

import EditionSpecificDefaultAdminUser from '../../../_assets/commonMarkdown/Edition_Specific_Default_Admin_User.mdx'
import EditionSpecificResetLostAdminPassword from '../../../_assets/commonMarkdown/Edition_Specific_Reset_Lost_Admin_Password.mdx'

Create and manage users using the native authentication within StarRocks through SQL commands.

StarRocks native authentication is a password-based authentication method. In addition to that, StarRocks also supports integrating with external authentication systems such as LDAP. For more instructions, see [Authenticate with Security Integration](./security_integration.md).

:::note

Users with the system-defined role `user_admin` can create users, alter users, and drop users in StarRocks.

:::

<EditionSpecificDefaultAdminUser />

## Create user

You can create a user by specifying the user identity, the authentication method, and optionally the default role. To enable the native authentication for the user, you need to explicitly specify the password in plaintext or ciphertext.

The following example creates the user `jack`, allows it to connect only from the IP address `172.10.1.10`, enables the native authentication, sets the password to `12345` in plaintext, and assigns the role `example_role` to it as its default role:

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED BY '12345' DEFAULT ROLE 'example_role';
```

:::note
- StarRocks encrypts users' passwords before storing them. You can get the encrypted password using the password() function.
- A system-defined default role `PUBLIC` is assigned to a user if no default role is specified during user creation.
:::

The default role of a user is automatically activated when the user connects to StarRocks. For instructions on how to enable all (default and granted) roles for a user after connection, see [Enable all roles](../authorization/User_privilege.md#enable-all-roles).

For more information and advanced instructions on creating a user, see [CREATE USER](../../../sql-reference/sql-statements/account-management/CREATE_USER.md).

## Alter user

You can alter the password, default role, or property for a user.

For instructions on how to alter the default role for a user, see [Alter default role](../authorization/User_privilege.md#alter-the-default-role-of-a-user).

### Alter the property of a user

You can set the property of a user using [ALTER USER](../../../sql-reference/sql-statements/account-management/ALTER_USER.md).

The following example sets the maximum number of connections for user `jack` to `1000`. User identities that have the same user name share the same property.

Therefore, you only need to set the property for `jack` and this setting takes effect for all the user identities with the user name `jack`.

```SQL
ALTER USER 'jack' SET PROPERTIES ("max_user_connections" = "1000");
```

### Reset password for a user

You can reset the password for a user using [SET PASSWORD](../../../sql-reference/sql-statements/account-management/SET_PASSWORD.md) or [ALTER USER](../../../sql-reference/sql-statements/account-management/ALTER_USER.md).

> **NOTE**
>
> - Any user can reset their own passwords without needing any privileges.

Both the following examples reset the password of `jack` to `54321`:

- Reset the password using SET PASSWORD:

  ```SQL
  SET PASSWORD FOR jack@'172.10.1.10' = PASSWORD('54321');
  ```

- Reset the password using ALTER USER:

  ```SQL
  ALTER USER jack@'172.10.1.10' IDENTIFIED BY '54321';
  ```

<EditionSpecificResetLostAdminPassword />

## Drop a user

You can drop a user using [DROP USER](../../../sql-reference/sql-statements/account-management/DROP_USER.md).

The following example drops the user `jack`:

```SQL
DROP USER jack@'172.10.1.10';
```

## View users

You can view all the users within the StarRocks cluster using SHOW USERS.

```SQL
SHOW USERS;
```

## View user property

You can view the property of a user using [SHOW PROPERTY](../../../sql-reference/sql-statements/account-management/SHOW_PROPERTY.md).

The following example shows the property of the user `jack`:

```SQL
SHOW PROPERTY FOR 'jack';
```

Or to view a specific property:

```SQL
SHOW PROPERTY FOR 'jack' LIKE 'max_user_connections';
```


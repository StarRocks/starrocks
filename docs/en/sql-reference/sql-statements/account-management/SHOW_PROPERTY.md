---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "SHOW PROPERTY displays properties of a user, including the maximum number of connections, the default catalog, and the default database."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# SHOW PROPERTY

SHOW PROPERTY displays properties of a user. Currently, only the maximum number of connections can be viewed using this command.

:::tip
The current user can view its own property. Only users with the `user_admin` role can view the property of other users.

:::

## Syntax

```SQL
SHOW PROPERTY [FOR 'user_name'] [LIKE '<property_name>']
```

## Parameters

| **Parameter**        | **Required** | **Description**                                                              |
| -------------------- | ------------ | ---------------------------------------------------------------------------- |
| user_name            | No           | The user name. If not specified, the property of the current user is viewed. |
| property_name        | No           | The user property name.                                                      |

## Examples

Example 1: View the property the current user.

```Plain
SHOW PROPERTY;

+----------------------+-------+
| Key                  | Value |
+----------------------+-------+
| max_user_connections | 10000 |
+----------------------+-------+
```

Example 2: View the property of the user `jack`.

```SQL
SHOW PROPERTY FOR 'jack';
```

Or

```SQL
SHOW PROPERTY FOR 'jack' LIKE 'max_user_connections';
```

```Plain
+----------------------+-------+
| Key                  | Value |
+----------------------+-------+
| max_user_connections | 100   |
+----------------------+-------+
```

## See also

[ALTER USER](./ALTER_USER.md): Sets properties for a user.

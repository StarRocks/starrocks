---
displayed_sidebar: docs
---

# SHOW PROPERTY

SHOW PROPERTY displays properties of a user, including the maximum number of connections, the default catalog, and the default database.

:::tip
The current user can view their own properties. Only users with the `user_admin` role can view the properties of other users.

:::

:::info
To set properties such as `database` or `catalog`, use the [ALTER USER](./ALTER_USER.md) command with `SET PROPERTIES`.
For `max_user_connections`, you can use the `SET PROPERTY` syntax.
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

Example 1: View the properties of the current user.

```sql
SHOW PROPERTY;
```

```Plain
+----------------------+-----------------+
| Key                  | Value           |
+----------------------+-----------------+
| max_user_connections | 1024            |
| catalog              | default_catalog |
| database             |                 |
+----------------------+-----------------+
```

Example 2: View the properties of the user `jack`.

```SQL
SHOW PROPERTY FOR 'jack';
```

```Plain
+----------------------+------------------+
| Key                  | Value            |
+----------------------+------------------+
| max_user_connections | 100              |
| catalog              | default_catalog  |
| database             | sales_db         |
+----------------------+------------------+
```

Example 3: Filter properties using `LIKE`.

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

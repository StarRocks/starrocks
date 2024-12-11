---
displayed_sidebar: docs
---

# SHOW PROPERTY

## Description

Views the properties of a user. Currently, only the maximum number of connections can be viewed using this command.

:::tip
The current user can view its own property. Only users with the `user_admin` role can view the property of other users.

:::

## Syntax

```SQL
<<<<<<< HEAD
SHOW PROPERTY [FOR 'user_name'] [LIKE 'max_user_connections']
=======
SHOW PROPERTY [FOR 'user_name'] [LIKE '<property_name>']
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
```

## Parameters

<<<<<<< HEAD
| **Parameter**              | **Required** | **Description**                                    |
| -------------------- | -------- | ----------------------------------------- |
| user_name            | No       | The user name. If not specified, the property of the current user is viewed. |
| max_user_connections | No       | The maximum number of connections for a user.      |

## Examples

Example 1: View the maximum number of connections for the current user.
=======
| **Parameter**        | **Required** | **Description**                                                              |
| -------------------- | ------------ | ---------------------------------------------------------------------------- |
| user_name            | No           | The user name. If not specified, the property of the current user is viewed. |
| property_name        | No           | The user property name.                                                      |

## Examples

Example 1: View the property the current user.
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

```Plain
SHOW PROPERTY;

+----------------------+-------+
| Key                  | Value |
+----------------------+-------+
| max_user_connections | 10000 |
+----------------------+-------+
```

<<<<<<< HEAD
Example 2: View the maximum number of connections for user `jack`.
=======
Example 2: View the property of the user `jack`.
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

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

<<<<<<< HEAD
[SET PROPERTY](./SET_PROPERTY.md): Sets the maximum number of connections for a user.
=======
[ALTER USER](./ALTER_USER.md): Sets properties for a user.
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

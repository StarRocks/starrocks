---
displayed_sidebar: docs
---

# applicable_roles

`applicable_roles` provides information about roles that are applicable to the current user.

The following fields are provided in `applicable_roles`:

| **Field**      | **Description**                                              |
| -------------- | ------------------------------------------------------------ |
| USER           | The user to whom the role is applicable.                     |
| HOST           | The host from which the user connects.                       |
| GRANTEE        | The user or role that granted the role.                      |
| GRANTEE_HOST   | The host from which the grantor connects.                    |
| ROLE_NAME      | The name of the role.                                        |
| ROLE_HOST      | The host associated with the role.                           |
| IS_GRANTABLE   | Whether the role can be granted to others (`YES` or `NO`).   |
| IS_DEFAULT     | Whether the role is a default role (`YES` or `NO`).          |
| IS_MANDATORY   | Whether the role is mandatory (`YES` or `NO`).               |

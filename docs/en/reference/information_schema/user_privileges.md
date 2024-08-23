---
displayed_sidebar: docs
---

# user_privileges

:::note

This view does not apply to the available features in StarRocks.

:::

`user_privileges` provides information about user privileges.

The following fields are provided in `user_privileges`:

| **Field**      | **Description**                                              |
| -------------- | ------------------------------------------------------------ |
| GRANTEE        | The name of the user to which the privilege is granted.      |
| TABLE_CATALOG  | The name of the catalog. This value is always `def`.         |
| PRIVILEGE_TYPE | The privilege granted. The value can be any privilege that can be granted at the global level. |
| IS_GRANTABLE   | `YES` if the user has the `GRANT OPTION` privilege, `NO` otherwise. The output does not list `GRANT OPTION` as a separate row with `PRIVILEGE_TYPE='GRANT OPTION'`. |

---
displayed_sidebar: "English"
---

# table_privileges

`table_privileges` provides information about table privileges.

The following fields are provided in `table_privileges`:

| **Field**      | **Description**                                              |
| -------------- | ------------------------------------------------------------ |
| GRANTEE        | The name of the user to which the privilege is granted.      |
| TABLE_CATALOG  | The name of the catalog to which the table belongs. This value is always `def`. |
| TABLE_SCHEMA   | The name of the database to which the table belongs.         |
| TABLE_NAME     | The name of the table.                                       |
| PRIVILEGE_TYPE | The privilege granted. The value can be any privilege that can be granted at the table level. |
| IS_GRANTABLE   | `YES` if the user has the `GRANT OPTION` privilege, `NO` otherwise. The output does not list `GRANT OPTION` as a separate row with `PRIVILEGE_TYPE='GRANT OPTION'`. |

---
displayed_sidebar: "English"
---

# column_privileges

`column_privileges` identifies all privileges granted on columns to a currently enabled role or by a currently enabled role.

The following fields are provided in `column_privileges`:

| **Field**      | **Description**                                              |
| -------------- | ------------------------------------------------------------ |
| GRANTEE        | The name of the user to which the privilege is granted.      |
| TABLE_CATALOG  | The name of the catalog to which the table containing the column belongs. This value is always `def`. |
| TABLE_SCHEMA   | The name of the database to which the table containing the column belongs. |
| TABLE_NAME     | The name of the table containing the column.                 |
| COLUMN_NAME    | The name of the column.                                      |
| PRIVILEGE_TYPE | The privilege granted. The value can be any privilege that can be granted at the column level. Each row lists a single privilege, so there is one row per column privilege held by the grantee. |
| IS_GRANTABLE   | `YES` if the user has the `GRANT OPTION` privilege, `NO` otherwise. The output does not list `GRANT OPTION` as a separate row with `PRIVILEGE_TYPE='GRANT OPTION'`. |

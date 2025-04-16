---
displayed_sidebar: docs
---

# schema_privileges

:::note

This view does not apply to the available features in StarRocks.

:::

`schema_privileges` provides information about database privileges.

The following fields are provided in `schema_privileges`:

| **Field**      | **Description**                                              |
| -------------- | ------------------------------------------------------------ |
| GRANTEE        | The name of the user to which the privilege is granted.      |
| TABLE_CATALOG  | The name of the catalog to which the schema belongs. This value is always `def`. |
| TABLE_SCHEMA   | The name of the schema.                                      |
| PRIVILEGE_TYPE | The privilege granted. Each row lists a single privilege, so there is one row per schema privilege held by the grantee. |
| IS_GRANTABLE   | `YES` if the user has the `GRANT OPTION` privilege, `NO` otherwise. The output does not list `GRANT OPTION` as a separate row with `PRIVILEGE_TYPE='GRANT OPTION'`. |

---
displayed_sidebar: docs
---

# routines

:::note

This view does not apply to the available features in StarRocks.

:::

`routines` contains all stored routines (stored procedures and stored functions).

The following fields are provided in `routine`:

| **Field**            | **Description**                                              |
| -------------------- | ------------------------------------------------------------ |
| SPECIFIC_NAME        | The name of the routine.                                     |
| ROUTINE_CATALOG      | The name of the catalog to which the routine belongs. This value is always `def`. |
| ROUTINE_SCHEMA       | The name of the database to which the routine belongs.       |
| ROUTINE_NAME         | The name of the routine.                                     |
| ROUTINE_TYPE         | `PROCEDURE` for stored procedures, `FUNCTION` for stored functions. |
| DTD_IDENTIFIER       | If the routine is a stored function, the return value data type. If the routine is a stored procedure, this value is empty. |
| ROUTINE_BODY         | The language used for the routine definition. This value is always `SQL`. |
| ROUTINE_DEFINITION   | The text of the SQL statement executed by the routine.       |
| EXTERNAL_NAME        | This value is always `NULL`.                                 |
| EXTERNAL_LANGUAGE    | The language of the stored routine.                          |
| PARAMETER_STYLE      | This value is always `SQL`.                                  |
| IS_DETERMINISTIC     | `YES` or `NO`, depending on whether the routine is defined with the `DETERMINISTIC` characteristic. |
| SQL_DATA_ACCESS      | The data access characteristic for the routine. The value is one of `CONTAINS SQL`, `NO SQL`, `READS SQL DATA`, or `MODIFIES SQL DATA`. |
| SQL_PATH             | This value is always `NULL`.                                 |
| SECURITY_TYPE        | The routine `SQL SECURITY` characteristic. The value is one of `DEFINER` or `INVOKER`. |
| CREATED              | The date and time when the routine was created. This is a `DATETIME` value. |
| LAST_ALTERED         | The date and time when the routine was last modified. This is a `DATETIME` value. If the routine has not been modified since its creation, this value is the same as the `CREATED` value. |
| SQL_MODE             | The SQL mode in effect when the routine was created or altered, and under which the routine executes. |
| ROUTINE_COMMENT      | The text of the comment, if the routine has one. If not, this value is empty. |
| DEFINER              | The user named in the `DEFINER` clause (often the user who created the routine). |
| CHARACTER_SET_CLIENT |                                                              |
| COLLATION_CONNECTION |                                                              |
| DATABASE_COLLATION   | The collation of the database with which the routine is associated. |
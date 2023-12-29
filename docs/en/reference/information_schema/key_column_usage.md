---
displayed_sidebar: "English"
---

# key_column_usage

`key_column_usage` identifies all columns that are restricted by some unique, primary key, or foreign key constraint.

The following fields are provided in `key_column_usage`:

| **Field**                     | **Description**                                              |
| ----------------------------- | ------------------------------------------------------------ |
| CONSTRAINT_CATALOG            | The name of the catalog to which the constraint belongs. This value is always `def`. |
| CONSTRAINT_SCHEMA             | The name of the database to which the constraint belongs.    |
| CONSTRAINT_NAME               | The name of the constraint.                                  |
| TABLE_CATALOG                 | The name of the catalog to which the table belongs. This value is always `def`. |
| TABLE_SCHEMA                  | The name of the database to which the table belongs.         |
| TABLE_NAME                    | The name of the table that has the constraint.               |
| COLUMN_NAME                   | The name of the column that has the constraint.If the constraint is a foreign key, then this is the column of the foreign key, not the column that the foreign key references. |
| ORDINAL_POSITION              | The column's position within the constraint, not the column's position within the table. Column positions are numbered beginning with 1. |
| POSITION_IN_UNIQUE_CONSTRAINT | `NULL` for unique and primary-key constraints. For foreign-key constraints, this column is the ordinal position in key of the table that is being referenced. |
| REFERENCED_TABLE_SCHEMA       | The name of the schema referenced by the constraint.         |
| REFERENCED_TABLE_NAME         | The name of the table referenced by the constraint.          |
| REFERENCED_COLUMN_NAME        | The name of the column referenced by the constraint.         |

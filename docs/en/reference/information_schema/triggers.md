---
displayed_sidebar: "English"
---

# triggers

`triggers` provides information about triggers.

The following fields are provided in `triggers`:

| **Field**                  | **Description**                                              |
| -------------------------- | ------------------------------------------------------------ |
| TRIGGER_CATALOG            | The name of the catalog to which the trigger belongs. This value is always `def`. |
| TRIGGER_SCHEMA             | The name of the database to which the trigger belongs.       |
| TRIGGER_NAME               | The name of the trigger.                                     |
| EVENT_MANIPULATION         | The trigger event. This is the type of operation on the associated table for which the trigger activates. The value is `INSERT` (a row was inserted), `DELETE` (a row was deleted), or `UPDATE` (a row was modified). |
| EVENT_OBJECT_CATALOG       | Every trigger is associated with exactly one table. The catalog in which this table occurs. |
| EVENT_OBJECT_SCHEMA        | Every trigger is associated with exactly one table. The database in which this table occurs. |
| EVENT_OBJECT_TABLE         | The name of the table that the trigger is associated with.   |
| ACTION_ORDER               | The ordinal position of the trigger's action within the list of triggers on the same table with the same `EVENT_MANIPULATION` and `ACTION_TIMING` values. |
| ACTION_CONDITION           | This value is always `NULL`.                                 |
| ACTION_STATEMENT           | The trigger body; that is, the statement executed when the trigger activates. This text uses UTF-8 encoding. |
| ACTION_ORIENTATION         | This value is always `ROW`.                                  |
| ACTION_TIMING              | Whether the trigger activates before or after the triggering event. The value is `BEFORE` or `AFTER`. |
| ACTION_REFERENCE_OLD_TABLE | This value is always `NULL`.                                 |
| ACTION_REFERENCE_NEW_TABLE | This value is always `NULL`.                                 |
| ACTION_REFERENCE_OLD_ROW   | The old column identifiers. The value is always `OLD`.       |
| ACTION_REFERENCE_NEW_ROW   | The new column identifiers. The value is always `NEW`.       |
| CREATED                    | The date and time when the trigger was created. This is a `DATETIME(2)` value (with a fractional part in hundredths of seconds) for triggers. |
| SQL_MODE                   | The SQL mode in effect when the trigger was created, and under which the trigger executes. |
| DEFINER                    | The user named in the `DEFINER` clause (often the user who created the trigger). |
| CHARACTER_SET_CLIENT       |                                                              |
| COLLATION_CONNECTION       |                                                              |
| DATABASE_COLLATION         | The collation of the database with which the trigger is associated. |

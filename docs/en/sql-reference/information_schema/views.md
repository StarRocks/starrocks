---
displayed_sidebar: docs
description: "views provides information about all user-defined views."
---

# views

`views` provides information about all user-defined views.

The following fields are provided in `views`:

| **Field**            | **Description**                                              |
| -------------------- | ------------------------------------------------------------ |
| TABLE_CATALOG        | The name of the catalog to which the view belongs. This value is always `def`. |
| TABLE_SCHEMA         | The name of the database to which the view belongs.          |
| TABLE_NAME           | The name of the view.                                        |
| VIEW_DEFINITION      | The `SELECT` statement that provides the definition of the view. |
| CHECK_OPTION         | The value of the `CHECK_OPTION` attribute. StarRocks does not support `WITH CHECK OPTION` on views, so this value is always `NONE`. |
| IS_UPDATABLE         | Whether the view is updatable. This column is not populated, so the value is always `NO`. |
| DEFINER              | The user who created the view. This column is not populated, so the value is always an empty string. |
| SECURITY_TYPE        | The view `SQL SECURITY` characteristic. This column is not populated, so the value is always an empty string. It does not reflect the `SECURITY` clause of [CREATE VIEW](../sql-statements/View/CREATE_VIEW.md). |
| CHARACTER_SET_CLIENT | The character set of the client connection that created the view. This value is always `utf8`. |
| COLLATION_CONNECTION | The collation of the client connection that created the view. This value is always `utf8_general_ci`. |


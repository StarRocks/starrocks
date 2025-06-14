---
displayed_sidebar: docs
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
| CHECK_OPTION         | The value of the `CHECK_OPTION` attribute. The value is one of `NONE`, `CASCADE`, or `LOCAL`. |
| IS_UPDATABLE         | Whether the view is updatable. The flag is set to `YES` (true) if `UPDATE` and `DELETE` (and similar operations) are legal for the view. Otherwise, the flag is set to `NO` (false). If a view is not updatable, statements such `UPDATE`, `DELETE`, and `INSERT` are illegal and are rejected. |
| DEFINER              | The user of the user who created the view.                   |
| SECURITY_TYPE        | The view `SQL SECURITY` characteristic. The value is one of `DEFINER` or `INVOKER`. |
| CHARACTER_SET_CLIENT |                                                              |
| COLLATION_CONNECTION |                                                              |


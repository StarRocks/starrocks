---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "verbose_session_variables provides session variable details including default values and whether they have been modified."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# verbose_session_variables

`verbose_session_variables` provides detailed information about session variables, including their default values and whether they have been changed from the default.

The following fields are provided in `verbose_session_variables`:

| **Field**       | **Description**                                              |
| --------------- | ------------------------------------------------------------ |
| VARIABLE_NAME   | The name of the session variable.                            |
| VARIABLE_VALUE  | The current value of the session variable.                   |
| DEFAULT_VALUE   | The default value of the session variable.                   |
| IS_CHANGED      | Indicates whether the variable's value has been changed from its default (`true`) or not (`false`). |

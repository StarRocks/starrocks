---
displayed_sidebar: "English"
---

# engines

`engines` provides information about storage engines.

The following fields are provided in `engines`:

| **Field**    | **Description**                                              |
| ------------ | ------------------------------------------------------------ |
| ENGINE       | The name of the storage engine.                              |
| SUPPORT      | The server's level of support for the storage engine. Valid values:<ul><li>`YES`: The engine is supported and active.</li><li>`DEFAULT`: Like `YES`, plus this is the default engine.</li><li>`NO`: The engine is not supported.</li><li>`DISABLED`: The engine is supported but has been disabled.</li></ul> |
| COMMENT      | A brief description of the storage engine.                   |
| TRANSACTIONS | Whether the storage engine supports transactions.            |
| XA           | Whether the storage engine supports XA transactions.         |
| SAVEPOINTS   | Whether the storage engine supports savepoints.              |

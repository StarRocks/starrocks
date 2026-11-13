---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "collations contains the available collations."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# collations

`collations` contains the available collations.

The following fields are provided in `collations`:

| **Field**          | **Description**                                              |
| ------------------ | ------------------------------------------------------------ |
| COLLATION_NAME     | The collation name.                                          |
| CHARACTER_SET_NAME | The name of the character set with which the collation is associated. |
| ID                 | The collation ID.                                            |
| IS_DEFAULT         | Whether the collation is the default for its character set.  |
| IS_COMPILED        | Whether the character set is compiled into the server.       |
| SORTLEN            | This is related to the amount of memory required to sort strings expressed in the character set. |

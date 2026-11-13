---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the name of the current database."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# database



Returns the name of the current database. If no database is selected, an empty value is returned.

## Syntax

```plaintext
database()
```

## Parameters

This function does not require parameters.

## Return value

Returns the name of the current database as a string.

## Examples

```sql
-- Select a destination database.
use db_test

-- Query the name of the current database.
select database();
+------------+
| DATABASE() |
+------------+
| db_test    |
+------------+
```

## See also

[USE](../../sql-statements/Database/USE.md): Switches to a destination database.

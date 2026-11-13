---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "DROP VIEW is used to drop a logical view VIEW"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# DROP VIEW

DROP VIEW is used to drop a logical view VIEW

## Syntax

```sql
DROP VIEW [IF EXISTS]
[db_name.]view_name
```

## Examples

1. If it exists, then drop the view example_view on example_db.

    ```sql
    DROP VIEW IF EXISTS example_db.example_view;
    ```

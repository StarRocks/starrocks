---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Use DROP RESOURCE to drop an existing resource."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# DROP RESOURCE

Use DROP RESOURCE to drop an existing resource. Only root or superuser can drop resources.

Syntax:

```sql
DROP RESOURCE 'resource_name'
```

## Examples

1. Drop Spark resource named spark0.

    ```SQL
    DROP RESOURCE 'spark0';
    ```

2. Drop Hive resource named hive0.

    ```SQL
    DROP RESOURCE 'hive0';
    ```

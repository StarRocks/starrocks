# DROP REPOSITORY

## description

This statement is used to drop an established repository. Only root or superuser can drop the repositories.

Syntax:

```sql
DROP REPOSITORY `repo_name`;
```

Note:

To drop the repository is just to drop the mapping of the repository in StarRocks, and the actual repository data won't be dropped. After deletion, you can map to the repository again by specifying the same broker and LOCATION.

## example

1. Delete the repository named bos_repo:

    ```sql
    DROP REPOSITORY `bos_repo`;
    ```

## keyword

DROP REPOSITORY

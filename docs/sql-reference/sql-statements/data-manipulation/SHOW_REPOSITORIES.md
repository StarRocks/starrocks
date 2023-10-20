# SHOW REPOSITORIES

## description

This statement is used to view the currently created repositories.

Syntax:

```sql
SHOW REPOSITORIES;
```

```plain text
Note：
1.The meanings of each column are as follows:：
RepoId：     unique repositories ID
RepoName：   repositories name
CreateTime： the time when the repository was first created
IsReadOnly： is it a read-only repository
Location：   the root directory in the repository used to back up data
Broker：     the dependent Broker
ErrMsg：     StarRocks will regularly check the connectivity of the repository. If there is a problem, an error message will be displayed here
```

## example

1. View created repositories：

    ```sql
    SHOW REPOSITORIES;
    ```

## keyword

SHOW, REPOSITORY, REPOSITORIES

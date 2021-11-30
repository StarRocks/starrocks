# CREATE CLUSTER

## description

This statement is used to create logical clusters which requires administrator privileges. If multi-tenant functionality is not used, it will create a cluster named "defaul_cluster" directly. Otherwise, clusters should be created with custom names.

Syntax:

```sql
CREATE CLUSTER [IF NOT EXISTS] cluster_name

PROPERTIES ("key"="value", ...)

IDENTIFIED BY 'password'
```

1. PROPERTIES

   Specify attributes of logical clusters

    ```sql
    PROPERTIES ("instance_num" = "3")

    instance_num is the number of nodes in a logical cluster
    ```

2. Every logical cluster contains a superuser whose password must be specified when creating a logical cluster.

## example

1. Create a logical cluster test_cluster with 3 be nodes and specify its superuser password.

    ```sql
    CREATE CLUSTER test_cluster PROPERTIES("instance_num"="3") IDENTIFIED BY 'test';
    ```

2. Create a logical cluster default_cluster with 3 be nodes (multi-tenant is not used) and specify its superuser password.

    ```sql
    CREATE CLUSTER default_cluster PROPERTIES("instance_num"="3") IDENTIFIED BY 'test';
    ```

## keyword

CREATE,CLUSTER

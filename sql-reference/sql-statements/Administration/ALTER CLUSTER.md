# ALTER CLUSTER

## description

This statement is used to update the logical cluster. It requires administrator privileges.

Syntax:

```sql
ALTER CLUSTER cluster_name PROPERTIES ("key"="value", ...);
```

1. Scaling up and scaling down (Based on the current number of "be" existing in the cluster, it scales up when the number is large and scales down when the number is small.) It scales up through synchronous operation and scales down through asynchronous operation. The status of backend shows whether the scaling is completed.

```sql
PROERTIES ("instance_num" = "3")
```

instance_num is the node number of logical clusters.

## example

1. Scale down and reduce the number of "be" in logical cluster test_cluster from 3 to 2.

    ```sql
    ALTER CLUSTER test_cluster PROPERTIES ("instance_num"="2");
    ```

2. Scale up and increase the number of "be" in logical cluster test_cluster from 3 to 4.

    ```sql
    ALTER CLUSTER test_cluster PROPERTIES ("instance_num"="4");
    ```

## keyword

ALTER,CLUSTER

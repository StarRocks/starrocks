# DROP CLUSTER

## description

This statement is used to delete logical clusters. To successfully delete logical clusters, db in those clusters must be deleted first, which requires admin privileges.

Syntax:

```sql
DROP CLUSTER [IF EXISTS] cluster_name
```

## example

Drop logical clusters test_cluster.

```sql
DROP CLUSTER test_cluster;
```

## keyword

DROP,CLUSTER

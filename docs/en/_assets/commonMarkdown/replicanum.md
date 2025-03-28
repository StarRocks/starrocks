---
---
:::note

If a StarRocks cluster in a staging environment contains only one BE, the number of replicas can be set to `1` in the `PROPERTIES` clause, such as `PROPERTIES( "replication_num" = "1" )`. The default number of replicas is 3, which is also the number recommended for production StarRocks clusters. If you want to use the default number, you do not need to configure the `replication_num` parameter.

:::

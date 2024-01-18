
:::note

如果测试环境中集群仅包含一个 BE，可以在 `PROPERTIES` 中将副本数设置为 `1`，即 `PROPERTIES( "replicaton_num" = "1" )`。默认副本数为 `3`，也是生产集群推荐的副本数。如果您需要使用默认设置，也可以不配置 `PROPERTIES`。

:::

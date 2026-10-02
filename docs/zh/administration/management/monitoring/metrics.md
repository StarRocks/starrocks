---
displayed_sidebar: docs
description: "StarRocks 通用监控指标"
sidebar_position: 10
---

# 通用监控指标

:::note

有关物化视图和存算分离集群专属监控指标，请参考对应章节：

- [异步物化视图监控项](./metrics-materialized_view.md)
- [存算分离集群监控项](./metrics-shared-data.md)

关于为您的 StarRocks 集群设置监控报警服务的详细说明，请参阅 [监控警报](./monitoring.md)。

:::

监控指标按字母顺序列于以下文件中：

- [a - c](./metric_details/a-c.md)
- [d - h](./metric_details/d-h.md)
- [i - p](./metric_details/i-p.md)
- [q - r](./metric_details/q-r.md)
- [s](./metric_details/s.md)
- [t - z](./metric_details/t-z.md)

ADBC 查询在现有的 FE Catalog 查询计数器和延迟直方图，以及 BE/CN 扫描字节数和行数计数器上使用 `catalog_type="adbc"` 标签，以区分 ADBC 与内部 Catalog 的查询。请参阅 [ADBC Catalog](../../../data_source/catalog/adbc_catalog.md)。

---
displayed_sidebar: docs
description: "StarRocks metrics for monitoring"
sidebar_position: 10
---

# General Monitoring Metrics

:::note

Metrics for materialized views and shared-data clusters are detailed in the corresponding sections:

- [Metrics for asynchronous materialized view metrics](./metrics-materialized_view.md)
- [Metrics for Shared-data Dashboard metrics, and Starlet Dashboard metrics](./metrics-shared-data.md)

For more information on how to build a monitoring service for your StarRocks cluster, see [Monitor and Alert](./monitoring.md).

:::

Monitoring metrics are listed alphabetically in these files:

- [a - c](./metric_details/a-c.md)
- [d - h](./metric_details/d-h.md)
- [i - p](./metric_details/i-p.md)
- [q - r](./metric_details/q-r.md)
- [s](./metric_details/s.md)
- [t - z](./metric_details/t-z.md)

## Catalog query metrics

FE groups catalog query counts, errors, and latency by the `catalog_type` label. When Lance SQL execution is enabled, Lance scans use `catalog_type="lance"` for `catalog_query_total`, `catalog_query_err`, and `catalog_query_latency_ms`.

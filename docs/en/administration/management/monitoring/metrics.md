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

ADBC queries use `catalog_type="adbc"` on the existing FE per-catalog query counters and latency histograms, and on the BE/CN scan-byte and scan-row counters. This label identifies ADBC traffic separately from the internal catalog. See [ADBC catalog](../../../data_source/catalog/adbc_catalog.md).

---
displayed_sidebar: docs
---

# fe_metrics

`fe_metrics` provides information about the metrics of each FE node.

The following fields are provided in `fe_metrics`:

| **Field** | **Description**                                              |
| --------- | ------------------------------------------------------------ |
| FE_ID     | ID of the FE node.                                           |
| NAME      | Name of the metric.                                          |
| LABELS    | Labels associated with the metric.                           |
| VALUE     | Current value of the metric.                                 |

The following Iceberg delete metrics are exposed through this table:

- `iceberg_delete_total`
- `iceberg_delete_duration_ms_total`
- `iceberg_delete_bytes`
- `iceberg_delete_rows`

---
displayed_sidebar: docs
---

# fe_metrics

`fe_metrics` 提供有关每个 FE 节点指标的信息。

`fe_metrics` 提供以下字段：

| **字段** | **描述**                                         |
| -------- | ------------------------------------------------ |
| FE_ID    | FE 节点的 ID。                                   |
| NAME     | 指标的名称。                                     |
| LABELS   | 与指标关联的标签。                               |
| VALUE    | 指标的当前值。                                   |

以下 Iceberg 删除相关指标可通过该表查询：

- `iceberg_delete_total`
- `iceberg_delete_duration_ms_total`
- `iceberg_delete_bytes`
- `iceberg_delete_rows`

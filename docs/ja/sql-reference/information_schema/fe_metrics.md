---
displayed_sidebar: docs
---

# fe_metrics

`fe_metrics` は各 FE ノードのメトリックに関する情報を提供します。

`fe_metrics` には以下のフィールドが提供されています:

| **フィールド** | **説明**                                         |
| -------- | ------------------------------------------------ |
| FE_ID    | FE ノードの ID。                                   |
| NAME     | メトリックの名前。                               |
| LABELS   | メトリックに関連付けられたラベル。               |
| VALUE    | メトリックの現在の値。                           |

以下の Iceberg 削除関連メトリクスがこのテーブルで確認できます:

- `iceberg_delete_total`
- `iceberg_delete_duration_ms_total`
- `iceberg_delete_bytes`
- `iceberg_delete_rows`

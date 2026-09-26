---
displayed_sidebar: docs
description: "StarRocks の一般的なモニタリングメトリクス"
sidebar_position: 10
---

# 一般的なモニタリングメトリクス

:::note

マテリアライズドビューや共有データクラスタに特化したメトリクスについては、該当するセクションを参照してください：

- [非同期マテリアライズドビューのメトリクス](./metrics-materialized_view.md)
- [共有データダッシュボードメトリクス、および Starlet ダッシュボードメトリクス](./metrics-shared-data.md)

StarRocks クラスタのモニタリングサービスの構築方法については、[モニタリングとアラート](./monitoring.md)を参照してください。

:::

モニタリングメトリクスはアルファベット順に以下のファイルに一覧表示されています：

- [a - c](./metric_details/a-c.md)
- [d - h](./metric_details/d-h.md)
- [i - p](./metric_details/i-p.md)
- [q - r](./metric_details/q-r.md)
- [s](./metric_details/s.md)
- [t - z](./metric_details/t-z.md)

ADBC クエリでは、既存の FE の catalog 別クエリカウンターとレイテンシーヒストグラム、および BE/CN のスキャンバイト数と行数のカウンターに `catalog_type="adbc"` ラベルを使用します。このラベルにより、ADBC と内部 catalog のクエリを区別できます。[ADBC catalog](../../../data_source/catalog/adbc_catalog.md)を参照してください。

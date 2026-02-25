---
displayed_sidebar: docs
---

# SSB フラットテーブルベンチマーク

スタースキーマベンチマーク (SSB) は、OLAP データベース製品の基本的なパフォーマンス指標をテストするために設計されています。SSB は、学術界や産業界で広く適用されているスタースキーマのテストセットを使用します。詳細については、 [Star Schema Benchmark](https://www.cs.umb.edu/~poneil/StarSchemaB.PDF) を参照してください。

ClickHouse はスタースキーマを広いフラットテーブルに変換し、SSB を単一テーブルのベンチマークに書き換えます。詳細については、 [Star schema benchmark of ClickHouse](https://clickhouse.com/docs/en/getting-started/example-datasets/star-schema) を参照してください。

このテストは、StarRocks、Apache Druid、ClickHouse のパフォーマンスを SSB 単一テーブルデータセットに対して比較します。

## テスト結論

このテストは、共有なし StarRocks クラスターの OLAP テーブルで、ClickHouse および Apache Druid と共に、同じデータセットに対して実行されました。

100 GB の SSB-Flat データセットに対して実行された 13 のクエリの結果に基づくと、StarRocks は **ClickHouse の 1.87 倍、Apache Druid の 4.75 倍** のクエリパフォーマンスを持っています。結果の単位はミリ秒です。

![SSB-SR](../_assets/benchmark/SSB-SR.png)

## テスト準備

### ハードウェア

StarRocks、Apache Druid、ClickHouse は、同じ構成のホストにデプロイされています - [AWS m7i.4xlarge](https://aws.amazon.com/ec2/instance-types/m7i/?nc1=h_ls)。

|                          | **Spec**    |
| ------------------------ | ----------- |
| インスタンス数           | 5           |
| vCPU                     | 16          |
| メモリ (GiB)             | 64          |
| ネットワーク帯域幅 (Gbps) | 最大 12.5   |
| EBS 帯域幅 (Gbps)        | 最大 10     |

### ソフトウェア

|                   | **StarRocks**     | **ClickHouse** | **Apache Druid**                                             |
| ----------------- | ----------------- | -------------- | ------------------------------------------------------------ |
| **クラスターサイズ** | One FE, Three BEs | Three nodes    | One Master Server, one Query Servers, and three Data Servers |
| **バージョン**     | 3.5.0             | 25.3.3.42      | 33.0.0                                                       |
| **リリース日**     | 2025.6.13         | 2025.4.22      | 2025.4.29                                                    |
| **構成**           | Default           | Default        | Default                                                      |

## テスト結果

以下の表は、13 のクエリに対するパフォーマンステストの結果を示しています。クエリの遅延の単位はミリ秒です。すべてのクエリは 1 回ウォームアップされ、その後 3 回実行して平均値を結果として取得します。表のヘッダーの `ClickHouse vs StarRocks` および `Druid vs StarRocks` は、ClickHouse/Druid のクエリ応答時間を StarRocks のクエリ応答時間で割った値を意味します。値が大きいほど、StarRocks のパフォーマンスが優れていることを示します。

| クエリ | StarRocks | ClickHouse | Druid | ClickHouse vs StarRocks | Druid vs StarRocks |
| ----- | --------- | ---------- | ----- | ----------------------- | ------------------ |
| SUM   | 992       | 1858       | 4710  | 1.87                    | 4.75               |
| Q01   | 30        | 49         | 330   | 1.63                    | 11.00              |
| Q02   | 16        | 31         | 260   | 1.94                    | 16.25              |
| Q03   | 26        | 29         | 250   | 1.12                    | 9.62               |
| Q04   | 143       | 197        | 420   | 1.38                    | 2.94               |
| Q05   | 120       | 179        | 440   | 1.49                    | 3.67               |
| Q06   | 63        | 158        | 320   | 2.51                    | 5.08               |
| Q07   | 133       | 249        | 510   | 1.87                    | 3.83               |
| Q08   | 90        | 197        | 380   | 2.19                    | 4.22               |
| Q09   | 86        | 150        | 350   | 1.74                    | 4.07               |
| Q10   | 20        | 33         | 250   | 1.65                    | 12.50              |
| Q11   | 156       | 340        | 550   | 2.18                    | 3.53               |
| Q12   | 66        | 133        | 330   | 2.02                    | 5.00               |
| Q13   | 43        | 113        | 320   | 2.63                    | 7.44               |
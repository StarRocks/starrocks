---
displayed_sidebar: docs
sidebar_position: 90
---

# Sorted streaming aggregate

データベースシステムにおける一般的な集計方法には、ハッシュ集計とソート集計があります。

バージョン 2.5 以降、StarRocks は **sorted streaming aggregate** をサポートしています。

## 動作原理

集計ノード (AGG) は主に GROUP BY と集計関数を処理する役割を担っています。

Sorted streaming aggregate は、ハッシュテーブルを作成することなく、キーの順序に従って GROUP BY キーを比較することでデータをグループ化します。これにより、集計に消費されるメモリリソースが効果的に削減されます。高い集計カーディナリティを持つクエリに対して、sorted streaming aggregate は集計パフォーマンスを向上させ、メモリ使用量を削減します。

次の変数を設定することで、sorted streaming aggregate を有効にできます。

```SQL
set enable_sort_aggregate=true;
```

## 制限事項

- StarRocks 共有データクラスタは、sorted streaming aggregate をサポートしていません。
- GROUP BY のキーには順序が必要です。例えば、ソートキーが `k1, k2, k3` の場合:
  - `GROUP BY k1` および `GROUP BY k1, k2` は許可されます。
  - `GROUP BY k1, k3` はソートキーの順序に従っていません。したがって、このような句には sorted streaming aggregate は適用されません。
- 選択されたパーティションは単一のパーティションでなければなりません（同じキーが異なるパーティションの異なるマシンに分散される可能性があるため）。
- GROUP BY のキーは、テーブル作成時に指定されたバケットキーと同じ分布でなければなりません。例えば、テーブルに `k1, k2, k3` の3つの列がある場合、バケットキーは `k1` または `k1, k2` になり得ます。
  - バケットキーが `k1` の場合、`GROUP BY` キーは `k1`, `k1, k2`, または `k1, k2, k3` になり得ます。
  - バケットキーが `k1, k2` の場合、GROUP BY キーは `k1, k2` または `k1, k2, k3` になり得ます。
  - クエリプランがこの要件を満たさない場合、この機能が有効になっていても、sorted streaming aggregate 機能は適用されません。
- Sorted streaming aggregate は、最初の段階の集計（つまり、AGG ノードの下に Scan ノードが1つだけある場合）にのみ機能します。

## 例

1. テーブルを作成し、データを挿入します。

    ```SQL
    CREATE TABLE `test_sorted_streaming_agg_basic`
    (
        `id_int` int(11) NOT NULL COMMENT "",
        `id_string` varchar(100) NOT NULL COMMENT ""
    ) 
    ENGINE=OLAP 
    DUPLICATE KEY(`id_int`)COMMENT "OLAP"
    DISTRIBUTED BY HASH(`id_int`)
    PROPERTIES
    ("replication_num" = "3"); 

    INSERT INTO test_sorted_streaming_agg_basic VALUES
    (1, 'v1'),
    (2, 'v2'),
    (3, 'v3'),
    (1, 'v4');
    ```

2. Sorted streaming aggregate を有効にし、EXPLAIN を使用して SQL プロファイルをクエリします。

    ```SQL
    set enable_sort_aggregate = true;

    explain costs select id_int, max(id_string)
    from test_sorted_streaming_agg_basic
    group by id_int;
    ```

## Sorted streaming aggregate が有効かどうかを確認する

`EXPLAIN costs` の結果を確認します。AGG ノードの `sorted streaming` フィールドが `true` であれば、この機能は有効です。

```Plain
|                                                                                                                                    |
|   1:AGGREGATE (update finalize)                                                                                                    |
|   |  aggregate: max[([2: id_string, VARCHAR, false]); args: VARCHAR; result: VARCHAR; args nullable: false; result nullable: true] |
|   |  group by: [1: id_int, INT, false]                                                                                             |
|   |  sorted streaming: true                                                                                                        |
|   |  cardinality: 1                                                                                                                |
|   |  column statistics:                                                                                                            |
|   |  * id_int-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN                                                                       |
|   |  * max-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN                                                                          |
|   |                                                                                                                                |
|   0:OlapScanNode                                                                                                                   |
|      table: test_sorted_streaming_agg_basic, rollup: test_sorted_streaming_agg_basic                                               |
|      preAggregation: on                                                                                                            |
|      partitionsRatio=1/1, tabletsRatio=10/10                                                                                       |
|      tabletList=30672,30674,30676,30678,30680,30682,30684,30686,30688,30690                                                        |
|      actualRows=0, avgRowSize=2.0                                                                                                  |
|      cardinality: 1                                                                                                                |
|      column statistics:                                                                                                            |
|      * id_int-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN                                                                       |
|      * id_string-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN                                                                    |
```
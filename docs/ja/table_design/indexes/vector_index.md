---
displayed_sidebar: docs
sidebar_position: 60
---

# ベクターインデックス

このトピックでは、StarRocks のベクターインデックス機能と、それを使用した近似最近傍探索 (ANNS) の方法について紹介します。

ベクターインデックス機能は、v3.4 以降の共有なしクラスタでのみサポートされています。

## 概要

現在、StarRocks はセグメントファイルレベルでのベクターインデックスをサポートしています。このインデックスは、各検索項目をセグメントファイル内の行 ID にマッピングし、ベクトル距離計算を行わずに対応するデータ行を直接特定することで、迅速なデータ取得を可能にします。システムは現在、Inverted File with Product Quantization (IVFPQ) と Hierarchical Navigable Small World (HNSW) の2種類のベクターインデックスを提供しており、それぞれ独自の組織構造を持っています。

### Inverted File with Product Quantization (IVFPQ)

Inverted File with Product Quantization (IVFPQ) は、大規模な高次元ベクトルにおける近似最近傍探索の手法であり、ディープラーニングや機械学習におけるベクトル検索タスクで一般的に使用されます。IVFPQ は、インバーテッドファイルとプロダクト量子化の2つの主要なコンポーネントで構成されています。

- **インバーテッドファイル**: このコンポーネントはインデックス作成手法です。データセットを複数のクラスタ（またはボロノイセル）に分割し、各クラスタにセントロイド（シードポイント）を持たせ、各データポイント（ベクトル）を最も近いクラスタ中心に割り当てます。ベクトル検索では、IVFPQ は最も近いクラスタのみを検索するため、検索範囲と複雑さを大幅に削減します。
- **プロダクト量子化**: このコンポーネントはデータ圧縮技術です。高次元ベクトルをサブベクトルに分割し、各サブベクトルを量子化して、事前定義されたセットの最も近いポイントにマッピングします。これにより、ストレージと計算コストを削減しながら、高精度を維持します。

インバーテッドファイルとプロダクト量子化を組み合わせることで、IVFPQ は大規模な高次元データセットにおける効率的な近似最近傍探索を可能にします。

### Hierarchical Navigable Small World (HNSW)

Hierarchical Navigable Small World (HNSW) は、高次元最近傍探索のためのグラフベースのアルゴリズムであり、ベクトル検索タスクで広く使用されています。

HNSW は、各レイヤーがナビゲーブルスモールワールド (NSW) グラフである階層的なグラフ構造を構築します。グラフ内では、各頂点がデータポイントを表し、エッジは頂点間の類似性を示します。グラフの上位レイヤーは、迅速なグローバル検索のために少ない頂点と疎な接続を持ち、下位レイヤーはすべての頂点と密な接続を持ち、正確なローカル検索を行います。

ベクトル検索では、HNSW は最初に最上位レイヤーを検索し、近似最近傍領域を迅速に特定し、次にレイヤーを下に移動して、最下位レイヤーで正確な最近傍を見つけます。

HNSW は効率性と精度の両方を提供し、さまざまなデータとクエリ分布に適応可能です。

### IVFPQ と HNSW の比較

- **データ圧縮率**: IVFPQ はより高い圧縮率（約 1:0.15）を持ちます。インデックス計算は、PQ がベクトルを圧縮するため、粗いランク付けの後に予備的なソート結果を提供するだけです。最終的なソート結果には追加の詳細なランク付けが必要であり、これにより計算と遅延が増加します。HNSW はより低い圧縮率（約 1:0.8）を持ち、追加の処理なしで正確なランク付けを提供し、計算コストと遅延が低く、ストレージコストが高くなります。
- **リコール調整**: 両方のインデックスは、パラメータ調整を通じてリコール率の調整をサポートしますが、IVFPQ は同様のリコール率でより高い計算コストがかかります。
- **キャッシング戦略**: IVFPQ は、インデックスブロックのキャッシュ比率を調整することでメモリコストと計算遅延のバランスを取ることができますが、HNSW は現在、フルファイルキャッシングのみをサポートしています。

## 使用方法

各テーブルは1つのベクターインデックスのみをサポートします。

### 前提条件

ベクターインデックスを作成する前に、FE の設定項目 `enable_experimental_vector` を `true` に設定して有効にする必要があります。

次のステートメントを実行して動的に有効にします。

```SQL
ADMIN SET FRONTEND CONFIG ("enable_experimental_vector" = "true");
```

永続的に有効にするには、FE 設定ファイル `fe.conf` に `enable_experimental_vector = true` を追加し、FE を再起動する必要があります。

### ベクターインデックスの作成

このチュートリアルでは、テーブルを作成する際にベクターインデックスを作成します。既存のテーブルにベクターインデックスを追加することもできます。詳細な手順については、[Append vector index](#append-vector-index) を参照してください。

- 次の例では、テーブル `hnsw` のカラム `vector` に HNSW ベクターインデックス `hnsw_vector` を作成します。

    ```SQL
    CREATE TABLE hnsw (
        id     BIGINT(20)   NOT NULL COMMENT "",
        vector ARRAY<FLOAT> NOT NULL COMMENT "",
        INDEX hnsw_vector (vector) USING VECTOR (
            "index_type" = "hnsw", 
            "dim"="5", 
            "metric_type" = "l2_distance", 
            "is_vector_normed" = "false", 
            "M" = "16", 
            "efconstruction" = "40"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1;
    ```
- 次の例では、テーブル `ivfpq` のカラム `vector` に IVFPQ ベクターインデックス `ivfpq_vector` を作成します。

    ```SQL
    CREATE TABLE ivfpq (
        id     BIGINT(20)   NOT NULL COMMENT "",
        vector ARRAY<FLOAT> NOT NULL COMMENT "",
        INDEX ivfpq_vector (vector) USING VECTOR (
            "index_type" = "ivfpq", 
            "dim"="5", 
            "metric_type" = "l2_distance", 
            "is_vector_normed" = "false", 
            "nbits" = "16", 
            "nlist" = "40"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1;
    ```

#### インデックス構築パラメータ

##### USING VECTOR

- **デフォルト**: N/A
- **必須**: はい
- **説明**: ベクターインデックスを作成します。

##### index_type

- **デフォルト**: N/A
- **必須**: はい
- **説明**: ベクターインデックスタイプ。有効な値: `hnsw` と `ivfpq`。

##### dim

- **デフォルト**: N/A
- **必須**: はい
- **説明**: インデックスの次元。インデックスが構築された後、次元要件を満たさないベクトルはベースカラムへのロードが拒否されます。`1` 以上の整数である必要があります。

##### metric_type

- **デフォルト**: N/A
- **必須**: はい
- **説明**: ベクターインデックスのメトリックタイプ（測定関数）。有効な値:
  - `l2_distance`: ユークリッド距離。値が小さいほど、類似性が高い。
  - `cosine_similarity`: コサイン類似度。値が大きいほど、類似性が高い。

##### is_vector_normed

- **デフォルト**: false
- **必須**: いいえ
- **説明**: ベクトルが正規化されているかどうか。有効な値は `true` と `false` です。`metric_type` が `cosine_similarity` の場合にのみ有効です。ベクトルが正規化されている場合、計算された距離の値は [-1, 1] の範囲内になります。ベクトルは、平方和が `1` であることを満たす必要があります。そうでない場合、エラーが返されます。

##### M

- **デフォルト**: 16
- **必須**: いいえ
- **説明**: HNSW 固有のパラメータ。グラフ構築中に新しい要素ごとに作成される双方向接続の数。`2` 以上の整数である必要があります。`M` の値は、グラフ構築と検索の効率と精度に直接影響します。グラフ構築中、各頂点は最も近い `M` 個の頂点との接続を確立しようとします。頂点がすでに `M` 個の接続を持っているが、より近い頂点を見つけた場合、最も遠い接続が削除され、より近い頂点との新しい接続が確立されます。ベクトル検索は、エントリーポイントから開始し、それに接続された頂点に沿って最も近い隣接点を見つけます。したがって、`M` の値が大きいほど、各頂点の検索範囲が広がり、検索効率が向上しますが、グラフ構築とストレージのコストも増加します。

##### efconstruction

- **デフォルト**: 40
- **必須**: いいえ
- **説明**: HNSW 固有のパラメータ。最も近い隣接点を含む候補リストのサイズ。`1` 以上の整数である必要があります。グラフ構築プロセス中の検索深度を制御するために使用されます。具体的には、`efconstruction` は、グラフ構築プロセス中の各頂点の検索リスト（候補リストとも呼ばれる）のサイズを定義します。この候補リストは、現在の頂点の隣接候補を格納するために使用され、リストのサイズは `efconstruction` です。`efconstruction` の値が大きいほど、グラフ構築プロセス中に頂点の隣接候補として考慮される候補が増え、その結果、グラフの品質（接続性の向上など）が向上しますが、グラフ構築の時間消費と計算の複雑さも増加します。

##### nbits

- **デフォルト**: 16
- **必須**: いいえ
- **説明**: IVFPQ 固有のパラメータ。プロダクト量子化 (PQ) の精度。`8` の倍数である必要があります。IVFPQ では、各ベクトルが複数のサブベクトルに分割され、各サブベクトルが量子化されます。`Nbits` は量子化の精度を定義し、各サブベクトルが何ビットに量子化されるかを示します。`nbits` の値が大きいほど、量子化精度が高くなりますが、ストレージと計算コストも増加します。

##### nlist

- **デフォルト**: 16
- **必須**: いいえ
- **説明**: IVFPQ 固有のパラメータ。クラスタの数、またはインバーテッドリストの数。`1` 以上の整数である必要があります。IVFPQ では、データセットがクラスタに分割され、各クラスタのセントロイドがインバーテッドリストに対応します。ベクトル検索は、最初にデータポイントに最も近いクラスタのセントロイドを見つけ、次に対応するインバーテッドリスト内で最も近い隣接点を取得します。したがって、`nlist` の値は検索の精度と効率に影響を与えます。`nlist` の値が大きいほど、クラスタリングの粒度が細かくなり、検索精度が向上しますが、検索の複雑さも増加します。

##### M_IVFPQ

- **必須**: はい
- **説明**: IVFPQ 固有のパラメータ。元のベクトルが分割されるサブベクトルの数。IVFPQ インデックスは、`dim` 次元のベクトルを `M_IVFPQ` 個の等長のサブベクトルに分割します。したがって、`dim` の値の因数である必要があります。

#### ベクターインデックスの追加

既存のテーブルにベクターインデックスを追加するには、[CREATE INDEX](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_INDEX.md) または [ALTER TABLE ADD INDEX](../../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md) を使用します。

例:

```SQL
CREATE INDEX ivfpq_vector 
ON ivfpq (vector) 
USING VECTOR (
    "index_type" = "ivfpq",
    "metric_type" = "l2_distance", 
    "is_vector_normed" = "false",  
    "dim"="5", 
    "nlist" = "256", 
    "nbits"="10"
);

ALTER TABLE ivfpq 
ADD INDEX ivfpq_vector (vector) 
USING VECTOR (
    "index_type" = "ivfpq",
    "metric_type" = "l2_distance", 
    "is_vector_normed" = "false", 
    "dim"="5", 
    "nlist" = "256", 
    "nbits"="10"
);
```

### ベクターインデックスの管理

#### ベクターインデックスの表示

[SHOW CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_CREATE_TABLE.md) ステートメントを使用して、ベクターインデックスの定義を表示できます。

例:

```SQL
mysql> SHOW CREATE TABLE hnsw \G
*************************** 1. row ***************************
       Table: hnsw
Create Table: CREATE TABLE hnsw (
  id bigint(20) NOT NULL COMMENT "",
  vector array<float> NOT NULL COMMENT "",
  INDEX index_vector (vector) USING VECTOR("dim" = "5", "efconstruction" = "40", "index_type" = "hnsw", "is_vector_normed" = "false", "M" = "512", "metric_type" = "l2_distance") COMMENT ''
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "false",
"replication_num" = "3"
);
1 row in set (0.00 sec)
```

#### ベクターインデックスの削除

[DROP INDEX](../../sql-reference/sql-statements/table_bucket_part_index/DROP_INDEX.md) または [ALTER TABLE DROP INDEX](../../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md) を使用してベクターインデックスを削除できます。

```SQL
DROP INDEX ivfpq_vector ON ivfpq;
ALTER TABLE ivfpq DROP INDEX ivfpq_vector;
```

### ベクターインデックスを使用した ANNS の実行

ベクトル検索を実行する前に、FE の設定項目 `enable_experimental_vector` が `true` に設定されていることを確認してください。

#### ベクターインデックスベースのクエリの要件

```SQL
SELECT *, <vector_index_distance_func>(v1, [1,2,3]) as dis
FROM table_name
WHERE <vector_index_distance_func>(v1, [1,2,3]) <= 10
ORDER BY <vector_index_distance_func>(v1, [1,2,3]) 
LIMIT 10
```

ベクターインデックスをクエリで使用するには、次のすべての要件を満たす必要があります。

- **ORDER BY の要件:**
  - ORDER BY 句の形式: `ORDER BY <vector_index_distance_func>(vector_column, constant_array)` の形式でなければならず、追加の ORDER BY カラムを含めてはなりません。
    - `<vector_index_distance_func>` の関数名要件:
      - `metric_type` が `l2_distance` の場合、関数名は `approx_l2_distance` でなければなりません。
      - `metric_type` が `cosine_similarity` の場合、関数名は `approx_cosine_similarity` でなければなりません。
    - `<vector_index_distance_func>` のパラメータ要件:
      - `constant_array` のカラムの1つは、ベクターインデックス `dim` と一致する次元を持つ定数 `ARRAY<FLOAT>` でなければなりません。
      - もう1つのカラム `vector_column` は、ベクターインデックスに対応するカラムでなければなりません。
  - ORDER の方向要件:
    - `metric_type` が `l2_distance` の場合、順序は `ASC` でなければなりません。
    - `metric_type` が `cosine_similarity` の場合、順序は `DESC` でなければなりません。
  - `LIMIT N` 句が必要です。
- **述語の要件:**
  - すべての述語は `<vector_index_distance_func>` 式でなければならず、`AND` および比較演算子（`>` または `<`）を使用して結合されます。比較演算子の方向は `ASC`/`DESC` の順序と一致していなければなりません。具体的には:
  - 要件 1:
    - `metric_type` が `l2_distance` の場合: `col_ref <= constant`。
    - `metric_type` が `cosine_similarity` の場合: `col_ref >= constant`。
    - ここで、`col_ref` は `<vector_index_distance_func>(vector_column, constant_array)` の結果を指し、`FLOAT` または `DOUBLE` 型にキャストできます。例えば:
      - `approx_l2_distance(v1, [1,2,3])`
      - `CAST(approx_l2_distance(v1, [1,2,3]) AS FLOAT)`
      - `CAST(approx_l2_distance(v1, [1,2,3]) AS DOUBLE)`
  - 要件 2:
    - 述語は `AND` を使用し、各子述語が要件 1 を満たしている必要があります。

#### 準備

ベクターインデックスを持つテーブルを作成し、ベクターデータを挿入します。

```SQL
CREATE TABLE test_hnsw (
    id     BIGINT(20)   NOT NULL COMMENT "",
    vector ARRAY<FLOAT> NOT NULL COMMENT "",
    INDEX index_vector (vector) USING VECTOR (
        "index_type" = "hnsw",
        "metric_type" = "l2_distance", 
        "is_vector_normed" = "false", 
        "M" = "512", 
        "dim"="5")
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1;

INSERT INTO t_test_vector_table VALUES
    (1, [1,2,3,4,5]),
    (2, [4,5,6,7,8]);
    
CREATE TABLE test_ivfpq (
    id     BIGINT(20)   NOT NULL COMMENT "",
    vector ARRAY<FLOAT> NOT NULL COMMENT "",
    INDEX index_vector (vector) USING VECTOR (
        "index_type" = "hnsw",
        "metric_type" = "l2_distance", 
        "is_vector_normed" = "false", 
        "nlist" = "256", 
        "nbits"="10")
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1;

INSERT INTO test_ivfpq VALUES
    (1, [1,2,3,4,5]),
    (2, [4,5,6,7,8]);
```

#### ベクトル検索の実行

##### 近似検索

近似検索はベクターインデックスにヒットし、検索プロセスを加速します。

次の例では、ベクトル `[1,1,1,1,1]` の最も近い近似最近傍を1つ検索します。

```SQL
SELECT id, approx_l2_distance([1,1,1,1,1], vector) 
FROM test_hnsw 
ORDER BY approx_l2_distance([1,1,1,1,1], vector) 
LIMIT 1;
```

##### スカラーとベクトルの結合検索

スカラー検索とベクトル検索を組み合わせることができます。

```SQL
SELECT id, approx_l2_distance([1,1,1,1,1], vector) 
FROM test_hnsw 
WHERE id = 1 
ORDER BY approx_l2_distance([1,1,1,1,1], vector) 
LIMIT 1;
```

##### 範囲検索

ベクトルデータに対して範囲検索を行うことができます。

次の例では、`score < 40` の条件をインデックスにプッシュダウンし、`score` の範囲でベクトルをフィルタリングします。

```SQL
SELECT * FROM (
    SELECT id, approx_l2_distance([1,1,1,1,1], vector) score 
    FROM test_hnsw
) a 
WHERE score < 40 
ORDER BY score 
LIMIT 1;
```

##### 正確な計算

正確な計算はベクターインデックスを無視し、ベクトル間の距離を直接計算して正確な結果を得ます。

```SQL
SELECT id, l2_distance([1,1,1,1,1], vector) 
FROM test_hnsw WHERE id = 1 
ORDER BY l2_distance([1,1,1,1,1], vector) 
LIMIT 1;
```

> **注意**
>
> 異なる距離メトリック関数は「類似性」を異なる方法で定義します。`l2_distance` では、値が小さいほど類似性が高く、`cosine_similarity` では、値が大きいほど類似性が高いです。したがって、`topN` を計算する際には、メトリックの類似性方向に一致するようにソート（ORDER BY）の方向を設定してください。`l2_distance` には `ORDER BY ASC LIMIT x` を使用し、`cosine_similarity` には `ORDER BY DESC LIMIT x` を使用します。

#### 検索のためのインデックスパラメータの微調整

パラメータの調整はベクトル検索において重要であり、パフォーマンスと精度の両方に影響を与えます。小さなデータセットで検索パラメータを調整し、期待されるリコールと遅延が達成された場合にのみ大規模なデータセットに移行することをお勧めします。

検索パラメータは、SQL ステートメント内のヒントを通じて渡されます。

##### HNSW インデックスの場合

進む前に、ベクトルカラムが HNSW インデックスで構築されていることを確認してください。

```SQL
SELECT 
    /*+ SET_VAR (ann_params='{efsearch=256}') */ 
    id, approx_l2_distance([1,1,1,1,1], vector) 
FROM test_hnsw 
WHERE id = 1 
ORDER BY approx_l2_distance([1,1,1,1,1], vector) 
LIMIT 1;
```

**パラメータ**:

###### efsearch

- **デフォルト**: 16
- **必須**: いいえ
- **説明**: 精度と速度のトレードオフを制御するパラメータ。階層的なグラフ構造の検索中に、このパラメータは検索中の候補リストのサイズを制御します。`efsearch` の値が大きいほど、精度が高くなりますが、速度が低下します。

##### IVFPQ インデックスの場合

進む前に、ベクトルカラムが IVFPQ インデックスで構築されていることを確認してください。

```SQL
SELECT 
    /*+ SET_VAR (ann_params='{
        nprobe=256,
        max_codes=0,
        scan_table_threshold=0,
        polysemous_ht=0,
        range_search_confidence=0.1
    }') */ 
    id, approx_l2_distance([1,1,1,1,1], vector) 
FROM test_ivfpq 
WHERE id = 1 
ORDER BY approx_l2_distance([1,1,1,1,1], vector) 
LIMIT 1;
```

**パラメータ**:

###### nprobe

- **デフォルト**: 1
- **必須**: いいえ
- **説明**: 検索中にチェックされるインバーテッドリストの数。`nprobe` の値が大きいほど、精度が高くなりますが、速度が低下します。

###### max_codes

- **デフォルト**: 0
- **必須**: いいえ
- **説明**: インバーテッドリストごとにチェックされる最大コード数。このパラメータも精度と速度に影響を与えます。

###### scan_table_threshold

- **デフォルト**: 0
- **必須**: いいえ
- **説明**: 多義ハッシュを制御するパラメータ。要素のハッシュと検索されるベクトルのハッシュ間のハミング距離がこのしきい値を下回る場合、その要素は候補リストに追加されます。

###### polysemous_ht

- **デフォルト**: 0
- **必須**: いいえ
- **説明**: 多義ハッシュを制御するパラメータ。要素のハッシュと検索されるベクトルのハッシュ間のハミング距離がこのしきい値を下回る場合、その要素は直接結果に追加されます。

###### range_search_confidence

- **デフォルト**: 0.1
- **必須**: いいえ
- **説明**: 近似範囲検索の信頼度。値の範囲: [0, 1]。`1` に設定すると、最も正確な結果が得られます。

#### 近似リコールの計算

近似リコールは、ブルートフォース検索からの `topK` 要素と近似検索からの要素を交差させることで計算できます: `Recall = TP / (TP + FN)`。

```SQL
-- 近似検索
SELECT id 
FROM test_hnsw 
ORDER BY approx_l2_distance([1,1,1,1,1], vector)
LIMIT 5;
8
9
7
5
1

-- ブルートフォース検索
SELECT id 
FROM test_hnsw
ORDER BY l2_distance([1,1,1,1,1], vector)
LIMIT 5;
8
9
5
7
10
```

上記の例では、近似検索は 8, 9, 7, 5 を返します。しかし、正しい結果は 8, 9, 5, 7, 10 です。この場合、リコールは 4/5=80% です。

#### ベクターインデックスが有効かどうかの確認

クエリステートメントに対して [EXPLAIN](../../sql-reference/sql-statements/cluster-management/plan_profile/EXPLAIN.md) を実行します。`OlapScanNode` プロパティが `VECTORINDEX: ON` を示している場合、ベクターインデックスが近似ベクトル検索に適用されていることを示します。

例:

```SQL
> EXPLAIN SELECT id FROM t_test_vector_table ORDER BY approx_l2_distance([1,1,1,1,1], vector) LIMIT 5;

+-----------------------------------------------------------------------------------------------------------------------------------------------------+
| Explain String                                                                                                                                      |
+-----------------------------------------------------------------------------------------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                                                                                                     |
|  OUTPUT EXPRS:1: id                                                                                                                                 |
|   PARTITION: UNPARTITIONED                                                                                                                          |
|                                                                                                                                                     |
|   RESULT SINK                                                                                                                                       |
|                                                                                                                                                     |
|   4:Project                                                                                                                                         |
|   |  <slot 1> : 1: id                                                                                                                               |
|   |  limit: 5                                                                                                                                       |
|   |                                                                                                                                                 |
|   3:MERGING-EXCHANGE                                                                                                                                |
|      limit: 5                                                                                                                                       |
|                                                                                                                                                     |
| PLAN FRAGMENT 1                                                                                                                                     |
|  OUTPUT EXPRS:                                                                                                                                      |
|   PARTITION: RANDOM                                                                                                                                 |
|                                                                                                                                                     |
|   STREAM DATA SINK                                                                                                                                  |
|     EXCHANGE ID: 03                                                                                                                                 |
|     UNPARTITIONED                                                                                                                                   |
|                                                                                                                                                     |
|   2:TOP-N                                                                                                                                           |
|   |  order by: <slot 3> 3: approx_l2_distance ASC                                                                                                   |
|   |  offset: 0                                                                                                                                      |
|   |  limit: 5                                                                                                                                       |
|   |                                                                                                                                                 |
|   1:Project                                                                                                                                         |
|   |  <slot 1> : 1: id                                                                                                                               |
|   |  <slot 3> : 4: __vector_approx_l2_distance                                                                                                      |
|   |                                                                                                                                                 |
|   0:OlapScanNode                                                                                                                                    |
|      TABLE: t_test_vector_table                                                                                                                     |
|      VECTORINDEX: ON                                                                                                                                |
|           IVFPQ: OFF, Distance Column: <4:__vector_approx_l2_distance>, LimitK: 5, Order: ASC, Query Vector: [1, 1, 1, 1, 1], Predicate Range: -1.0 |
|      PREAGGREGATION: ON                                                                                                                             |
|      partitions=1/1                                                                                                                                 |
|      rollup: t_test_vector_table                                                                                                                    |
|      tabletRatio=1/1                                                                                                                                |
|      tabletList=11302                                                                                                                               |
|      cardinality=2                                                                                                                                  |
|      avgRowSize=4.0                                                                                                                                 |
+-----------------------------------------------------------------------------------------------------------------------------------------------------+
```

## 制限事項

- 各テーブルは1つのベクターインデックスのみをサポートします。
- 単一のクエリステートメントで複数のベクターインデックスを検索することはサポートされていません。
- ベクトル検索はサブクエリやジョインとして使用することはできません。
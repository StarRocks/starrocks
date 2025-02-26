---
displayed_sidebar: docs
sidebar_position: 100
---

# AUTO INCREMENT とグローバル辞書を使用した COUNT(DISTINCT) とジョインの高速化

このトピックでは、AUTO INCREMENT 列とグローバル辞書を使用して、COUNT(DISTINCT) 計算とジョインを高速化する方法について説明します。

## ユースケース

- **シナリオ 1**: 大量のデータ（小売や配送の注文など）に対して正確な重複排除を行う必要があるとします。しかし、重複排除のための列が STRING 型であるため、カウント中にパフォーマンスが最適でない可能性があります。例えば、`orders` テーブルの `order_uuid` 列は、注文 ID を表す STRING 型で、通常 32 から 36 バイトのサイズで、`UUID()` や類似の関数によって生成されます。この場合、STRING 列 `order_uuid` に対する COUNT(DISTINCT) クエリ `SELECT count(DISTINCT order_uuid) FROM orders WHERE create_date >= CURDATE();` は、満足のいくパフォーマンスを提供しない可能性があります。正確な重複排除のために INTEGER 列を使用することで、パフォーマンスが大幅に向上します。

- **シナリオ 2**: [ビットマップ関数を使用して多次元分析における正確な重複排除を高速化したい](distinct_values/Using_bitmap.md) とします。`bitmap_count()` 関数は INTEGER 入力を必要としますが、重複排除する列が STRING 型の場合、`bitmap_hash()` 関数を使用する必要があります。これにより、重複排除のカウントが若干低くなる可能性があり、クエリパフォーマンスが低下し、ストレージ要件が増加する可能性があります。これは、bitmap_hash() によって生成される INTEGER 値が、順次割り当てられる INTEGER 値と比較してより分散しているためです。

- **シナリオ 3**: 注文の発注と支払いの間の短い時間間隔での注文数をクエリする必要があるとします。この場合、注文の発注と支払いの時間が異なるビジネスチームによって管理される異なるテーブルに保存されている可能性があります。これらのテーブルを注文 ID に基づいてジョインし、注文を重複排除する必要があるかもしれません。例えば:

  ```SQL
  SELECT count(distinct order_uuid)
  FROM orders_t1 as t1 JOIN orders_t2 as t2
      ON t1.order_uuid = t2.order_uuid
  WHERE t2.payment_time - t1.create_time <= 3600
      AND create_date >= CURDATE();
  ```

  しかし、ジョインに STRING 型の `order_uuid` 列を使用するのは、INTEGER 列を使用するよりも効率が悪いです。

## 最適化アプローチ

上記のシナリオの問題に対処するために、最適化アプローチは、注文データをターゲットテーブルにロードし、STRING 値と INTEGER 値の間にマッピングを確立することを含みます。後続のクエリアナリシスは INTEGER 列に基づいて行われます。このアプローチは次のステージに分けられます:

1. ステージ 1: グローバル辞書を作成し、STRING 値と INTEGER 値の間にマッピングを確立します。この辞書では、キー列は STRING 型で、値列は AUTO INCREMENT INTEGER 型です。データがロードされると、システムは各 STRING 値に対して一意の ID を自動的に生成し、STRING 値と INTEGER 値の間にマッピングを作成します。

2. ステージ 2: 注文データとグローバル辞書の間のマッピング関係をターゲットテーブルにロードします。

3. ステージ 3: 後続のクエリアナリシス中に、正確な重複排除やジョインのためにターゲットテーブルの INTEGER 列を使用し、パフォーマンスを大幅に向上させます。

4. ステージ 4: さらなるパフォーマンスの最適化のために、INTEGER 列にビットマップ関数を使用して正確な重複排除を高速化できます。

## ソリューション

v3.2.5 より前では、ステージ 2 は次の 2 つの方法で実装できました:

- 外部テーブルまたは内部テーブルを中間テーブルとして使用し、ロード前に辞書テーブルとジョインして対応する辞書 ID を取得します。
- 主キーテーブルを使用してデータロードを行い、その後、ジョイン操作を伴う UPDATE ステートメントを使用して辞書 ID を更新します。しかし、このデータロードプロセスは不便で、多くの制約があります。

v3.2.5 以降、StarRocks は `dict_mapping()` 関数を導入し、ターゲットテーブルの辞書 ID 列を `dict_mapping()` 式を使用して生成列として定義できるようにしました。後続のデータロードタスクは、ジョイン操作を伴う UPDATE ステートメントを使用せずに、通常のデータロードのように処理されます。データロード中に、システムは元のテーブルを辞書テーブルと自動的に関連付け、対応する辞書 ID を挿入し、テーブルタイプに関係なく、さまざまなロード方法をサポートし、グローバル辞書テーブルを使用したデータロードプロセスを大幅に簡素化します。

### ビジネスシナリオ

以下の例では、`batch1.csv` と `batch2.csv` の 2 つの例の CSV ファイルを使用します。それぞれのファイルには、`id` と `order_uuid` の 2 つの列が含まれています。

- `batch1.csv`

  ```csv
  1, a1
  2, a2
  3, a3
  11, a1
  11, a2
  12, a1
  ```

- `batch2.csv`

  ```csv
  1, a2
  2, a2
  3, a2
  11, a2
  12, a101
  12, a102
  13, a102
  ```

### プロセス

#### ステージ 1

グローバル辞書テーブルを作成し、CSV ファイルから注文 ID 列の値をロードして、STRING 値と INTEGER 値の間にマッピングを確立します。

1. グローバル辞書として機能する主キーテーブルを作成します。主キー `order_uuid`（STRING 型）と値列 `order_id_int`（AUTO INCREMENT INTEGER 型）を定義します。

    :::info

    `dict_mapping` 関数は、グローバル辞書テーブルが主キーテーブルであることを要求します。

    :::

    ```SQL
    CREATE TABLE dict (
        order_uuid STRING,
        order_id_int BIGINT AUTO_INCREMENT  -- 各 order_uuid 値に自動的に ID を割り当てます。
    )
    PRIMARY KEY (order_uuid)
    DISTRIBUTED BY HASH (order_uuid)
    PROPERTIES("replicated_storage" = "true");
    ```

2. Stream Load を使用して、2 つの CSV ファイルから `order_uuid` 列を辞書テーブル `dict` の `order_uuid` 列にバッチロードします。列モードで部分更新を使用したことを確認してください。

    ```Bash
    curl --location-trusted -u root: \
        -H "partial_update: true" \
        -H "format: CSV" -H "column_separator:," -H "columns: id, order_uuid" \
        -T batch1.csv \
        -XPUT http://<fe_host>:<fe_http_port>/api/example_db/dict/_stream_load
        
    curl --location-trusted -u root: \
        -H "partial_update: true" \
        -H "format: CSV" -H "column_separator:," -H "columns: id, order_uuid" \
        -T batch2.csv \
        -XPUT http://<fe_host>:<fe_http_port>/api/example_db/dict/_stream_load
    ```

> **注意**
>
> 次のステージに進む前にデータソースに新しいデータが追加された場合、すべての新しいデータを辞書テーブルにロードして、マッピングが存在することを確認する必要があります。

#### ステージ 2

辞書 ID 列に `dict_mapping` 属性を持つターゲットテーブルを作成します。注文データがターゲットテーブルにロードされると、システムは自動的に辞書テーブルと関連付け、対応する辞書 ID を挿入します。

1. CSV ファイルのすべての列を含むテーブル `dest_table` を作成します。また、STRING 型の `order_uuid` 列とマッピングする INTEGER 列 `order_id_int`（通常は BIGINT）を定義し、`dict_mapping` 列属性を持たせる必要があります。将来のクエリアナリシスはこの `order_id_int` 列に基づいて行われます。

    ```SQL
    -- ターゲットテーブルで、STRING 型の列 `order_uuid` とマッピングする BIGINT dict_mapping 列 `order_id_int` を定義します。
    CREATE TABLE dest_table (
        id BIGINT,
        order_uuid STRING, -- この列は STRING 型の注文 ID を記録します。
        batch INT comment '異なるバッチロードを区別するために使用されます',
        order_id_int BIGINT AS dict_mapping('dict', order_uuid) -- 辞書 ID dict_mapping 列は `order_uuid` に対応します。
    )
    DUPLICATE KEY (id, order_uuid)
    DISTRIBUTED BY HASH(id);
    ```

2. Stream Load または他の利用可能な方法を使用してターゲットテーブルにデータをロードします。`order_id_int` 列が `dict_mapping` 属性を持っているため、システムはロード中に `dict` から辞書 ID を自動的に取得します。

    ```Bash
    curl --location-trusted -u root: \
        -H "format: CSV" -H "column_separator:," -H "columns: id, order_uuid, batch=1" \
        -T batch1.csv \
        -XPUT http://<fe_host>:<fe_http_port>/api/example_db/dest_table/_stream_load
        
    curl --location-trusted -u root: \
        -H "format: CSV" -H "column_separator:," -H "columns: id, order_uuid, batch=2" \
        -T batch2.csv \
        -XPUT http://<fe_host>:<fe_http_port>/api/example_db/dest_table/_stream_load
    ```

#### ステージ 3

クエリアナリシス中に、INTEGER 列 `order_id_int` に対して正確な重複排除やジョインを行うことで、STRING 列 `order_uuid` を使用するよりもパフォーマンスが大幅に向上します。

```SQL
-- BIGINT 型の order_id_int に基づく正確な重複排除。
SELECT id, COUNT(DISTINCT order_id_int) FROM dest_table GROUP BY id ORDER BY id;
-- STRING 型の order_uuid に基づく正確な重複排除。
SELECT id, COUNT(DISTINCT order_uuid) FROM dest_table GROUP BY id ORDER BY id;
```

また、[ビットマップ関数を使用して正確な重複排除を高速化する](#use-bitmap-functions-to-accelerate-exact-deduplication) こともできます。

### ビットマップ関数を使用して正確な重複排除を高速化する

計算をさらに高速化するために、グローバル辞書を作成した後、辞書テーブルの INTEGER 列の値をビットマップ列に直接挿入できます。その後、このビットマップ列に対してビットマップ関数を使用して正確な重複排除を行うことができます。

#### 方法 1

グローバル辞書を構築し、すでに `dest_table` に注文データをインポートしている場合、次の手順に従います:

1. 2 つの列を持つ集計テーブル `dest_table_bitmap` を作成します: `bitmap_union()` 関数を使用して集計するための BITMAP 型 `order_id_bitmap` 列と INTEGER 型の `id` 列。このテーブルには元の STRING 列は含まれていません。そうでないと、各ビットマップには 1 つの値しか含まれず、加速の利点が失われます。

    ```SQL
    CREATE TABLE dest_table_bitmap (
        id BIGINT,
        order_id_bitmap BITMAP BITMAP_UNION
    )
    AGGREGATE KEY (id)
    DISTRIBUTED BY HASH(id) BUCKETS 6;
    ```

2. `dest_table_bitmap` にデータを挿入します。`dest_table` の `id` 列のデータを `id` 列に挿入し、辞書テーブル `dict` の INTEGER 列 `order_id_int` のデータを `to_bitmap()` 関数で処理して `order_id_bitmap` 列に挿入します。

    ```SQL
    INSERT INTO dest_table_bitmap (id, order_id_bitmap)
    SELECT id,  to_bitmap(dict_mapping('dict', order_uuid))
    FROM dest_table
    WHERE dest_table.batch = 1; -- 異なるバッチを示します。
        
    INSERT INTO dest_table_bitmap (id, order_id_bitmap)
    SELECT id, to_bitmap(dict_mapping('dict', order_uuid))
    FROM dest_table
    WHERE dest_table.batch = 2;
    ```

3. BITMAP 列に対して `BITMAP_UNION_COUNT()` 関数を使用して正確な重複排除を行います。

    ```SQL
    SELECT id, BITMAP_UNION_COUNT(order_id_bitmap) FROM dest_table_bitmap
    GROUP BY id ORDER BY id;
    ```

#### 方法 2

グローバル辞書を作成した後、特定の注文データを保持する必要がなく、データを直接 `dest_table_bitmap` テーブルにロードしたい場合、次の手順に従います:

1. 2 つの列を持つ集計テーブル `dest_table_bitmap` を作成します: `bitmap_union()` 関数を使用して集計するための BITMAP 型 `order_id_bitmap` 列と INTEGER 型の `id` 列。このテーブルには元の STRING 列は含まれていません。そうでないと、各ビットマップには 1 つの値しか含まれず、加速の利点が失われます。

    ```SQL
    CREATE TABLE dest_table_bitmap (
        id BIGINT,
        order_id_bitmap BITMAP BITMAP_UNION
    )
    AGGREGATE KEY (id)
    DISTRIBUTED BY HASH(id) BUCKETS 6;
    ```

2. 集計テーブルにデータを挿入します。CSV ファイルの `id` 列のデータを `id` 列に挿入し、辞書テーブル `dict` の INTEGER 列 `order_id_int` のデータを `to_bitmap()` 関数で処理して `order_id_bitmap` 列に挿入します。

    ```bash
    curl --location-trusted -u root: \
        -H "format: CSV" -H "column_separator:," \
        -H "columns: id, order_uuid,  order_id_bitmap=to_bitmap(dict_mapping('dict', order_uuid))" \
        -T batch1.csv \
        -XPUT http://<fe_host>:<fe_http_port>/api/example_db/dest_table_bitmap/_stream_load
    
    curl --location-trusted -u root: \
        -H "format: CSV" -H "column_separator:," \
        -H "columns: id, order_uuid, order_id_bitmap=to_bitmap(dict_mapping('dict', order_uuid))" \
        -T batch2.csv \
        -XPUT http:///<fe_host>:<fe_http_port>/api/example_db/dest_table_bitmap/_stream_load
    ```

3. BITMAP 列に対して `BITMAP_UNION_COUNT()` 関数を使用して正確な重複排除を行います。

    ```SQL
    SELECT id, BITMAP_UNION_COUNT(order_id_bitmap) FROM dest_table_bitmap
    GROUP BY id ORDER BY id;
    ```
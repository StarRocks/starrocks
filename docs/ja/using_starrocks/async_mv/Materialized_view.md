---
displayed_sidebar: docs
sidebar_position: 10
keywords: ['materialized', 'view', 'views']
---

# 非同期マテリアライズドビュー

このトピックでは、非同期マテリアライズドビューの理解、作成、使用、管理方法について説明します。非同期マテリアライズドビューは、StarRocks v2.4以降でサポートされています。

同期マテリアライズドビューと比較して、非同期マテリアライズドビューはマルチテーブルジョインやより多くの集計関数をサポートしています。非同期マテリアライズドビューのリフレッシュは、手動で、事前に指定された間隔で定期的に、またはベーステーブルのデータ変更時に自動的にトリガーされることがあります。また、マテリアライズドビュー全体ではなく一部のパーティションをリフレッシュすることもでき、リフレッシュのコストを大幅に削減できます。さらに、非同期マテリアライズドビューはさまざまなクエリの書き換えシナリオをサポートし、自動的かつ透明なクエリアクセラレーションを可能にします。

同期マテリアライズドビュー（Rollup）のシナリオと使用法については、[Synchronous materialized view (Rollup)](../Materialized_view-single_table.md) を参照してください。

## 概要

データベースのアプリケーションでは、大規模なテーブルに対して複雑なクエリを実行することがよくあります。これらのクエリは、数十億行を含むテーブルでのマルチテーブルジョインや集計を伴います。これらのクエリを処理するには、システムリソースや結果を計算するための時間がかかることがあります。

StarRocksの非同期マテリアライズドビューは、これらの問題に対処するために設計されています。非同期マテリアライズドビューは、1つ以上のベーステーブルから事前に計算されたクエリ結果を保持する特別な物理テーブルです。ベーステーブルに対して複雑なクエリを実行すると、StarRocksは関連するマテリアライズドビューから事前に計算された結果を返してこれらのクエリを処理します。この方法により、繰り返しの複雑な計算を回避できるため、クエリパフォーマンスが向上します。このパフォーマンスの違いは、クエリが頻繁に実行される場合や十分に複雑な場合に顕著です。

さらに、非同期マテリアライズドビューは、データウェアハウス上で数学モデルを構築するのに特に役立ちます。これにより、上位層のアプリケーションに統一されたデータ仕様を提供し、基盤の実装を隠蔽したり、ベーステーブルの生データのセキュリティを保護したりすることができます。

### StarRocksにおけるマテリアライズドビューの理解

StarRocks v2.3以前のバージョンでは、単一テーブル上にのみ構築可能な同期マテリアライズドビューが提供されていました。同期マテリアライズドビュー、またはRollupは、データの新鮮さを高く保ち、リフレッシュコストを低く抑えます。しかし、v2.4以降でサポートされている非同期マテリアライズドビューと比較すると、同期マテリアライズドビューには多くの制限があります。クエリを加速または書き換えるために同期マテリアライズドビューを構築したい場合、集計オペレーターの選択肢が限られています。

次の表は、StarRocksにおける非同期マテリアライズドビュー（ASYNC MV）と同期マテリアライズドビュー（SYNC MV）のサポートされている機能の観点からの比較です。

|                       | **単一テーブル集計** | **マルチテーブルジョイン** | **クエリの書き換え** | **リフレッシュ戦略** | **ベーステーブル** |
| --------------------- | -------------------- | -------------------- | ----------------- | -------------------- | -------------- |
| **ASYNC MV** | はい | はい | はい | <ul><li>非同期リフレッシュ</li><li>手動リフレッシュ</li></ul> | 複数のテーブルから:<ul><li>Default Catalog</li><li>External catalogs (v2.5)</li><li>既存のマテリアライズドビュー (v2.5)</li><li>既存のビュー (v3.1)</li></ul> |
| **SYNC MV (Rollup)**  | 集計関数の選択肢が限られる | いいえ | はい | データロード中の同期リフレッシュ | Default Catalog の単一テーブル |

### 基本概念

- **ベーステーブル**

  ベーステーブルは、マテリアライズドビューの駆動テーブルです。

  StarRocksの非同期マテリアライズドビューでは、ベーステーブルは [default catalog](../../data_source/catalog/default_catalog.md) のStarRocks内部テーブル、外部カタログのテーブル（v2.5からサポート）、既存の非同期マテリアライズドビュー（v2.5からサポート）、およびビュー（v3.1からサポート）であることができます。StarRocksは、すべての [StarRocksテーブルタイプ](../../table_design/table_types/table_types.md) に対して非同期マテリアライズドビューの作成をサポートしています。

- **リフレッシュ**

  非同期マテリアライズドビューを作成すると、そのデータはその時点でのベーステーブルの状態を反映します。ベーステーブルのデータが変更された場合、マテリアライズドビューをリフレッシュして変更を同期させる必要があります。

  現在、StarRocksは2つの一般的なリフレッシュ戦略をサポートしています。

  - ASYNC: 非同期リフレッシュモード。ベーステーブルのデータが変更されたとき、または指定された間隔に基づいて定期的にマテリアライズドビューを自動的にリフレッシュできます。
  - MANUAL: 手動リフレッシュモード。マテリアライズドビューは自動的にリフレッシュされません。リフレッシュタスクはユーザーによって手動でのみトリガーされます。

- **クエリの書き換え**

  クエリの書き換えとは、マテリアライズドビューが構築されたベーステーブルに対してクエリを実行する際に、システムがマテリアライズドビューの事前計算結果をクエリに再利用できるかどうかを自動的に判断することを意味します。再利用できる場合、システムは関連するマテリアライズドビューからデータを直接ロードし、時間とリソースを消費する計算やジョインを回避します。

  v2.5以降、StarRocksはSPJGタイプの非同期マテリアライズドビューに基づく自動的かつ透明なクエリの書き換えをサポートしています。SPJGタイプのマテリアライズドビューは、プランにScan、Filter、Project、およびAggregateタイプのオペレーターのみを含むマテリアライズドビューを指します。

  > **注意**
  >
  > JDBCカタログまたはHudiカタログのベーステーブルに作成された非同期マテリアライズドビューは、クエリの書き換えをサポートしていません。

## マテリアライズドビューを作成するタイミングを決定する

データウェアハウス環境で次のような要求がある場合、非同期マテリアライズドビューを作成できます。

- **繰り返しの集計関数を使用したクエリの加速**

  データウェアハウスのほとんどのクエリが集計関数を含む同じサブクエリを含み、これらのクエリが計算リソースの大部分を消費しているとします。このサブクエリに基づいて非同期マテリアライズドビューを作成することで、サブクエリのすべての結果を計算して保存できます。マテリアライズドビューが構築された後、StarRocksはサブクエリを含むすべてのクエリを書き換え、マテリアライズドビューに保存された中間結果をロードし、これらのクエリを加速します。

- **複数テーブルの定期的なジョイン**

  データウェアハウスで複数のテーブルを定期的にジョインして新しいワイドテーブルを作成する必要があるとします。これらのテーブルに対して非同期マテリアライズドビューを構築し、固定時間間隔でリフレッシュタスクをトリガーするASYNCリフレッシュ戦略を設定できます。マテリアライズドビューが構築された後、クエリ結果はマテリアライズドビューから直接返され、ジョイン操作による遅延が回避されます。

- **データウェアハウスのレイヤリング**

  データウェアハウスに大量の生データが含まれており、クエリが複雑なETL操作セットを必要とする場合、非同期マテリアライズドビューの複数のレイヤーを構築してデータウェアハウス内のデータを階層化し、クエリを一連の単純なサブクエリに分解できます。これにより、繰り返しの計算を大幅に削減し、さらに重要なことに、DBAが問題を簡単かつ効率的に特定するのに役立ちます。それに加えて、データウェアハウスのレイヤリングは、生データと統計データを分離し、機密性の高い生データのセキュリティを保護します。

- **データレイクでのクエリの加速**

  データレイクのクエリは、ネットワーク遅延やオブジェクトストレージのスループットのために遅くなることがあります。データレイクの上に非同期マテリアライズドビューを構築することで、クエリパフォーマンスを向上させることができます。さらに、StarRocksは既存のマテリアライズドビューを使用するようにクエリをインテリジェントに書き換えることができ、クエリを手動で変更する手間を省くことができます。

非同期マテリアライズドビューの具体的な使用例については、以下のコンテンツを参照してください。

- [Data Modeling](use_cases/data_modeling_with_materialized_views.md)
- [Query Rewrite](use_cases/query_rewrite_with_materialized_views.md)
- [Data Lake Query Acceleration](use_cases/data_lake_query_acceleration_with_materialized_views.md)

## 非同期マテリアライズドビューを作成する

StarRocksの非同期マテリアライズドビューは、次のベーステーブルに基づいて作成できます。

- StarRocksの内部テーブル（すべてのStarRocksテーブルタイプがサポートされています）
- [external catalogs](./feature-support-asynchronous-materialized-views.md#materialized-views-on-external-catalogs) のテーブル
- 既存の非同期マテリアライズドビュー（v2.5以降）
- 既存のビュー（v3.1以降）

### 始める前に

次の例では、default catalog の2つのベーステーブルを使用します。

- テーブル `goods` は、アイテムID `item_id1`、アイテム名 `item_name`、およびアイテム価格 `price` を記録します。
- テーブル `order_list` は、注文ID `order_id`、クライアントID `client_id`、アイテムID `item_id2`、および注文日 `order_date` を記録します。

列 `goods.item_id1` は列 `order_list.item_id2` と等価です。

次のステートメントを実行してテーブルを作成し、データを挿入します。

```SQL
CREATE TABLE goods(
    item_id1          INT,
    item_name         STRING,
    price             FLOAT
) DISTRIBUTED BY HASH(item_id1);

INSERT INTO goods
VALUES
    (1001,"apple",6.5),
    (1002,"pear",8.0),
    (1003,"potato",2.2);

CREATE TABLE order_list(
    order_id          INT,
    client_id         INT,
    item_id2          INT,
    order_date        DATE
) DISTRIBUTED BY HASH(order_id);

INSERT INTO order_list
VALUES
    (10001,101,1001,"2022-03-13"),
    (10001,101,1002,"2022-03-13"),
    (10002,103,1002,"2022-03-13"),
    (10002,103,1003,"2022-03-14"),
    (10003,102,1003,"2022-03-14"),
    (10003,102,1001,"2022-03-14");
```

次の例のシナリオでは、各注文の合計を頻繁に計算する必要があります。これは、2つのベーステーブルの頻繁なジョインと集計関数 `sum()` の集中的な使用を必要とします。さらに、ビジネスシナリオでは、データのリフレッシュが1日の間隔で必要です。

クエリステートメントは次のとおりです。

```SQL
SELECT
    order_id,
    sum(goods.price) as total
FROM order_list INNER JOIN goods ON goods.item_id1 = order_list.item_id2
GROUP BY order_id;
```

### マテリアライズドビューを作成する

特定のクエリステートメントに基づいてマテリアライズドビューを作成するには、[CREATE MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW.md) を使用します。

テーブル `goods`、`order_list`、および上記のクエリステートメントに基づいて、次の例では各注文の合計を分析するためのマテリアライズドビュー `order_mv` を作成します。マテリアライズドビューは1日の間隔で自動的にリフレッシュされるように設定されています。

```SQL
CREATE MATERIALIZED VIEW order_mv
DISTRIBUTED BY HASH(`order_id`)
REFRESH ASYNC START('2022-09-01 10:00:00') EVERY (interval 1 day)
AS SELECT
    order_list.order_id,
    sum(goods.price) as total
FROM order_list INNER JOIN goods ON goods.item_id1 = order_list.item_id2
GROUP BY order_id;
```

> **注意**
>
> - 非同期マテリアライズドビューを作成する際には、データ分散戦略またはマテリアライズドビューのリフレッシュ戦略、またはその両方を指定する必要があります。
> - 非同期マテリアライズドビューのパーティショニングおよびバケッティング戦略は、ベーステーブルのものとは異なるものを設定できますが、マテリアライズドビューを作成するために使用されるクエリステートメントにパーティションキーとバケットキーを含める必要があります。
> - 非同期マテリアライズドビューは、より長いスパンでの動的パーティショニング戦略をサポートしています。たとえば、ベーステーブルが1日の間隔でパーティション化されている場合、マテリアライズドビューを1か月の間隔でパーティション化するように設定できます。
> - 現在、StarRocksはリストパーティショニング戦略を使用して作成されたテーブルに基づく非同期マテリアライズドビューの作成をサポートしていません。
> - マテリアライズドビューを作成するために使用されるクエリステートメントは、rand()、random()、uuid()、およびsleep()を含むランダム関数をサポートしていません。
> - 非同期マテリアライズドビューは、さまざまなデータタイプをサポートしています。詳細については、[CREATE MATERIALIZED VIEW - Supported data types](../../sql-reference/sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW.md#supported-data-types) を参照してください。
> - デフォルトでは、CREATE MATERIALIZED VIEWステートメントを実行すると、リフレッシュタスクが即座にトリガーされ、システムリソースの一定の割合を消費する可能性があります。リフレッシュタスクを遅延させたい場合は、CREATE MATERIALIZED VIEWステートメントにREFRESH DEFERREDパラメータを追加できます。

- **非同期マテリアライズドビューのリフレッシュメカニズムについて**

  現在、StarRocksは2つのオンデマンドリフレッシュ戦略をサポートしています: MANUALリフレッシュとASYNCリフレッシュ。

  StarRocks v2.5では、非同期マテリアライズドビューはリフレッシュのコストを制御し、成功率を高めるためのさまざまな非同期リフレッシュメカニズムをさらにサポートしています。

  - マテリアライズドビューに多くの大きなパーティションがある場合、各リフレッシュは大量のリソースを消費する可能性があります。v2.5では、StarRocksはリフレッシュタスクの分割をサポートしています。リフレッシュする最大パーティション数を指定でき、StarRocksは指定された最大パーティション数以下のバッチサイズでバッチごとにリフレッシュを実行します。この機能は、大規模な非同期マテリアライズドビューが安定してリフレッシュされることを保証し、データモデリングの安定性と堅牢性を向上させます。
  - 非同期マテリアライズドビューのパーティションの有効期限（TTL）を指定して、マテリアライズドビューが占めるストレージサイズを削減できます。
  - 最新のいくつかのパーティションのみをリフレッシュするためのリフレッシュ範囲を指定して、リフレッシュのオーバーヘッドを削減できます。
  - データ変更が対応するマテリアライズドビューのリフレッシュを自動的にトリガーしないベーステーブルを指定できます。
  - リフレッシュタスクにリソースグループを割り当てることができます。

  詳細については、[CREATE MATERIALIZED VIEW - Parameters](../../sql-reference/sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW.md#parameters) の **PROPERTIES** セクションを参照してください。既存の非同期マテリアライズドビューのメカニズムを変更するには、[ALTER MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/ALTER_MATERIALIZED_VIEW.md) を使用できます。

  > **注意**
  >
  > フルリフレッシュ操作がシステムリソースを使い果たし、タスクの失敗を引き起こすのを防ぐために、パーティション化されたベーステーブルに基づいてパーティション化されたマテリアライズドビューを作成することをお勧めします。これにより、ベーステーブルのパーティション内でデータの更新が発生した場合、マテリアライズドビューの対応するパーティションのみがリフレッシュされ、マテリアライズドビュー全体をリフレッシュすることはありません。詳細については、[Data Modeling with Materialized Views - Partitioned Modeling](use_cases/data_modeling_with_materialized_views.md#partitioned-modeling) を参照してください。

- **ネストされたマテリアライズドビューについて**

  StarRocks v2.5は、ネストされた非同期マテリアライズドビューの作成をサポートしています。既存の非同期マテリアライズドビューに基づいて非同期マテリアライズドビューを構築できます。各マテリアライズドビューのリフレッシュ戦略は、上位または下位のマテリアライズドビューには影響しません。現在、StarRocksはネストレベルの数を制限していません。実稼働環境では、ネストレイヤーの数が3を超えないことをお勧めします。

- **外部カタログマテリアライズドビューについて**

  StarRocksは、Hive Catalog（v2.5以降）、Hudi Catalog（v2.5以降）、Iceberg Catalog（v2.5以降）、およびJDBC Catalog（v3.0以降）に基づく非同期マテリアライズドビューの構築をサポートしています。外部カタログにマテリアライズドビューを作成することは、default catalog に非同期マテリアライズドビューを作成することと似ていますが、いくつかの使用制限があります。詳細については、[Data lake query acceleration with materialized views](use_cases/data_lake_query_acceleration_with_materialized_views.md) を参照してください。

## 非同期マテリアライズドビューを手動でリフレッシュする

非同期マテリアライズドビューは、そのリフレッシュ戦略に関係なく、[REFRESH MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/REFRESH_MATERIALIZED_VIEW.md) を使用してリフレッシュできます。StarRocks v2.5は、パーティション名を指定して非同期マテリアライズドビューの特定のパーティションをリフレッシュすることをサポートしています。StarRocks v3.1は、リフレッシュタスクの同期呼び出しをサポートしており、タスクが成功または失敗したときにのみSQLステートメントが返されます。

```SQL
-- 非同期呼び出し（デフォルト）を介してマテリアライズドビューをリフレッシュします。
REFRESH MATERIALIZED VIEW order_mv;
-- 同期呼び出しを介してマテリアライズドビューをリフレッシュします。
REFRESH MATERIALIZED VIEW order_mv WITH SYNC MODE;
```

非同期呼び出しを介して送信されたリフレッシュタスクをキャンセルするには、[CANCEL REFRESH MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/CANCEL_REFRESH_MATERIALIZED_VIEW.md) を使用します。

## 非同期マテリアライズドビューを直接クエリする

作成した非同期マテリアライズドビューは、クエリステートメントに従って完全な事前計算結果を含む物理テーブルです。したがって、マテリアライズドビューが初めてリフレッシュされた後、直接クエリすることができます。

```Plain
MySQL > SELECT * FROM order_mv;
+----------+--------------------+
| order_id | total              |
+----------+--------------------+
|    10001 |               14.5 |
|    10002 | 10.200000047683716 |
|    10003 |  8.700000047683716 |
+----------+--------------------+
3 rows in set (0.01 sec)
```

> **注意**
>
> 非同期マテリアライズドビューを直接クエリすることができますが、ベーステーブルに対するクエリから得られる結果と一致しない場合があります。

## 非同期マテリアライズドビューでクエリをリライトして加速する

StarRocks v2.5は、SPJGタイプの非同期マテリアライズドビューに基づく自動的かつ透明なクエリの書き換えをサポートしています。SPJGタイプのマテリアライズドビューのクエリの書き換えには、単一テーブルのクエリの書き換え、ジョインクエリの書き換え、集計クエリの書き換え、ユニオンクエリの書き換え、およびネストされたマテリアライズドビューに基づくクエリの書き換えが含まれます。詳細については、[Query Rewrite with Materialized Views](./use_cases/query_rewrite_with_materialized_views.md) を参照してください。

現在、StarRocksは、default catalog またはHive catalog、Hudi catalog、Iceberg catalogなどの外部カタログに作成された非同期マテリアライズドビューに対するクエリの書き換えをサポートしています。default catalog のデータをクエリする際、StarRocksは、ベーステーブルとデータが一致しないマテリアライズドビューを除外することで、書き換えられたクエリと元のクエリの結果の強い一貫性を保証します。マテリアライズドビューのデータが期限切れになると、マテリアライズドビューは候補マテリアライズドビューとして使用されません。外部カタログのデータをクエリする際、StarRocksは、外部カタログのデータ変更を認識できないため、結果の強い一貫性を保証しません。外部カタログに基づいて作成された非同期マテリアライズドビューの詳細については、[Data lake query acceleration with materialized views](use_cases/data_lake_query_acceleration_with_materialized_views.md) を参照してください。

> **注意**
>
> JDBCカタログのベーステーブルに作成された非同期マテリアライズドビューは、クエリの書き換えをサポートしていません。

## 非同期マテリアライズドビューを管理する

### 非同期マテリアライズドビューを変更する

非同期マテリアライズドビューのプロパティを変更するには、[ALTER MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/ALTER_MATERIALIZED_VIEW.md) を使用します。

- 非アクティブなマテリアライズドビューを有効にします。

  ```SQL
  ALTER MATERIALIZED VIEW order_mv ACTIVE;
  ```

- 非同期マテリアライズドビューの名前を変更します。

  ```SQL
  ALTER MATERIALIZED VIEW order_mv RENAME order_total;
  ```

- 非同期マテリアライズドビューのリフレッシュ間隔を2日に変更します。

  ```SQL
  ALTER MATERIALIZED VIEW order_mv REFRESH ASYNC EVERY(INTERVAL 2 DAY);
  ```

### 非同期マテリアライズドビューを表示する

データベース内の非同期マテリアライズドビューを表示するには、[SHOW MATERIALIZED VIEWS](../../sql-reference/sql-statements/materialized_view/SHOW_MATERIALIZED_VIEW.md) を使用するか、Information Schemaのシステムメタデータビューをクエリします。

- データベース内のすべての非同期マテリアライズドビューを確認します。

  ```SQL
  SHOW MATERIALIZED VIEWS;
  ```

- 特定の非同期マテリアライズドビューを確認します。

  ```SQL
  SHOW MATERIALIZED VIEWS WHERE NAME = "order_mv";
  ```

- 名前に一致する特定の非同期マテリアライズドビューを確認します。

  ```SQL
  SHOW MATERIALIZED VIEWS WHERE NAME LIKE "order%";
  ```

- Information Schemaのメタデータビュー `materialized_views` をクエリして、すべての非同期マテリアライズドビューを確認します。詳細については、[information_schema.materialized_views](../../sql-reference/information_schema/materialized_views.md) を参照してください。

  ```SQL
  SELECT * FROM information_schema.materialized_views;
  ```

### 非同期マテリアライズドビューの定義を確認する

非同期マテリアライズドビューを作成するために使用されたクエリを確認するには、[SHOW CREATE MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/SHOW_CREATE_MATERIALIZED_VIEW.md) を使用します。

```SQL
SHOW CREATE MATERIALIZED VIEW order_mv;
```

### 非同期マテリアライズドビューの実行ステータスを確認する

非同期マテリアライズドビューの実行（構築またはリフレッシュ）ステータスを確認するには、[Information Schema](../../sql-reference/information_schema/information_schema.md) の [`tasks`](../../sql-reference/information_schema/tasks.md) および [`task_runs`](../../sql-reference/information_schema/task_runs.md) をクエリします。

次の例では、最も最近作成されたマテリアライズドビューの実行ステータスを確認します。

1. テーブル `tasks` で最も最近のタスクの `TASK_NAME` を確認します。

    ```Plain
    mysql> select * from information_schema.tasks  order by CREATE_TIME desc limit 1\G;
    *************************** 1. row ***************************
      TASK_NAME: mv-59299
    CREATE_TIME: 2022-12-12 17:33:51
      SCHEDULE: MANUAL
      DATABASE: ssb_1
    DEFINITION: insert overwrite hive_mv_lineorder_flat_1 SELECT `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_orderkey`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_linenumber`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_custkey`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_partkey`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_orderpriority`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_ordtotalprice`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_revenue`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`p_mfgr`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`s_nation`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`c_city`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`c_nation`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_orderdate`
    FROM `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`
    WHERE `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_orderdate` = '1997-01-01'
    EXPIRE_TIME: NULL
    1 row in set (0.02 sec)
    ```

2. 見つけた `TASK_NAME` を使用して、テーブル `task_runs` で実行ステータスを確認します。

    ```Plain
    mysql> select * from information_schema.task_runs where task_name='mv-59299' order by CREATE_TIME\G
    *************************** 1. row ***************************
        QUERY_ID: d9cef11f-7a00-11ed-bd90-00163e14767f
        TASK_NAME: mv-59299
      CREATE_TIME: 2022-12-12 17:39:19
      FINISH_TIME: 2022-12-12 17:39:22
            STATE: SUCCESS
        DATABASE: ssb_1
      DEFINITION: insert overwrite hive_mv_lineorder_flat_1 SELECT `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_orderkey`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_linenumber`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_custkey`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_partkey`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_orderpriority`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_ordtotalprice`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_revenue`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`p_mfgr`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`s_nation`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`c_city`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`c_nation`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_orderdate`
    FROM `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`
    WHERE `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_orderdate` = '1997-01-01'
      EXPIRE_TIME: 2022-12-15 17:39:19
      ERROR_CODE: 0
    ERROR_MESSAGE: NULL
        PROGRESS: 100%
    2 rows in set (0.02 sec)
    ```

### 非同期マテリアライズドビューを削除する

非同期マテリアライズドビューを削除するには、[DROP MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/DROP_MATERIALIZED_VIEW.md) を使用します。

```Plain
DROP MATERIALIZED VIEW order_mv;
```

### 関連するセッション変数

次の変数は、非同期マテリアライズドビューの動作を制御します。

- `analyze_mv`: リフレッシュ後にマテリアライズドビューを分析するかどうか、およびその方法。 有効な値は、空の文字列（分析しない）、`sample`（サンプリングされた統計収集）、および `full`（完全な統計収集）です。デフォルトは `sample` です。
- `enable_materialized_view_rewrite`: マテリアライズドビューの自動書き換えを有効にするかどうか。 有効な値は `true`（v2.5以降のデフォルト）と `false` です。
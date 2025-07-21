---
displayed_sidebar: docs
sidebar_position: 30
---

# マテリアライズドビューによるデータレイククエリアクセラレーション

このトピックでは、StarRocks の非同期マテリアライズドビューを使用してデータレイク内のクエリパフォーマンスを最適化する方法について説明します。

StarRocks は、データレイク内のデータの探索的クエリや分析に非常に効果的な、すぐに使えるデータレイククエリ機能を提供します。ほとんどのシナリオでは、[Data Cache](../../../data_source/data_cache.md) がブロックレベルのファイルキャッシングを提供し、リモートストレージのジッターや大量の I/O 操作によるパフォーマンス低下を回避できます。

しかし、データレイクからのデータを使用して複雑で効率的なレポートを作成したり、これらのクエリをさらに加速したりする場合、パフォーマンスの課題に直面することがあります。非同期マテリアライズドビューを使用することで、レポートやデータアプリケーションのために、より高い同時実行性と優れたクエリパフォーマンスを実現できます。

## 概要

StarRocks は、Hive catalog、Iceberg catalog、Hudi catalog、JDBC catalog、Paimon catalog などの external catalog に基づく非同期マテリアライズドビューの構築をサポートしています。external catalog ベースのマテリアライズドビューは、特に次のようなシナリオで有用です。

- **データレイクレポートの透明なアクセラレーション**

  データレイクレポートのクエリパフォーマンスを確保するために、データエンジニアは通常、データアナリストと密接に協力して、レポートのアクセラレーションレイヤーの構築ロジックを探る必要があります。アクセラレーションレイヤーにさらなる更新が必要な場合、構築ロジック、処理スケジュール、およびクエリステートメントを適宜更新する必要があります。

  マテリアライズドビューのクエリの書き換え機能を通じて、レポートアクセラレーションはユーザーにとって透明で知覚できないものになります。遅いクエリが特定された場合、データエンジニアは遅いクエリのパターンを分析し、必要に応じてマテリアライズドビューを作成できます。アプリケーション側のクエリは、マテリアライズドビューによってインテリジェントに書き換えられ、透明に加速されるため、ビジネスアプリケーションやクエリステートメントのロジックを変更することなく、クエリパフォーマンスを迅速に向上させることができます。

- **履歴データと関連するリアルタイムデータのインクリメンタル計算**

  ビジネスアプリケーションが StarRocks の内部テーブルにあるリアルタイムデータとデータレイクにある履歴データの関連付けを必要とする場合、マテリアライズドビューは簡単な解決策を提供します。たとえば、リアルタイムのファクトテーブルが StarRocks の内部テーブルであり、ディメンジョンテーブルがデータレイクに保存されている場合、内部テーブルと外部データソースのテーブルを関連付けるマテリアライズドビューを構築することで、インクリメンタル計算を簡単に実行できます。

- **メトリックレイヤーの迅速な構築**

  高次元のデータを扱う際、メトリックの計算と処理に課題が生じることがあります。マテリアライズドビューを使用すると、データの事前集計とロールアップを実行して、比較的軽量なメトリックレイヤーを作成できます。さらに、マテリアライズドビューは自動的にリフレッシュされるため、メトリック計算の複雑さをさらに軽減します。

マテリアライズドビュー、Data Cache、および StarRocks の内部テーブルはすべて、クエリパフォーマンスを大幅に向上させる効果的な方法です。以下の表は、それらの主な違いを比較しています。

<table class="comparison">
  <thead>
    <tr>
      <th>&nbsp;</th>
      <th>Data Cache</th>
      <th>Materialized view</th>
      <th>Native table</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><b>データロードと更新</b></td>
      <td>クエリが自動的にデータキャッシングをトリガーします。</td>
      <td>リフレッシュタスクが自動的にトリガーされます。</td>
      <td>さまざまなインポート方法をサポートしますが、インポートタスクの手動メンテナンスが必要です。</td>
    </tr>
    <tr>
      <td><b>データキャッシングの粒度</b></td>
      <td><ul><li>ブロックレベルのデータキャッシングをサポート</li><li>LRU キャッシュエビクションメカニズムに従う</li><li>計算結果はキャッシュされません</li></ul></td>
      <td>事前計算されたクエリ結果を保存します</td>
      <td>テーブルスキーマに基づいてデータを保存します</td>
    </tr>
    <tr>
      <td><b>クエリパフォーマンス</b></td>
      <td colspan="3" style={{textAlign: 'center'}} >Data Cache &le; Materialized view = Native table</td>
    </tr>
    <tr>
      <td><b>クエリステートメント</b></td>
      <td><ul><li>データレイクに対するクエリステートメントを変更する必要はありません</li><li>クエリがキャッシュにヒットすると、計算が行われます。</li></ul></td>
      <td><ul><li>データレイクに対するクエリステートメントを変更する必要はありません</li><li>事前計算された結果を再利用するためにクエリの書き換えを活用します</li></ul></td>
      <td>内部テーブルをクエリするためにクエリステートメントの変更が必要です</td>
    </tr>
  </tbody>
</table>

<br />

データレイクのデータを直接クエリするか、内部テーブルにデータをロードするのと比較して、マテリアライズドビューは以下のようなユニークな利点を提供します：

- **ローカルストレージアクセラレーション**: マテリアライズドビューは、StarRocks のインデックス、パーティショニング、バケッティング、コロケートグループなどのローカルストレージによるアクセラレーションの利点を活用でき、データレイクから直接クエリするよりも優れたクエリパフォーマンスを実現します。
- **ロードタスクのゼロメンテナンス**: マテリアライズドビューは自動リフレッシュタスクを通じてデータを透明に更新します。スケジュールされたデータ更新を実行するためにロードタスクを維持する必要はありません。さらに、Hive、Iceberg、および Paimon catalog ベースのマテリアライズドビューは、データの変更を検出し、パーティションレベルでインクリメンタルリフレッシュを実行できます。
- **インテリジェントクエリの書き換え**: クエリはマテリアライズドビューを使用するように透明に書き換えられます。クエリステートメントを変更することなく、即座にアクセラレーションの恩恵を受けることができます。

したがって、次のようなシナリオでマテリアライズドビューの使用をお勧めします：

- Data Cache が有効になっていても、クエリのレイテンシーと同時実行性に対する要件を満たしていない場合。
- クエリが再利用可能なコンポーネントを含む場合（例：固定された集計関数やジョインパターン）。
- データがパーティションに整理されており、クエリが比較的高レベルでの集計を含む場合（例：日ごとの集計）。

次のシナリオでは、Data Cache を通じたアクセラレーションを優先することをお勧めします：

- クエリに多くの再利用可能なコンポーネントがなく、データレイクから任意のデータをスキャンする可能性がある場合。
- リモートストレージに大きな変動や不安定性があり、アクセスに影響を与える可能性がある場合。

## 外部カタログベースのマテリアライズドビューの作成

external catalog のテーブルにマテリアライズドビューを作成することは、StarRocks の内部テーブルにマテリアライズドビューを作成することと似ています。使用するデータソースに応じて適切なリフレッシュ戦略を設定し、外部カタログベースのマテリアライズドビューのクエリの書き換えを手動で有効にするだけです。

### 適切なリフレッシュ戦略を選択する

現在、StarRocks は Hudi catalog のパーティションレベルのデータ変更を検出できません。そのため、タスクがトリガーされるとフルサイズのリフレッシュが実行されます。

Hive Catalog、Iceberg Catalog（v3.1.4 以降）、JDBC catalog（v3.1.4 以降、MySQL の範囲パーティションテーブルのみ）、および Paimon Catalog（v3.2.1 以降）では、StarRocks はパーティションレベルでのデータ変更を検出することをサポートしています。その結果、StarRocks は次のことが可能です：

- データ変更のあるパーティションのみをリフレッシュし、フルサイズのリフレッシュを回避し、リフレッシュによるリソース消費を削減します。

- クエリの書き換え中にある程度のデータ整合性を確保します。データレイクのベーステーブルにデータ変更がある場合、クエリはマテリアライズドビューを使用するように書き換えられません。

:::tip

マテリアライズドビューを作成する際にプロパティ `mv_rewrite_staleness_second` を設定することで、ある程度のデータ不整合を許容することもできます。詳細については、[CREATE MATERIALIZED VIEW](../../../sql-reference/sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW.md) を参照してください。

:::

パーティションごとにリフレッシュする必要がある場合、マテリアライズドビューのパーティションキーはベーステーブルのパーティションキーに含まれている必要があります。

v3.2.3 以降、StarRocks は Iceberg テーブルに [Partition Transforms](https://iceberg.apache.org/spec/#partition-transforms) を使用してパーティション化されたマテリアライズドビューの作成をサポートしており、マテリアライズドビューは変換後の列でパーティション化されます。現在、`identity`、`year`、`month`、`day`、`hour` の変換を持つ Iceberg テーブルのみがサポートされています。

以下の例は、`day` パーティション変換を持つ Iceberg テーブルの定義を示し、それに対して整列したパーティションを持つマテリアライズドビューを作成します：

```SQL
-- Iceberg テーブルの定義。
CREATE TABLE spark_catalog.test.iceberg_sample_datetime_day (
  id         BIGINT,
  data       STRING,
  category   STRING,
  ts         TIMESTAMP)
USING iceberg
PARTITIONED BY (days(ts))

-- Iceberg テーブルに対するマテリアライズドビューを作成。
CREATE MATERIALIZED VIEW `test_iceberg_datetime_day_mv` (`id`, `data`, `category`, `ts`)
PARTITION BY (`ts`)
DISTRIBUTED BY HASH(`id`)
REFRESH MANUAL
AS 
SELECT 
  `iceberg_sample_datetime_day`.`id`, 
  `iceberg_sample_datetime_day`.`data`, 
  `iceberg_sample_datetime_day`.`category`, 
  `iceberg_sample_datetime_day`.`ts`
FROM `iceberg`.`test`.`iceberg_sample_datetime_day`;
```

Hive catalog では、Hive メタデータキャッシュリフレッシュ機能を有効にして、StarRocks がパーティションレベルでのデータ変更を検出できるようにすることができます。この機能が有効になると、StarRocks は定期的に Hive Metastore Service (HMS) または AWS Glue にアクセスして、最近クエリされたホットデータのメタデータ情報を確認します。

Hive メタデータキャッシュリフレッシュ機能を有効にするには、次の FE 動的設定項目を [ADMIN SET FRONTEND CONFIG](../../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md) を使用して設定できます：

### 設定項目

#### enable_background_refresh_connector_metadata

**デフォルト**: v3.0 では true、v2.5 では false<br/>
**説明**: 定期的な Hive メタデータキャッシュリフレッシュを有効にするかどうか。これを有効にすると、StarRocks は Hive クラスターのメタストア（Hive Metastore または AWS Glue）をポーリングし、頻繁にアクセスされる Hive catalog のキャッシュされたメタデータをリフレッシュしてデータ変更を感知します。true は Hive メタデータキャッシュリフレッシュを有効にし、false は無効にします。<br/>

#### background_refresh_metadata_interval_millis

**デフォルト**: 600000 (10 分)<br/>
**説明**: 2 回の連続した Hive メタデータキャッシュリフレッシュの間隔。単位：ミリ秒。<br/>

#### background_refresh_metadata_time_secs_since_last_access_secs

**デフォルト**: 86400 (24 時間)<br/>
**説明**: Hive メタデータキャッシュリフレッシュタスクの有効期限。アクセスされた Hive catalog が指定された時間以上アクセスされていない場合、StarRocks はそのキャッシュされたメタデータのリフレッシュを停止します。アクセスされていない Hive catalog については、StarRocks はそのキャッシュされたメタデータをリフレッシュしません。単位：秒。

v3.1.4 以降、StarRocks は Iceberg Catalog のパーティションレベルでのデータ変更の検出をサポートしています。現在、Iceberg V1 テーブルのみがサポートされています。

### 外部カタログベースのマテリアライズドビューのクエリの書き換えを有効にする

デフォルトでは、StarRocks は Hudi および JDBC catalog に基づいて構築されたマテリアライズドビューのクエリの書き換えをサポートしていません。これは、このシナリオでのクエリの書き換えが結果の強い整合性を保証できないためです。この機能を有効にするには、マテリアライズドビューを作成する際にプロパティ `force_external_table_query_rewrite` を `true` に設定します。Hive catalog のテーブルに基づいて構築されたマテリアライズドビューでは、クエリの書き換えはデフォルトで有効になっています。

例：

```SQL
CREATE MATERIALIZED VIEW ex_mv_par_tbl
PARTITION BY emp_date
DISTRIBUTED BY hash(empid)
PROPERTIES (
"force_external_table_query_rewrite" = "true"
) 
AS
select empid, deptno, emp_date
from `hive_catalog`.`emp_db`.`emps_par_tbl`
where empid < 5;
```

クエリの書き換えを含むシナリオでは、非常に複雑なクエリステートメントを使用してマテリアライズドビューを構築する場合、クエリステートメントを分割して、ネストされた形で複数のシンプルなマテリアライズドビューを構築することをお勧めします。ネストされたマテリアライズドビューはより汎用性があり、より広範なクエリパターンに対応できます。

## ベストプラクティス

実際のビジネスシナリオでは、監査ログや大規模クエリログを分析することで、実行レイテンシーが高くリソース消費が多いクエリを特定できます。さらに、[query profiles](../../../best_practices/query_tuning/query_profile_overview.md) を使用して、クエリが遅い特定のステージを特定することができます。以下のセクションでは、マテリアライズドビューを使用してデータレイククエリのパフォーマンスを向上させる方法についての指示と例を提供します。

### ケース1: データレイクでのジョイン計算の加速

データレイクでのジョインクエリを加速するために、マテリアライズドビューを使用できます。

Hive catalog 上の次のクエリが特に遅いと仮定します：

```SQL
--Q1
SELECT SUM(lo_extendedprice * lo_discount) AS REVENUE
FROM hive.ssb_1g_csv.lineorder, hive.ssb_1g_csv.dates
WHERE
    lo_orderdate = d_datekey
    AND d_year = 1993
    AND lo_discount BETWEEN 1 AND 3
    AND lo_quantity < 25;

--Q2
SELECT SUM(lo_extendedprice * lo_discount) AS REVENUE
FROM hive.ssb_1g_csv.lineorder, hive.ssb_1g_csv.dates
WHERE
    lo_orderdate = d_datekey
    AND d_yearmonth = 'Jan1994'
    AND lo_discount BETWEEN 4 AND 6
    AND lo_quantity BETWEEN 26 AND 35;

--Q3 
SELECT SUM(lo_revenue), d_year, p_brand
FROM hive.ssb_1g_csv.lineorder, hive.ssb_1g_csv.dates, hive.ssb_1g_csv.part, hive.ssb_1g_csv.supplier
WHERE
    lo_orderdate = d_datekey
    AND lo_partkey = p_partkey
    AND lo_suppkey = s_suppkey
    AND p_brand BETWEEN 'MFGR#2221' AND 'MFGR#2228'
    AND s_region = 'ASIA'
GROUP BY d_year, p_brand
ORDER BY d_year, p_brand;
```

これらのクエリプロファイルを分析することで、クエリの実行時間が主に `lineorder` テーブルと他のディメンジョンテーブルの `lo_orderdate` 列でのハッシュジョインに費やされていることに気付くかもしれません。

ここで、Q1 と Q2 は `lineorder` と `dates` をジョインした後に集計を行い、Q3 は `lineorder`、`dates`、`part`、`supplier` をジョインした後に集計を行います。

したがって、StarRocks の [View Delta Join rewrite](query_rewrite_with_materialized_views.md#view-delta-join-rewrite) 機能を利用して、`lineorder`、`dates`、`part`、`supplier` をジョインするマテリアライズドビューを構築できます。

```SQL
CREATE MATERIALIZED VIEW lineorder_flat_mv
DISTRIBUTED BY HASH(LO_ORDERDATE, LO_ORDERKEY) BUCKETS 48
PARTITION BY LO_ORDERDATE
REFRESH ASYNC EVERY(INTERVAL 1 DAY) 
PROPERTIES ( 
    -- 一意制約を指定します。
    "unique_constraints" = "
    hive.ssb_1g_csv.supplier.s_suppkey;
    hive.ssb_1g_csv.part.p_partkey;
    hive.ssb_1g_csv.dates.d_datekey",
    -- 外部キーを指定します。
    "foreign_key_constraints" = "
    hive.ssb_1g_csv.lineorder(lo_partkey) REFERENCES hive.ssb_1g_csv.part(p_partkey);
    hive.ssb_1g_csv.lineorder(lo_suppkey) REFERENCES hive.ssb_1g_csv.supplier(s_suppkey);
    hive.ssb_1g_csv.lineorder(lo_orderdate) REFERENCES hive.ssb_1g_csv.dates(d_datekey)",
    -- 外部カタログベースのマテリアライズドビューのクエリの書き換えを有効にします。
    "force_external_table_query_rewrite" = "TRUE"
)
AS SELECT
       l.LO_ORDERDATE AS LO_ORDERDATE,
       l.LO_ORDERKEY AS LO_ORDERKEY,
       l.LO_PARTKEY AS LO_PARTKEY,
       l.LO_SUPPKEY AS LO_SUPPKEY,
       l.LO_QUANTITY AS LO_QUANTITY,
       l.LO_EXTENDEDPRICE AS LO_EXTENDEDPRICE,
       l.LO_DISCOUNT AS LO_DISCOUNT,
       l.LO_REVENUE AS LO_REVENUE,
       s.S_REGION AS S_REGION,
       p.P_BRAND AS P_BRAND,
       d.D_YEAR AS D_YEAR,
       d.D_YEARMONTH AS D_YEARMONTH
   FROM hive.ssb_1g_csv.lineorder AS l
            INNER JOIN hive.ssb_1g_csv.supplier AS s ON s.S_SUPPKEY = l.LO_SUPPKEY
            INNER JOIN hive.ssb_1g_csv.part AS p ON p.P_PARTKEY = l.LO_PARTKEY
            INNER JOIN hive.ssb_1g_csv.dates AS d ON l.LO_ORDERDATE = d.D_DATEKEY;
```

### ケース2: データレイクでの集計とジョインの集計の加速

マテリアライズドビューは、単一テーブルまたは複数テーブルを含む集計クエリを加速するために使用できます。

- 単一テーブルの集計クエリ

  単一テーブルの典型的なクエリでは、そのクエリプロファイルが AGGREGATE ノードが多くの時間を消費していることを示します。一般的な集計演算子を使用してマテリアライズドビューを構築できます。

  次のクエリが遅いと仮定します：

  ```SQL
  --Q4
  SELECT
  lo_orderdate, count(distinct lo_orderkey)
  FROM hive.ssb_1g_csv.lineorder
  GROUP BY lo_orderdate
  ORDER BY lo_orderdate limit 100;
  ```

  Q4 は、日ごとのユニークな注文数を計算します。count distinct 計算は計算コストが高いため、次の2種類のマテリアライズドビューを作成して加速できます：

  ```SQL
  CREATE MATERIALIZED VIEW mv_2_1 
  DISTRIBUTED BY HASH(lo_orderdate)
  PARTITION BY LO_ORDERDATE
  REFRESH ASYNC EVERY(INTERVAL 1 DAY) 
  AS 
  SELECT
  lo_orderdate, count(distinct lo_orderkey)
  FROM hive.ssb_1g_csv.lineorder
  GROUP BY lo_orderdate;
  
  CREATE MATERIALIZED VIEW mv_2_2 
  DISTRIBUTED BY HASH(lo_orderdate)
  PARTITION BY LO_ORDERDATE
  REFRESH ASYNC EVERY(INTERVAL 1 DAY) 
  AS 
  SELECT
  -- lo_orderkey はクエリの書き換えに使用できるように BIGINT 型である必要があります。
  lo_orderdate, bitmap_union(to_bitmap(lo_orderkey))
  FROM hive.ssb_1g_csv.lineorder
  GROUP BY lo_orderdate;
  ```

  このコンテキストでは、書き換えの失敗を避けるために、LIMIT および ORDER BY 句を含むマテリアライズドビューを作成しないでください。クエリの書き換えの制限についての詳細は、[Query rewrite with materialized views - Limitations](query_rewrite_with_materialized_views.md#limitations) を参照してください。

- 複数テーブルの集計クエリ

  ジョイン結果の集計を含むシナリオでは、既存のマテリアライズドビューに基づいてネストされたマテリアライズドビューを作成し、ジョイン結果をさらに集計できます。たとえば、ケース1の例に基づいて、Q1 と Q2 の集計パターンが似ているため、次のマテリアライズドビューを作成して加速できます：

  ```SQL
  CREATE MATERIALIZED VIEW mv_2_3
  DISTRIBUTED BY HASH(lo_orderdate)
  PARTITION BY LO_ORDERDATE
  REFRESH ASYNC EVERY(INTERVAL 1 DAY) 
  AS 
  SELECT
  lo_orderdate, lo_discount, lo_quantity, d_year, d_yearmonth, SUM(lo_extendedprice * lo_discount) AS REVENUE
  FROM lineorder_flat_mv
  GROUP BY lo_orderdate, lo_discount, lo_quantity, d_year, d_yearmonth;
  ```

  もちろん、単一のマテリアライズドビュー内でジョインと集計計算の両方を実行することも可能です。これらのタイプのマテリアライズドビューは、特定の計算のためにクエリの書き換えの機会が少ないかもしれませんが、集計後のストレージスペースを通常より少なく占有します。具体的なユースケースに基づいて選択できます。

  ```SQL
  CREATE MATERIALIZED VIEW mv_2_4
  DISTRIBUTED BY HASH(lo_orderdate)
  PARTITION BY LO_ORDERDATE
  REFRESH ASYNC EVERY(INTERVAL 1 DAY) 
  PROPERTIES (
      "force_external_table_query_rewrite" = "TRUE"
  )
  AS
  SELECT lo_orderdate, lo_discount, lo_quantity, d_year, d_yearmonth, SUM(lo_extendedprice * lo_discount) AS REVENUE
  FROM hive.ssb_1g_csv.lineorder, hive.ssb_1g_csv.dates
  WHERE lo_orderdate = d_datekey
  GROUP BY lo_orderdate, lo_discount, lo_quantity, d_year, d_yearmonth;
  ```

### ケース3: データレイクでの集計に対するジョインの加速

一部のシナリオでは、最初に1つのテーブルで集計計算を行い、その後他のテーブルとジョインクエリを実行する必要があるかもしれません。StarRocks のクエリの書き換え機能を最大限に活用するために、ネストされたマテリアライズドビューを構築することをお勧めします。たとえば：

```SQL
--Q5
SELECT * FROM  (
    SELECT 
      l.lo_orderkey, l.lo_orderdate, c.c_custkey, c_region, sum(l.lo_revenue)
    FROM 
      hive.ssb_1g_csv.lineorder l 
      INNER JOIN (
        SELECT distinct c_custkey, c_region 
        from 
          hive.ssb_1g_csv.customer 
        WHERE 
          c_region IN ('ASIA', 'AMERICA') 
      ) c ON l.lo_custkey = c.c_custkey
      GROUP BY  l.lo_orderkey, l.lo_orderdate, c.c_custkey, c_region
  ) c1 
WHERE 
  lo_orderdate = '19970503';
```

Q5 は最初に `customer` テーブルで集計クエリを実行し、その後 `lineorder` テーブルとジョインおよび集計を行います。類似のクエリは、`c_region` および `lo_orderdate` に対する異なるフィルタを含むかもしれません。クエリの書き換え機能を活用するために、集計用の1つのマテリアライズドビューとジョイン用のもう1つのマテリアライズドビューを作成できます。

```SQL
--mv_3_1
CREATE MATERIALIZED VIEW mv_3_1
DISTRIBUTED BY HASH(c_custkey)
REFRESH ASYNC EVERY(INTERVAL 1 DAY) 
PROPERTIES (
    "force_external_table_query_rewrite" = "TRUE"
)
AS
SELECT distinct c_custkey, c_region from hive.ssb_1g_csv.customer; 

--mv_3_2
CREATE MATERIALIZED VIEW mv_3_2
DISTRIBUTED BY HASH(lo_orderdate)
PARTITION BY LO_ORDERDATE
REFRESH ASYNC EVERY(INTERVAL 1 DAY) 
PROPERTIES (
    "force_external_table_query_rewrite" = "TRUE"
)
AS
SELECT l.lo_orderdate, l.lo_orderkey, mv.c_custkey, mv.c_region, sum(l.lo_revenue)
FROM hive.ssb_1g_csv.lineorder l 
INNER JOIN mv_3_1 mv
ON l.lo_custkey = mv.c_custkey
GROUP BY l.lo_orderkey, l.lo_orderdate, mv.c_custkey, mv.c_region;
```

### ケース4: データレイクでのリアルタイムデータと履歴データのホットデータとコールドデータの分離

次のシナリオを考えてみましょう：過去3日以内に更新されたデータは直接 StarRocks に書き込まれ、より古いデータは確認されて Hive にバッチ書き込みされます。しかし、過去7日間のデータを含むクエリがまだ存在するかもしれません。この場合、マテリアライズドビューを使用してデータを自動的に期限切れにするシンプルなモデルを作成できます。

```SQL
CREATE MATERIALIZED VIEW mv_4_1 
DISTRIBUTED BY HASH(lo_orderdate)
PARTITION BY LO_ORDERDATE
REFRESH ASYNC EVERY(INTERVAL 1 DAY) 
AS 
SELECT lo_orderkey, lo_orderdate, lo_revenue
FROM hive.ssb_1g_csv.lineorder
WHERE lo_orderdate<=current_date()
AND lo_orderdate>=date_add(current_date(), INTERVAL -4 DAY);
```

上位層のアプリケーションのロジックに基づいて、さらにビューまたはマテリアライズドビューを構築することができます。
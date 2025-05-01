---
displayed_sidebar: docs
---

# マテリアライズドビューによるデータレイククエリアクセラレーション

このトピックでは、StarRocks の非同期マテリアライズドビューを使用してデータレイク内のクエリパフォーマンスを最適化する方法について説明します。

StarRocks は、データレイク内の探索的クエリやデータ分析に非常に効果的な、すぐに使えるデータレイククエリ機能を提供します。ほとんどのシナリオでは、[Data Cache](../data_source/data_cache.md) がブロックレベルのファイルキャッシングを提供し、リモートストレージのジッターや大量の I/O 操作によるパフォーマンス低下を回避します。

しかし、データレイクからのデータを使用して複雑で効率的なレポートを作成したり、これらのクエリをさらに加速したりする際には、パフォーマンスの課題に直面することがあります。非同期マテリアライズドビューを使用することで、レポートやデータアプリケーションのクエリパフォーマンスを向上させ、高い同時実行性を実現できます。

## 概要

StarRocks は、Hive catalog、Iceberg catalog、Hudi catalog などの external catalog に基づいて非同期マテリアライズドビューを構築することをサポートしています。external catalog ベースのマテリアライズドビューは、以下のシナリオで特に有用です：

- **データレイクレポートの透過的なアクセラレーション**

  データレイクレポートのクエリパフォーマンスを確保するために、データエンジニアは通常、データアナリストと密接に連携して、レポートのアクセラレーション層の構築ロジックを探る必要があります。アクセラレーション層がさらに更新を必要とする場合、構築ロジック、処理スケジュール、およびクエリステートメントを適宜更新する必要があります。

  マテリアライズドビューのクエリの書き換え機能を通じて、レポートのアクセラレーションをユーザーに対して透明かつ知覚されないものにすることができます。遅いクエリが特定された場合、データエンジニアは遅いクエリのパターンを分析し、必要に応じてマテリアライズドビューを作成します。アプリケーション側のクエリは、マテリアライズドビューによってインテリジェントに書き換えられ、透明に加速されるため、ビジネスアプリケーションやクエリステートメントのロジックを変更することなく、クエリパフォーマンスを迅速に向上させることができます。

- **履歴データと関連するリアルタイムデータのインクリメンタル計算**

  ビジネスアプリケーションが StarRocks の内部テーブルにあるリアルタイムデータとデータレイクにある履歴データの関連付けを必要とする場合、マテリアライズドビューは簡単な解決策を提供します。たとえば、リアルタイムのファクトテーブルが StarRocks の内部テーブルであり、ディメンジョンテーブルがデータレイクに保存されている場合、内部テーブルと外部データソースのテーブルを関連付けるマテリアライズドビューを構築することで、インクリメンタル計算を簡単に実行できます。

- **メトリック層の迅速な構築**

  高次元のデータを扱う際に、メトリックの計算と処理が課題となることがあります。マテリアライズドビューを使用することで、データの事前集計とロールアップを行い、比較的軽量なメトリック層を作成できます。さらに、マテリアライズドビューは自動的にリフレッシュされ、メトリック計算の複雑さをさらに軽減します。

マテリアライズドビュー、Data Cache、および StarRocks の内部テーブルはすべて、クエリパフォーマンスを大幅に向上させる効果的な方法です。以下の表は、それらの主な違いを比較しています：

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
      <td><ul><li>ブロックレベルのデータキャッシングをサポート</li><li>LRU キャッシュエビクションメカニズムに従う</li><li>計算結果はキャッシュされない</li></ul></td>
      <td>事前計算されたクエリ結果を保存</td>
      <td>テーブルスキーマに基づいてデータを保存</td>
    </tr>
    <tr>
      <td><b>クエリパフォーマンス</b></td>
      <td colspan="3" style={{textAlign: 'center'}} >Data Cache &le; Materialized view = Native table</td>
    </tr>
    <tr>
      <td><b>クエリステートメント</b></td>
      <td><ul><li>データレイクに対するクエリステートメントを変更する必要はありません</li><li>クエリがキャッシュにヒットすると、計算が行われます。</li></ul></td>
      <td><ul><li>データレイクに対するクエリステートメントを変更する必要はありません</li><li>クエリの書き換えを利用して事前計算された結果を再利用</li></ul></td>
      <td>内部テーブルをクエリするためにクエリステートメントの変更が必要です</td>
    </tr>
  </tbody>
</table>

<br />

データレイクデータを直接クエリするか、内部テーブルにデータをロードするのと比較して、マテリアライズドビューはいくつかのユニークな利点を提供します：

- **ローカルストレージによるアクセラレーション**: マテリアライズドビューは、StarRocks のローカルストレージによるアクセラレーションの利点を活用できます。たとえば、インデックス、パーティショニング、バケッティング、コロケートグループなどにより、データレイクから直接データをクエリするよりも優れたクエリパフォーマンスを実現します。
- **ロードタスクのゼロメンテナンス**: マテリアライズドビューは自動リフレッシュタスクを通じてデータを透明に更新します。スケジュールされたデータ更新を実行するためにロードタスクを維持する必要はありません。さらに、Hive catalog ベースのマテリアライズドビューはデータの変更を検出し、パーティションレベルでインクリメンタルリフレッシュを実行できます。
- **インテリジェントなクエリの書き換え**: クエリはマテリアライズドビューを使用するように透明に書き換えられます。クエリステートメントを変更することなく、即座にアクセラレーションの恩恵を受けることができます。

<br />

したがって、次のシナリオではマテリアライズドビューの使用をお勧めします：

- Data Cache が有効になっていても、クエリのレイテンシーや同時実行性に関する要件を満たしていない場合。
- クエリが再利用可能なコンポーネント、たとえば固定された集計関数やジョインパターンを含む場合。
- データがパーティションに整理されており、クエリが比較的高いレベルでの集計を含む場合（例：日単位の集計）。

<br />

次のシナリオでは、Data Cache を使用したアクセラレーションを優先することをお勧めします：

- クエリに再利用可能なコンポーネントがあまり含まれておらず、データレイクから任意のデータをスキャンする可能性がある場合。
- リモートストレージに大きな変動や不安定性があり、アクセスに影響を与える可能性がある場合。

## external catalog ベースのマテリアライズドビューの作成

external catalog のテーブルにマテリアライズドビューを作成することは、StarRocks の内部テーブルにマテリアライズドビューを作成することと似ています。使用しているデータソースに応じて適切なリフレッシュ戦略を設定し、external catalog ベースのマテリアライズドビューのクエリの書き換えを手動で有効にするだけです。

### 適切なリフレッシュ戦略を選択する

現在、StarRocks は Hudi catalog、Iceberg catalog、および JDBC catalog におけるパーティションレベルのデータ変更を検出することができません。そのため、タスクがトリガーされるとフルサイズのリフレッシュが実行されます。

Hive catalog では、Hive メタデータキャッシュリフレッシュ機能を有効にして、StarRocks がパーティションレベルでデータ変更を検出できるようにすることができます。この機能を有効にするには、マテリアライズドビューのパーティションキーがベーステーブルのパーティションキーに含まれている必要があります。この機能が有効になると、StarRocks は定期的に Hive Metastore Service (HMS) または AWS Glue にアクセスし、最近クエリされたホットデータのメタデータ情報を確認します。その結果、StarRocks は以下を行うことができます：

- データ変更のあるパーティションのみをリフレッシュし、フルサイズのリフレッシュを回避し、リフレッシュによるリソース消費を削減します。

- クエリの書き換え中にある程度のデータ整合性を確保します。データレイクのベーステーブルにデータ変更がある場合、クエリはマテリアライズドビューを使用するように書き換えられません。

  > **注意**
  >
  > マテリアライズドビューを作成する際に `mv_rewrite_staleness_second` プロパティを設定することで、ある程度のデータ不整合を許容することもできます。詳細については、[CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW.md) を参照してください。

Hive メタデータキャッシュリフレッシュ機能を有効にするには、[ADMIN SET FRONTEND CONFIG](../sql-reference/sql-statements/Administration/ADMIN_SET_CONFIG.md) を使用して次の FE 動的設定項目を設定できます：

| **設定項目**                                       | **デフォルト**                | **説明**                                              |
| ------------------------------------------------------------ | -------------------------- | ------------------------------------------------------------ |
| enable_background_refresh_connector_metadata                 | v3.0 では true、v2.5 では false | 定期的な Hive メタデータキャッシュリフレッシュを有効にするかどうか。これを有効にすると、StarRocks は Hive クラスターのメタストア (Hive Metastore または AWS Glue) をポーリングし、頻繁にアクセスされる Hive catalog のキャッシュされたメタデータをリフレッシュしてデータ変更を認識します。true は Hive メタデータキャッシュリフレッシュを有効にし、false は無効にします。 |
| background_refresh_metadata_interval_millis                  | 600000 (10 分)        | 2 回の連続した Hive メタデータキャッシュリフレッシュの間隔。単位：ミリ秒。 |
| background_refresh_metadata_time_secs_since_last_access_secs | 86400 (24 時間)           | Hive メタデータキャッシュリフレッシュタスクの有効期限。アクセスされた Hive catalog に対して、指定された時間を超えてアクセスされていない場合、StarRocks はそのキャッシュされたメタデータのリフレッシュを停止します。アクセスされていない Hive catalog に対して、StarRocks はそのキャッシュされたメタデータをリフレッシュしません。単位：秒。 |

### external catalog ベースのマテリアライズドビューのクエリの書き換えを有効にする

デフォルトでは、StarRocks は Hudi、Iceberg、および JDBC catalog に基づいて構築されたマテリアライズドビューのクエリの書き換えをサポートしていません。これは、このシナリオでのクエリの書き換えが結果の強い整合性を保証できないためです。この機能を有効にするには、マテリアライズドビューを作成する際に `force_external_table_query_rewrite` プロパティを `true` に設定します。Hive catalog のテーブルに基づいて構築されたマテリアライズドビューでは、クエリの書き換えはデフォルトで有効です。

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

クエリの書き換えを含むシナリオでは、非常に複雑なクエリステートメントを使用してマテリアライズドビューを構築する場合、クエリステートメントを分割し、複数のシンプルなマテリアライズドビューをネストして構築することをお勧めします。ネストされたマテリアライズドビューはより汎用性があり、より広範なクエリパターンに対応できます。

## ベストプラクティス

実際のビジネスシナリオでは、監査ログを分析することで実行レイテンシーが高くリソース消費が多いクエリを特定できます。さらに、[query profiles](../administration/query_profile.md) を使用して、クエリが遅い特定のステージを特定できます。以下のセクションでは、マテリアライズドビューを使用してデータレイククエリパフォーマンスを向上させる方法と例を提供します。

### ケース 1: データレイクでのジョイン計算の加速

マテリアライズドビューを使用して、データレイクでのジョインクエリを加速できます。

次のような Hive catalog 上のクエリが特に遅いとします：

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

これらのクエリプロファイルを分析することで、クエリの実行時間が主に `lineorder` テーブルと他のディメンジョンテーブルとの `lo_orderdate` 列でのハッシュジョインに費やされていることがわかるかもしれません。

ここで、Q1 と Q2 は `lineorder` と `dates` をジョインした後に集計を行い、Q3 は `lineorder`、`dates`、`part`、および `supplier` をジョインした後に集計を行います。

したがって、StarRocks の [View Delta Join rewrite](./query_rewrite_with_materialized_views.md#view-delta-join-rewrite) 機能を利用して、`lineorder`、`dates`、`part`、および `supplier` をジョインするマテリアライズドビューを構築できます。

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
    -- external catalog ベースのマテリアライズドビューのクエリの書き換えを有効にします。
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

### ケース 2: データレイクでの集計とジョインの集計の加速

マテリアライズドビューは、単一テーブルまたは複数テーブルを含む集計クエリを加速するために使用できます。

- 単一テーブルの集計クエリ

  単一テーブルに対する典型的なクエリでは、クエリプロファイルが AGGREGATE ノードが多くの時間を消費していることを示します。一般的な集計演算子を使用してマテリアライズドビューを構築できます。

  次のような遅いクエリがあるとします：

  ```SQL
  --Q4
  SELECT
  lo_orderdate, count(distinct lo_orderkey)
  FROM hive.ssb_1g_csv.lineorder
  GROUP BY lo_orderdate
  ORDER BY lo_orderdate limit 100;
  ```

  Q4 はユニークな注文の毎日のカウントを計算します。count distinct 計算は計算コストが高い可能性があるため、次の 2 種類のマテリアライズドビューを作成して加速できます：

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

  このコンテキストでは、書き換えの失敗を避けるために LIMIT および ORDER BY 句を含むマテリアライズドビューを作成しないでください。クエリの書き換えの制限に関する詳細は、[Query rewrite with materialized views - Limitations](./query_rewrite_with_materialized_views.md#limitations) を参照してください。

- 複数テーブルの集計クエリ

  ジョイン結果の集計を含むシナリオでは、既存のマテリアライズドビューに基づいてテーブルをジョインし、ジョイン結果をさらに集計するネストされたマテリアライズドビューを作成できます。たとえば、ケース 1 の例に基づいて、Q1 および Q2 の集計パターンが似ているため、次のマテリアライズドビューを作成して加速できます：

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

  もちろん、単一のマテリアライズドビュー内でジョインと集計計算の両方を実行することも可能です。これらのタイプのマテリアライズドビューは、特定の計算のためにクエリの書き換えの機会が少ないかもしれませんが、集計後のストレージスペースを通常は少なく占有します。具体的なユースケースに基づいて選択できます。

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

### ケース 3: データレイクでの集計に基づくジョインの加速

一部のシナリオでは、最初に 1 つのテーブルで集計計算を行い、その後他のテーブルとのジョインクエリを実行する必要があるかもしれません。StarRocks のクエリの書き換え機能を最大限に活用するために、ネストされたマテリアライズドビューを構築することをお勧めします。たとえば：

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

Q5 は最初に `customer` テーブルで集計クエリを実行し、その後 `lineorder` テーブルとのジョインと集計を行います。類似のクエリは、`c_region` と `lo_orderdate` に対する異なるフィルタを含むかもしれません。クエリの書き換え機能を活用するために、1 つは集計用、もう 1 つはジョイン用の 2 つのマテリアライズドビューを作成できます。

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

### ケース 4: データレイクでのリアルタイムデータと履歴データのホットデータとコールドデータの分離

次のシナリオを考えてみましょう：過去 3 日以内に更新されたデータは直接 StarRocks に書き込まれ、より古いデータはチェックされて Hive にバッチ書き込みされます。しかし、過去 7 日間のデータを含むクエリがまだ存在するかもしれません。この場合、マテリアライズドビューを使用してデータを自動的に期限切れにするシンプルなモデルを作成できます。

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

上層アプリケーションのロジックに基づいて、これに基づいてビューまたはマテリアライズドビューをさらに構築できます。
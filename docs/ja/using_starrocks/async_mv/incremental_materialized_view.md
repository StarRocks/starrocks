---
displayed_sidebar: docs
description: "ネイティブテーブル上の増分マテリアライズドビューを作成、使用、リフレッシュ、監視する方法。差分サイズのリフレッシュコストで新鮮な結果を得られます。"
sidebar_position: 2
keywords: ['zengliang', 'wuhua', 'IMV', 'zengliangshuaxin']
---

# 増分マテリアライズドビュー

シェアードデータクラスターにおいて、増分マテリアライズドビュー（IMV）の作成、使用、リフレッシュ、監視、およびトラブルシューティングを行います。

増分マテリアライズドビューは、非同期マテリアライズドビューの**厳密な増分リフレッシュモード**です。最初のリフレッシュで完全なベースラインを確立し、それ以降の各リフレッシュでは、前回の成功したリフレッシュ以降の変更のみを消費して、その結果をマテリアライズドビューにマージします。変更やクエリの形式を安全に増分的に維持できない場合、システムは暗黙的に完全な再計算にフォールバックするのではなく、明示的にエラーを返します。

:::note

- 増分マテリアライズドビューは、シェアードデータクラスターのクラウドネイティブテーブルおよび Iceberg の追記専用（append-only）外部テーブルでのみサポートされます。シェアードナッシングクラスターはサポートされません。
- 増分マテリアライズドビューは自動クエリ書き換えの対象になりません。マテリアライズドビューを直接クエリする必要があります。
- Incremental View Maintenance（IVM）はソースコードやエラーメッセージ内で使用される内部名であり、IMV はシステムテーブルのフィールドや製品ドキュメントで使用されます。どちらも同じ機能を指します。
:::

## 背景

### 増分マテリアライズドビューが必要な理由

非同期マテリアライズドビューは、デフォルトで PCT（Partition Change Tracking、パーティション変更追跡）リフレッシュを使用します。ベーステーブルのパーティション内のデータが変更されると、そのパーティション全体が再計算されます。

これは構造的な問題を生み出します。**リフレッシュコストは、変更されたデータ量ではなく、影響を受けるパーティションの総データ量に比例する**のです。例えば、日次パーティションに3,000万行が含まれている場合、わずか1万行を追加するだけでも、3,000万行すべてを再計算する必要があります。履歴データが蓄積するにつれて、リフレッシュ時間は増加し続けます。最終的には、コストを抑えるためにリフレッシュ頻度を下げざるを得なくなり、その代償としてデータの鮮度が犠牲になります。

増分マテリアライズドビューは、このカーブを平坦化します。**リフレッシュコストは、現在のバッチで変更されたデータ量のみに依存し、履歴データの総量とは切り離されます**。

増分マテリアライズドビューは、特に以下のシナリオで有用です。

- データ量の増加に伴ってリフレッシュコストが増加し続ける既存の非同期マテリアライズドビュー。
- 鮮度要件が厳しいダッシュボード。
- ベーステーブルが主に追記専用（append-only）である、大規模な明細テーブル上の運用、広告、トランザクション、または IoT の集計レポート。

### 他のマテリアライズドビューとの違い

|                                | 単一テーブル集計                | マルチテーブルジョイン | クエリの書き換え | リフレッシュコスト                           | リフレッシュ戦略                                              | ベーステーブル                                                |
| ------------------------------ | -------------------------------- | ---------------------- | ----------------- | -------------------------------------------- | -------------------------------------------------------------- | -------------------------------------------------------------- |
| 増分マテリアライズドビュー      | サポート（制限あり）              | INNER/CROSS JOIN のみ  | 非サポート        | 現在のバッチの変更量に比例                    | <ul><li>ベーステーブルの変更によるトリガー</li><li>スケジュールリフレッシュ</li><li>手動リフレッシュ</li></ul> | シェアードデータのネイティブテーブル（DUPLICATE / PRIMARY / AGGREGATE KEY）、Iceberg の追記専用テーブル |
| 非同期マテリアライズドビュー    | サポート                          | サポート                | サポート          | 影響を受けるパーティションの総データ量に比例  | <ul><li>スケジュールリフレッシュ</li><li>手動リフレッシュ</li></ul>   | マルチテーブル定義をサポートし、Default Catalog、External Catalog、既存のマテリアライズドビュー、既存のビューに基づいて作成可能 |
| 同期マテリアライズドビュー      | 一部の集計関数のみ                | 非サポート              | サポート          | データ取り込みと同期してリフレッシュ          | 取り込み中の同期リフレッシュ                                    | Default Catalog のみに基づく単一テーブル定義                    |

### 関連する概念

- **Change Data Capture (CDC)**: シェアードデータテーブルのタブレットメタデータから行レベルの変更を導出する仕組みです。DUPLICATE KEY テーブルおよび AGGREGATE KEY テーブルは、追加の設定や書き込みオーバーヘッドなしにネイティブに変更を導出できます。PRIMARY KEY テーブルは、変更箇所を特定するために必要な軽量なメタデータを各取り込み操作中に記録するため、テーブルプロパティ `enable_change_data_capture = 'true'` を明示的に設定する必要があります。

- **デルタウィンドウ（Delta window）**: 1回の増分リフレッシュで消費される変更の範囲を表し、`(前回のリフレッシュ位置, 現在の位置]` として表現されます。ネイティブテーブルの場合、位置は**ブックマーク ID** であり、Iceberg テーブルの場合はスナップショット ID です。デルタウィンドウは連続しており、隙間や重複はありません。

- **ブックマーク（Bookmark）**: マテリアライズドビューがネイティブテーブルに対して保持するバージョン参照です。増分リフレッシュが一貫した過去のバージョンから読み取ることを保証します。トレードオフとして、**参照されなくなるまで古いベーステーブルファイルを回収できません**。[コストとオーバーヘッド](#costs-and-overheads)を参照してください。

- **正味変更（Net change）**: 同じ行が1つのウィンドウ内で複数回変更された場合、「ウィンドウ開始時点の値の削除 + ウィンドウ終了時点の値の挿入」というペアのみが保持されます。同じウィンドウ内で作成されて削除された行は、まったく現れません。増分リフレッシュは**常に正味変更を消費します**。

- **取り消し（Retraction）**: ベーステーブルに対する `DELETE` / `UPDATE` 操作をマテリアライズドビューに反映することです。マテリアライズドビューは常に PRIMARY KEY テーブルであるため、対応する行を UPSERT / DELETE のセマンティクスを使用して正確に更新できます。

### 主な制限事項

厳密な増分セマンティクスを保証するため、増分マテリアライズドビューにはいくつかの領域で制限があります。この機能を採用する前に、ソリューション設計の段階でこれらの制限を確認してください。完全なリストについては、[Capability Boundaries](#capability-boundaries) を参照してください。

- **クエリの書き換えに参加しない**

  クエリの書き換えは、書き換え関連の設定に関わらず無効化されており、スイッチで有効にすることはできません。既存のビジネス SQL を変更せずに高速化したい場合は、移行コストを評価してください。すべての利用側が明示的にマテリアライズドビューをクエリする必要があります。このコストが高すぎる場合は、非同期マテリアライズドビュー（PCT）を引き続き使用してください。

- **シェアードデータクラスターでのみサポート**

  シェアードナッシングクラスターはサポートされておらず、回避策もありません。シェアードナッシングクラスターでのリアルタイムレポートの高速化には、別のクラスターアーキテクチャを計画するか、非同期マテリアライズドビュー（PCT）を引き続き使用する必要があります。

- **パーティションレベルまたはテーブルレベルの破壊的操作は増分チェーンを恒久的に破壊する**

  `INSERT OVERWRITE`、`DROP PARTITION`、`TRUNCATE` はリフレッシュの失敗を引き起こし、マテリアライズドビューを非アクティブ化します。マテリアライズドビューはドロップして再作成する必要があります。ベーステーブルに保持期間ベースのスケジュールされた `DROP PARTITION` ジョブがある場合や、バックフィルに `INSERT OVERWRITE` を使用している場合は、影響を受けるパーティション範囲を分離するなど、事前に互換性のあるワークフローを設計するか、定期的なマテリアライズドビューの再構築を受け入れる必要があります。**CDC が有効な PRIMARY KEY ベーステーブルに対する行レベルの `DELETE` / `UPDATE` は正常にサポートされ**、この制限の影響を受けません。

- **限定的な SQL サポート**

  OUTER / SEMI / ANTI JOIN、ウィンドウ関数、CTE、多階層のネストされたサブクエリ、`LIMIT`、集計を含む `HAVING`、`GROUPING SETS` / `ROLLUP` / `CUBE`、`GROUP BY` のないグローバル集計、およびその他のサポートされていない演算子は拒否されます。許可リストに登録されている集計関数はわずか13個です。レポートで `count(distinct)`、`stddev`、`variance`、`percentile_approx`、`min_by` などの関数を使用している場合は、クエリを書き換えてください。正確な重複排除カウントには `bitmap_union(to_bitmap(col))` を、概算の重複排除カウントには `hll_union(hll_hash(col))` を使用します。パーセンタイルや標準偏差などのコストの高い統計は、通常、生テーブルに対するドリルダウンクエリに残すべきです。サポートされていないクエリは、引き続き非同期マテリアライズドビュー（PCT）を使用してください。

- **制限されたベーステーブルタイプ**

  UNIQUE KEY テーブルはサポートされていません。AGGREGATE KEY テーブルは厳密なロールアップのみをサポートします。マテリアライズドビューをベーステーブルとして使用すること（MV on MV）はできません。UNIQUE KEY テーブルの高速化には、PRIMARY KEY モデルへの移行を検討してください。既存の多階層マテリアライズドビューチェーンは、各マテリアライズドビューがベーステーブルを直接参照するようにフラット化する必要があります。

- **PCT と INCREMENTAL をその場で切り替えることはできない**

  `refresh_mode` は `ALTER` を使用してモード間で変更することはできません。「まず低コストで試してみて、後で元に戻す」ことはできません。本番ワークロードを変更する前に、テスト環境またはシャドウマテリアライズドビューで設計を検証してください。

## 増分マテリアライズドビューを作成する

### 構文

増分マテリアライズドビューを作成するには、`PROPERTIES` 内で `refresh_mode` を `INCREMENTAL` に指定します。

```SQL
CREATE MATERIALIZED VIEW <mv_name>
[PARTITION BY <expr>]
[DISTRIBUTED BY HASH(<col>) [BUCKETS <n>]]
REFRESH [IMMEDIATE | DEFERRED]
    { ASYNC
    | { ASYNC | SCHEDULE } [START('<start_time>')] EVERY(INTERVAL <n> <unit>)
    | MANUAL }
PROPERTIES ("refresh_mode" = "INCREMENTAL")
AS <query>;
```

:::note

- `refresh_mode` に指定できる有効な値は `PCT`（デフォルト）と `INCREMENTAL` です。それ以外の値を指定すると `Invalid refresh_mode: <v>. Only INCREMENTAL, PCT are supported.` が返されます。
- `START('<start_time>')` は**`EVERY(...)` と組み合わせた場合にのみ使用でき**、単独では指定できません。
- `IMMEDIATE` / `DEFERRED` は、`ASYNC` を含むリフレッシュ設定全体に適用され、`MANUAL` に限定されません。
- ベーステーブルとして PRIMARY KEY テーブルを使用する場合は、テーブルプロパティ `enable_change_data_capture` を `true` に設定して CDC を有効にする必要があります。
:::

### 例 1: 単一テーブルのフィルタと射影（DUPLICATE KEY ベーステーブル）

```SQL
CREATE TABLE base_dup (
    k INT,
    v INT
) DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 1;

INSERT INTO base_dup VALUES (1, 10), (2, 20), (3, 30);

CREATE MATERIALIZED VIEW mv_dup
DISTRIBUTED BY HASH(k) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ("refresh_mode" = "INCREMENTAL")
AS SELECT k, v FROM base_dup WHERE v > 15;

REFRESH MATERIALIZED VIEW mv_dup WITH SYNC MODE;
SELECT k, v FROM mv_dup ORDER BY k;
```

```Plain
+------+------+
| k    | v    |
+------+------+
|    2 |   20 |
|    3 |   30 |
+------+------+
```

追加のデータが挿入された後、述語を満たす新しい行のみが増分的に書き込まれます。

```SQL
INSERT INTO base_dup VALUES (4, 40), (5, 50), (6, 5);
REFRESH MATERIALIZED VIEW mv_dup WITH SYNC MODE;
SELECT k, v FROM mv_dup ORDER BY k;
```

```Plain
+------+------+
| k    | v    |
+------+------+
|    2 |   20 |
|    3 |   30 |
|    4 |   40 |
|    5 |   50 |
+------+------+
```

### 例 2: 単一テーブルの集計（DUPLICATE KEY ベーステーブル）

以下の例は、許可リストに登録されている集計関数の大部分をカバーしています。

```SQL
CREATE TABLE base_orders (
    order_id       BIGINT,
    region         VARCHAR(8),
    amount         DECIMAL(10,2),
    score          BIGINT,
    nullable_score INT,
    flag           BOOLEAN,
    uid            INT
) DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 1;

INSERT INTO base_orders VALUES
    (1001, 'CN', 99.90, 10,    5, FALSE,  1),
    (1002, 'US', 20.50, 20, NULL, FALSE, 10),
    (1003, 'CN', 15.50, 30, NULL, TRUE,   1),
    (1004, 'US', 75.00, 40,    8, FALSE, 10);

CREATE MATERIALIZED VIEW mv_orders
DISTRIBUTED BY HASH(region) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ("refresh_mode" = "INCREMENTAL")
AS SELECT region,
          SUM(amount)                AS s,
          COUNT(*)                   AS c_all,
          COUNT(nullable_score)      AS c_non_null,
          AVG(amount)                AS avg_dec,
          MIN(amount)                AS mn,
          MAX(amount)                AS mx,
          APPROX_COUNT_DISTINCT(uid) AS acd,
          BOOL_OR(flag)              AS any_flag
   FROM base_orders
   GROUP BY region;

REFRESH MATERIALIZED VIEW mv_orders WITH SYNC MODE;

-- 新しい JP グループを追加し、既存の CN グループにデータを追加します
INSERT INTO base_orders VALUES
    (1005, 'JP', 100.00, 50,   12, FALSE, 99),
    (1006, 'CN',  25.00, 60, NULL, FALSE,  2);
REFRESH MATERIALIZED VIEW mv_orders WITH SYNC MODE;
SELECT * FROM mv_orders ORDER BY region;
```

`CN` と `JP` グループのみが再計算されます。`US` グループには影響がありません。

### 例 3: INNER JOIN（2 つの DUPLICATE KEY ベーステーブル）

```SQL
CREATE TABLE orders (
    order_id BIGINT, uid BIGINT, amount DECIMAL(10,2)
) DUPLICATE KEY(order_id) DISTRIBUTED BY HASH(order_id) BUCKETS 1;

CREATE TABLE users (
    user_id BIGINT, country VARCHAR(8)
) DUPLICATE KEY(user_id) DISTRIBUTED BY HASH(user_id) BUCKETS 1;

INSERT INTO users  VALUES (1, 'CN'), (2, 'US');
INSERT INTO orders VALUES (1001, 1, 99.90), (1002, 2, 20.50);

CREATE MATERIALIZED VIEW mv_join
DISTRIBUTED BY HASH(order_id) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ("refresh_mode" = "INCREMENTAL")
AS SELECT o.order_id, u.country, o.amount
   FROM orders o JOIN users u ON o.uid = u.user_id;

REFRESH MATERIALIZED VIEW mv_join WITH SYNC MODE;

-- ディメンションテーブルとファクトテーブルの両方を増分的に更新します
INSERT INTO users  VALUES (3, 'JP');
INSERT INTO orders VALUES (1003, 1, 15.50), (1004, 3, 200.00);
REFRESH MATERIALIZED VIEW mv_join WITH SYNC MODE;
SELECT * FROM mv_join ORDER BY order_id;
```

```Plain
+----------+---------+--------+
| order_id | country | amount |
+----------+---------+--------+
|     1001 | CN      |  99.90 |
|     1002 | US      |  20.50 |
|     1003 | CN      |  15.50 |
|     1004 | JP      | 200.00 |
+----------+---------+--------+
```

### 例 4: Bitmap を使用した正確な重複排除カウント

```SQL
CREATE TABLE uv_base (
    k INT, uid INT
) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 2;

INSERT INTO uv_base VALUES (1, 10), (1, 20), (1, 20), (2, 30);

CREATE MATERIALIZED VIEW mv_uv
DISTRIBUTED BY HASH(k) BUCKETS 2
REFRESH DEFERRED MANUAL
PROPERTIES ("refresh_mode" = "INCREMENTAL")
AS SELECT k, bitmap_union(to_bitmap(uid)) AS uv FROM uv_base GROUP BY k;

REFRESH MATERIALIZED VIEW mv_uv WITH SYNC MODE;

INSERT INTO uv_base VALUES (1, 30), (2, 30), (2, 40);
REFRESH MATERIALIZED VIEW mv_uv WITH SYNC MODE;

SELECT k, bitmap_count(uv) AS dc FROM mv_uv ORDER BY k;
```

```Plain
+------+------+
| k    | dc   |
+------+------+
|    1 |    3 |
|    2 |    2 |
+------+------+
```

結果は `count(distinct)` と等価です。

```SQL
SELECT COUNT(*) FROM (
    SELECT k, bitmap_count(uv) FROM mv_uv
    EXCEPT SELECT k, COUNT(DISTINCT uid) FROM uv_base GROUP BY k) t;   -- 0
```

概算の重複排除カウントには、`bitmap_union(to_bitmap(uid))` を `hll_union(hll_hash(uid))` に置き換え、クエリ時に `hll_cardinality()` を使用してください。

### 例 5: AGGREGATE KEY ベーステーブルでの厳密なロールアップ

```SQL
CREATE TABLE base_agg (
    region VARCHAR(8),
    city   VARCHAR(8),
    s      BIGINT SUM,
    mx     BIGINT MAX,
    mn     BIGINT MIN
) AGGREGATE KEY(region, city)
DISTRIBUTED BY HASH(region) BUCKETS 1;

INSERT INTO base_agg VALUES ('CN','BJ',10,100,5), ('CN','BJ',20,50,8), ('US','NY',30,200,1);

CREATE MATERIALIZED VIEW mv_agg
DISTRIBUTED BY HASH(region) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ("refresh_mode" = "INCREMENTAL")
AS SELECT region, city, SUM(s) AS s_sum, MAX(mx) AS mx_max, MIN(mn) AS mn_min
   FROM base_agg
   GROUP BY region, city;

REFRESH MATERIALIZED VIEW mv_agg WITH SYNC MODE;

INSERT INTO base_agg VALUES ('CN','BJ',5,150,2), ('JP','TK',7,7,7);
REFRESH MATERIALIZED VIEW mv_agg WITH SYNC MODE;
SELECT * FROM mv_agg ORDER BY region, city;
```

```Plain
+--------+------+-------+--------+--------+
| region | city | s_sum | mx_max | mn_min |
+--------+------+-------+--------+--------+
| CN     | BJ   |    35 |    150 |      2 |
| JP     | TK   |     7 |      7 |      7 |
| US     | NY   |    30 |    200 |      1 |
+--------+------+-------+--------+--------+
```

`GROUP BY region`（キー列のサブセットへのロールアップ）も有効です。ただし、`GROUP BY region, city` 以外の式、および `COUNT(*)` / `AVG()` は拒否されます。

### 例 6: PRIMARY KEY ベーステーブルでの UPDATE / DELETE の取り消し（Retraction）

これは、ネイティブテーブル上の増分マテリアライズドビューにおいて最も差別化された機能の一つです。

```SQL
CREATE TABLE pk_base (
    id INT NOT NULL,
    g  INT,
    v  BIGINT
) PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES ('enable_change_data_capture' = 'true');

INSERT INTO pk_base VALUES (1,10,100),(2,10,200),(3,20,300),(4,20,400),(5,30,500);

CREATE MATERIALIZED VIEW mv_pk
DISTRIBUTED BY HASH(g) BUCKETS 2
REFRESH DEFERRED MANUAL
PROPERTIES ("refresh_mode" = "INCREMENTAL")
AS SELECT g, SUM(v) AS sv, MAX(v) AS mx, MIN(v) AS mn, COUNT(*) AS c
   FROM pk_base
   GROUP BY g;

REFRESH MATERIALIZED VIEW mv_pk WITH SYNC MODE;
SELECT * FROM mv_pk ORDER BY g;
```

```Plain
+------+------+------+------+------+
| g    | sv   | mx   | mn   | c    |
+------+------+------+------+------+
|   10 |  300 |  200 |  100 |    2 |
|   20 |  700 |  400 |  300 |    2 |
|   30 |  500 |  500 |  500 |    1 |
+------+------+------+------+------+
```

`UPDATE` は古い値を正しく取り消し、`MAX` を再計算します。

```SQL
UPDATE pk_base SET v = 250 WHERE id = 2;
REFRESH MATERIALIZED VIEW mv_pk WITH SYNC MODE;
-- g=10 は sv=350, mx=250, mn=100, c=2 になります
```

`DELETE` は行を正しく削除します。グループ全体が空になった場合、対応する行がマテリアライズドビューから削除されます。

```SQL
DELETE FROM pk_base WHERE id = 4;
REFRESH MATERIALIZED VIEW mv_pk WITH SYNC MODE;
-- g=20 は sv=300, mx=300, mn=300, c=1 になります

DELETE FROM pk_base WHERE id = 5;
REFRESH MATERIALIZED VIEW mv_pk WITH SYNC MODE;
-- g=30 の行は完全に削除されます
```

グルーピング列が変更され、行がグループ間を移動する場合、旧グループと新グループの両方が更新されます。

```SQL
UPDATE pk_base SET g = 20 WHERE id = 1;
REFRESH MATERIALIZED VIEW mv_pk WITH SYNC MODE;
```

### 例 7: ベーステーブル変更トリガーを使用したパーティション化されたマテリアライズドビュー

```SQL
CREATE TABLE events (
    id BIGINT, dt DATE, v BIGINT
) DUPLICATE KEY(id, dt)
PARTITION BY RANGE(dt) (
    PARTITION p1 VALUES LESS THAN ('2026-02-01'),
    PARTITION p2 VALUES LESS THAN ('2026-03-01'),
    PARTITION p3 VALUES LESS THAN ('2026-04-01')
)
DISTRIBUTED BY HASH(id) BUCKETS 2;

CREATE MATERIALIZED VIEW mv_events
PARTITION BY dt
DISTRIBUTED BY HASH(id) BUCKETS 2
REFRESH ASYNC                                   -- 各ベーステーブルの取り込み後に増分リフレッシュを自動的にトリガーします
PROPERTIES ("refresh_mode" = "INCREMENTAL")
AS SELECT id, dt, v FROM events;

INSERT INTO events VALUES (1,'2026-01-10',10), (2,'2026-02-10',20);
-- 手動での REFRESH は不要です。取り込みトランザクションがコミットされた後にリフレッシュがトリガーされます。
```

トリガーモードを確認します。

```SQL
SELECT TABLE_NAME, REFRESH_TRIGGER, REFRESH_POLICY
FROM information_schema.materialized_views
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'mv_events';
```

```Plain
+-----------+----------------------+----------------------+
| TABLE_NAME| REFRESH_TRIGGER      | REFRESH_POLICY       |
+-----------+----------------------+----------------------+
| mv_events | ON_BASE_TABLE_CHANGE | ON_BASE_TABLE_CHANGE |
+-----------+----------------------+----------------------+
```

## 増分マテリアライズドビューをリフレッシュする

### 最初のリフレッシュは完全なベースラインとなる

**最初のリフレッシュは、増分の開始点を確立するために完全な PCT ブートストラップを実行します**。真の増分リフレッシュは2回目のリフレッシュから始まります。

したがって、`information_schema.task_runs` で `PCT SUCCESS` と `INCREMENTAL SUCCESS` の両方が表示されるのは、エラーではなく**想定された動作**です。

```SQL
SELECT DISTINCT get_json_string(EXTRA_MESSAGE, '$.refreshMode') AS executed_mode, STATE
FROM information_schema.task_runs
WHERE TASK_NAME = (SELECT TASK_NAME FROM information_schema.materialized_views
                   WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '<mv>')
  AND get_json_double(EXTRA_MESSAGE, '$.processStartTime') > 0
ORDER BY 1, 2;
```

```Plain
+---------------+---------+
| executed_mode | STATE   |
+---------------+---------+
| INCREMENTAL   | SUCCESS |
| PCT           | SUCCESS |
+---------------+---------+
```

ブートストラップ中の同時書き込みが二重にカウントされることはありません。システムは exactly-once セマンティクスを提供するために、読み取りを固定された位置にピン留めします。

### 3 つのリフレッシュトリガーモード

リフレッシュトリガーモードには3種類あります。

**ベーステーブル変更トリガー**

- **DDL 構文**: `EVERY` を伴わない `REFRESH ASYNC`
- **説明**: 各ベーステーブルのトランザクションがコミットされた後に、増分リフレッシュを自動的にトリガーします。**これはネイティブテーブル固有の機能です。** Iceberg ベーステーブルはこれをサポートしません。マテリアライズドビュープロパティ `excluded_trigger_tables` を使用して、特定のベーステーブルを除外できます。

**スケジュールリフレッシュ**

- **DDL 構文**: `REFRESH ASYNC [START('<t>')] EVERY(INTERVAL <n> <unit>)`
- **説明**: 最小間隔は FE 設定項目 `materialized_view_min_refresh_interval`（デフォルトで60秒）によって制御されます。

**手動リフレッシュ**

- **DDL 構文**: `REFRESH [DEFERRED] MANUAL` + `REFRESH MATERIALIZED VIEW <mv>`
- **説明**: `WITH SYNC MODE` を追加すると、完了を同期的に待機します。


### 変更がない場合はリフレッシュをスキップする

どのベーステーブルにも変更がない場合、リフレッシュプランは生成されません。タスク実行の状態は理由 `MV_UP_TO_DATE` とともに `SKIPPED` になりますが、鮮度のタイムスタンプは引き続き前進します。

### トランザクションとリトライ

増分リフレッシュは、本質的に以下のステートメントを実行しています。

```SQL
INSERT INTO <mv> SELECT <incrementally rewritten query>
```

**増分位置の前進とデータの書き込みは、同一トランザクション内でコミットされます。** したがって:

- リフレッシュ失敗 ⇒ 位置が前進しない ⇒ 次のリフレッシュが同じウィンドウを再度消費するため、**重複やデータ損失のない冪等なリトライ**が保証されます。
- 増分リフレッシュのリトライは正確に1回です。`max_mv_refresh_failure_retry_times` は PCT リフレッシュにのみ適用されます。ロックタイムアウトには、デフォルトで3の別個の設定 `max_mv_refresh_try_lock_failure_retry_times` があります。
- 連続した失敗が `max_task_consecutive_fail_count`（デフォルトで10）を超えると、リフレッシュタスクは一時停止され、マテリアライズドビューは非アクティブ化されます。

### バッチ処理（現在ネイティブテーブルには効果なし）

FE 設定項目 `mv_max_rows_per_refresh`（デフォルトで1億行）および `mv_max_bytes_per_refresh`（デフォルトで20 GB）は、蓄積された増分変更を複数のバッチに分割することを目的としています。

## 増分マテリアライズドビューをクエリする

増分マテリアライズドビューは**直接クエリする必要があります**。

```SQL
SELECT * FROM mv_orders WHERE region = 'CN';
```

## 監視とトラブルシューティング

増分マテリアライズドビューのオブザーバビリティには3つのレベルがあります。

### レベル 1: マテリアライズドビューレベル

```SQL
SELECT TABLE_NAME, REFRESH_MODE, REFRESH_TRIGGER, REFRESH_POLICY,
       IS_ACTIVE, INACTIVE_REASON,
       QUERY_REWRITE_STATUS, QUERY_REWRITE_STATUS_REASON,
       LAST_REFRESH_STATE, LAST_REFRESH_FINISHED_TIME,
       LAST_FRESHNESS_CONFIRMED_AT, BASE_TABLE_REFRESH_VERSION_TIMES,
       LAST_REFRESH_ERROR_MESSAGE, TASK_NAME
FROM information_schema.materialized_views
WHERE TABLE_SCHEMA = DATABASE() AND REFRESH_MODE = 'INCREMENTAL';
```

- `LAST_FRESHNESS_CONFIRMED_AT`: 鮮度が確認された直近の時刻です。変更がないためリフレッシュがスキップされた場合にも前進します。
- `BASE_TABLE_REFRESH_VERSION_TIMES`: 各ベーステーブルのデータバージョンのタイムスタンプで、エンドツーエンドの鮮度遅延を評価する際に役立ちます。

### レベル 2: リフレッシュジョブレベル

```SQL
SELECT JOB_ID, REFRESH_TRIGGER, REFRESH_STATE,
       SUBMIT_TIME, FINISH_TIME, DURATION_TIME,
       IMV_SOURCE_VERSION_RANGE,
       IMV_SOURCE_TIMESTAMP_RANGE,
       IMV_SOURCE_PINNED_SNAPSHOT_ID_MAP,
       ERROR_CODE, ERROR_MESSAGE, FAILED_QUERY_ID
FROM information_schema.materialized_view_refresh_jobs
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '<mv>'
ORDER BY SUBMIT_TIME DESC LIMIT 20;
```

- `IMV_SOURCE_VERSION_RANGE`: このジョブが各ベーステーブルに対して消費した位置範囲です。JSON のキーは、カタログを含む完全修飾の可読名を使用します。例:

  ```Plain
  {"default_catalog.mydb.base_tbl":{"start":"1024","end":"1088"}}
  ```

  `start == end` は、このジョブでベーステーブルに変更がなかったことを示します。最初のリフレッシュでは、`start` はリテラル `MIN` としてレンダリングされます。

- `IMV_SOURCE_TIMESTAMP_RANGE`: 上記2つのエンドポイントに対応する、エポックミリ秒単位のコミットタイムスタンプです。**ブックマークがすでに回収されている場合、対応するエンドポイントのタイムスタンプは解決できず、そのベーステーブルのエントリは省略されます。** 長期的な監査には `task_runs` を利用してください。

- PCT ブートストラップジョブなど、増分範囲が消費されない場合、これら3つのカラムはすべて `{}` ではなく `NULL` になります。

### レベル 3: 個々の実行レベル

```SQL
SELECT QUERY_ID, STATE, PROCESS_TIME, FINISH_TIME,
       get_json_string(EXTRA_MESSAGE, '$.refreshMode')           AS executed_mode,
       get_json_string(EXTRA_MESSAGE, '$.imvSourceVersionRange') AS ver_range,
       ERROR_MESSAGE
FROM information_schema.task_runs
WHERE JOB_ID = '<job_id>'
ORDER BY CREATE_TIME;
```

増分ウィンドウが連続しており、隙間がないことを確認するには:

```SQL
WITH r AS (
  SELECT regexp_extract(get_json_string(EXTRA_MESSAGE,'$.imvSourceVersionRange'),'"start": *"([0-9]+)"',1) AS s,
         regexp_extract(get_json_string(EXTRA_MESSAGE,'$.imvSourceVersionRange'),'"end": *"([0-9]+)"',1)   AS e
  FROM information_schema.task_runs
  WHERE TASK_NAME = '<task_name>'
    AND get_json_string(EXTRA_MESSAGE,'$.refreshMode') = 'INCREMENTAL'
    AND get_json_string(EXTRA_MESSAGE,'$.imvSourceVersionRange') <> '{}'
)
SELECT (SELECT COUNT(*) FROM r)                         AS incremental_runs,
       (SELECT COUNT(*) FROM r a JOIN r b ON a.e = b.s) AS adjacent_pairs,
       (SELECT COUNT(*) FROM r WHERE s = e)             AS degenerate_windows;
```

### 失敗が回復可能かどうかを判定する

```SQL
SELECT QUERY_ID, CREATE_TIME, STATE, ERROR_CODE,
       ERROR_MESSAGE LIKE '%CDC-ERROR-1 (CHANGE_NOT_TRACKABLE):%'        AS permanently_broken_be,
       ERROR_MESSAGE LIKE '%do not support non-append-only base changes%' AS permanently_broken_fe,
       ERROR_MESSAGE
FROM information_schema.task_runs
WHERE `DATABASE` = DATABASE()
  AND (DEFINITION LIKE '%<mv>%' OR EXTRA_MESSAGE LIKE '%<mv>%')
  AND STATE = 'FAILED'
ORDER BY CREATE_TIME DESC LIMIT 20;
```

- いずれかのマーカーが `1` の場合、増分チェーンは恒久的に破壊されています。マテリアライズドビューは非アクティブ化されており、**ドロップして再作成する必要があります**。
- 両方が `0` の場合、失敗は通常、メモリ不足エラーやロックタイムアウトなど、リトライ可能なものです。マテリアライズドビューはアクティブなままです。根本的な問題を解決してから、再度リフレッシュしてください。

### 関連するメトリクス

- `mv_global_count{refresh_mode, status}`: リフレッシュモードとアクティブステータス別に非同期マテリアライズドビューをカウントします。
- `mv_global_refresh_jobs_total` / `..._success_jobs_total` / `..._failed_jobs_total` / `mv_global_refresh_duration` / `mv_global_refresh_pending_jobs`。
- `mv_global_query_mv_usage_total{usage_type, refresh_mode}`: `usage_type = DIRECT` は、特に増分マテリアライズドビューの使用パターンを表し、ビジネスクエリが実際にマテリアライズドビューに切り替わったことを確認するために使用できます。
- **ブックマークメトリクス**は、ネイティブテーブル上の増分マテリアライズドビューに固有のもので、保持コストの監視に使用されます。`bookmark_count`、`bookmark_reference_count`、`bookmark_max_active_age_ms` などです。`bookmark_max_active_age_ms` が継続的に増加している場合、マテリアライズドビューが長期間リフレッシュされておらず、古いベーステーブルファイルを回収できないことを示しています。

## 管理と復旧

### サポートされる操作

| 操作                                                          | サポート                                            | 説明                                                           |
| ------------------------------------------------------------ | ------------------------------------------------- | ------------------------------------------------------------ |
| `REFRESH MATERIALIZED VIEW <mv>`                             | はい                                                | リフレッシュを非同期に送信します                               |
| `REFRESH MATERIALIZED VIEW <mv> WITH SYNC MODE`              | はい                                                | 完了を同期的に待機します                                       |
| `REFRESH ... PARTITION START(...) END(...)`                  | **いいえ**                                          | `Partition refresh is not supported for materialized views with refresh_mode=INCREMENTAL. Please refresh the whole materialized view instead.` |
| `REFRESH ... FORCE`                                          | **いいえ**                                          | `FORCE refresh is not supported for materialized views with refresh_mode=INCREMENTAL. Please drop and re-create the materialized view instead.` |
| `ALTER MATERIALIZED VIEW ... SET("refresh_mode"=...)`        | **同じ値のみ**                                       | モード間の切り替えは拒否されます                               |
| `ALTER MATERIALIZED VIEW ... REFRESH ASYNC EVERY(...)` / `... REFRESH MANUAL` | はい                                                | 増分位置を保持します。次のリフレッシュも増分のままです         |
| `ALTER MATERIALIZED VIEW ... REFRESH ASYNC`（ベーステーブル変更トリガーへの切り替え） | サポートされますが、**増分位置が失われます**           | リフレッシュコンテキストを再構築し、記録されていた増分位置をクリアします。`ALTER` 後に最初にトリガーされるリフレッシュは**完全な PCT ブートストラップ**です。ベーステーブル変更トリガーに切り替える際は、フルリフレッシュのための時間枠を確保してください。 |
| `ALTER MATERIALIZED VIEW ... ACTIVE / INACTIVE`              | はい                                                | 以下の説明を参照してください                                   |
| `DROP MATERIALIZED VIEW`                                     | はい                                                | マテリアライズドビューがベーステーブルに対して保持していたすべてのブックマークを解放し、古いバージョンの回収を可能にします |
| 同じベーステーブルに対する複数の増分マテリアライズドビュー    | はい                                                | 各マテリアライズドビューは独立した位置を維持します             |

### 増分チェーンを破壊するベーステーブル操作

| ベーステーブル操作                                            | 結果                                                           | 主要なエラーメッセージ                                          |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| DUPLICATE KEY ベーステーブルに対する `DELETE`                 | リフレッシュが失敗し、マテリアライズドビューが非アクティブ化される（**回復不能**） | `CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): CDC for DUP_KEYS does not support delete` |
| `ALTER TABLE ... DROP PARTITION`                             | リフレッシュが失敗する（**回復不能**）                          | `non-append-only change on base table`                       |
| `TRUNCATE TABLE [PARTITION p]`                               | リフレッシュが失敗する（**回復不能**）                          | 上記と同じ                                                    |
| `INSERT OVERWRITE`                                           | リフレッシュが失敗する（**回復不能**）                          | 上記と同じ                                                    |
| `DROP TABLE`（ベーステーブル）                                | マテリアライズドビューが非アクティブ化される                    | `INACTIVE_REASON` に `base-table dropped` が含まれる           |
| `ALTER TABLE ... RENAME`                                     | マテリアライズドビューが非アクティブ化される                    | `base-table renamed` を含む                                   |
| `ALTER TABLE a SWAP WITH b`                                  | マテリアライズドビューが非アクティブ化される                    | `base-table swapped` を含む                                   |
| `ALTER TABLE ... MODIFY COLUMN`（型の変更）                   | リフレッシュが失敗し、マテリアライズドビューが非アクティブ化される | `column schema not compatible`                               |
| 分散カラム／ソートキーの変更                                   | マテリアライズドビューが非アクティブ化される                    | `INACTIVE_REASON` が `base-table optimized:` で始まる          |
| PRIMARY KEY テーブルの復旧（`recover`）                        | 対応するウィンドウが恒久的に失敗する                            | `Change data capture does not support primary key recover`   |
| クロスクラスターレプリケーションの書き込み                     | 対応するウィンドウが恒久的に失敗する                            | `Change data capture does not support replication`            |
| CDC が無効化されていたウィンドウ                               | 対応するウィンドウが恒久的に失敗する                            | `CHANGES window on tablet <id> spans version <v> which was not recorded` |
| バージョンの祖先チェーンがすでに回収されている                  | 対応するウィンドウが恒久的に失敗する                            | `CHANGES ancestor chain on tablet <id> cannot reach base version <v>` |
| **CDC が有効な PRIMARY KEY ベーステーブルに対する `DELETE` / `UPDATE`** | **通常の増分メンテナンス**                                | —                                                              |
| **マテリアライズドビューがその列を参照していない場合の `ADD COLUMN` / `DROP COLUMN`** | **チェーンは破壊されず、増分リフレッシュが継続する**       | —                                                              |
| **バケット数のみの変更（`DISTRIBUTED BY ... BUCKETS n`）**     | **チェーンは破壊されず、増分リフレッシュが継続する**             | —                                                              |

### 復旧

増分チェーンが恒久的に破壊されると:

```Plain
Cannot incrementally refresh materialized view <mv>: <reason>.
INCREMENTAL materialized views do not support non-append-only base changes
(DELETE / OVERWRITE / DROP PARTITION / snapshot expiration / table replacement).
Drop and recreate the materialized view to recover.
```

マテリアライズドビューは非アクティブ化され、`INACTIVE_REASON` は次のように設定されます。

```Plain
incremental refresh broken by non-append-only base change: <mv>
```

システムは、バックグラウンドでマテリアライズドビューを自動的に再アクティブ化することは**ありません**。

## コストとオーバーヘッド

| コスト                          | 説明                                                           |
| -------------------------------- | ------------------------------------------------------------ |
| **ストレージ増幅**                | 隠しカラム `__ROW_ID__` と `__AGG_STATE_*` がストレージ使用量を増加させます。ベンチマークでは約2.7倍の増幅が示されました。 |
| **ガベージコレクションの遅延**    | マテリアライズドビューが保持するブックマークにより、ベーステーブルの最小保持バージョンが、まだ参照されている最古のバージョンまで引き下げられます。**マテリアライズドビューが存在し、かつリフレッシュされていない限り、古いベーステーブルファイルは回収できません。** そのため、長期間リフレッシュが行われないと、ストレージが継続的に増加する可能性があります。`bookmark_max_active_age_ms` を監視してください。 |
| **PRIMARY KEY の書き込みオーバーヘッド** | CDC が有効な場合、各取り込みは変更箇所を特定するための追加のメタデータページを記録します。これらの構造は取り込みをまたいで蓄積されることはなく、取り込みごとの増分的なメタデータオーバーヘッドを表します。CDC が無効な場合、オーバーヘッドはゼロです。 |
| **限定的な追跡範囲**              | BE 設定項目 `cloud_native_tablet_metadata_ancestors_recorded`（デフォルトで5）は、各タブレットに対して保持される履歴メタデータバージョンの数を決定し、それによって増分リフレッシュがどこまで遡れるかを決定します。この値を増やすと、追加のメタデータストレージを代償に追跡範囲を拡張できます。 |
| **移行コスト**                    | クエリの書き換えはサポートされていないため、ビジネス SQL をマテリアライズドビューに直接クエリするように変更する必要があります。 |

## 意思決定ツリー

```Plain
クラスターはシェアードデータクラスターですか？
├─ いいえ → 非同期マテリアライズドビュー（PCT）を使用する
└─ はい → 既存のビジネス SQL を、マテリアライズドビューを直接クエリするように変更できますか？
         ├─ いいえ → 非同期マテリアライズドビュー（PCT、クエリの書き換えをサポート）を使用する
         └─ はい → ベーステーブルにスケジュールされた DROP PARTITION / TRUNCATE / INSERT OVERWRITE 操作がありますか？
                  ├─ はい → 非同期マテリアライズドビュー（PCT）を使用するか、それらの操作を再設計する
                  └─ いいえ → クエリはサポートされている演算子と集計関数のみを使用していますか？
                           ├─ いいえ → 非同期マテリアライズドビュー（PCT）を使用する
                           └─ はい → ベーステーブルのデータモデルは何ですか？
                                    ├─ UNIQUE KEY → サポート対象外。モデルを変更するか PCT を使用する
                                    ├─ AGGREGATE KEY → クエリを厳密なロールアップとして表現できますか？
                                    │                   ├─ はい → 増分マテリアライズドビューを使用する
                                    │                   └─ いいえ → PCT を使用する
                                    ├─ PRIMARY KEY → CDC を有効にして増分マテリアライズドビューを使用する
                                    └─ DUPLICATE KEY → 増分マテリアライズドビューを使用する
```

## よくある誤解

| 誤解                                                          | 実際の動作                                                     |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| 「増分マテリアライズドビューは非同期マテリアライズドビューのアップグレード版であり、完全に置き換えることができる」 | いいえ。これは PCT よりも SQL サポート範囲が大幅に狭い特化モードであり、クエリの書き換えをサポートしません。 |
| 「マテリアライズドビューを作成すれば、既存のクエリは自動的に高速化される」 | いいえ。ビジネス SQL は、マテリアライズドビューをクエリするように明示的に書き換える必要があります。 |
| 「PRIMARY KEY ベーステーブルに対する `DELETE` は増分チェーンを破壊する」 | いいえ。CDC が有効な PRIMARY KEY ベーステーブルでは、行レベルの `DELETE` / `UPDATE` は正常にサポートされます。チェーンを破壊するのは、パーティションレベル／テーブルレベルの破壊的操作です。 |
| 「タスク実行で `PCT` が表示されるのは、増分設定が反映されていないことを意味する」 | いいえ。最初のリフレッシュは完全なブートストラップであるため `PCT` を使用します。`PCT` と `INCREMENTAL` の両方が表示されるのは想定内です。 |
| 「`materialized_view_refresh_jobs.REFRESH_MODE` は実際に実行されたモードを示す」 | いいえ。これは設定されたモードです。実際の実行モードは `task_runs.EXTRA_MESSAGE.$.refreshMode` にあります。 |
| 「マテリアライズドビューはまず PCT として作成し、後で `ALTER` を使って INCREMENTAL に変更できる」 | いいえ。`refresh_mode` は `ALTER` によってモード間で変更することはできません。代わりにマテリアライズドビューをドロップして再作成してください。 |
| 「`default_mv_refresh_mode` を `incremental` に変更すると、新規作成されるすべてのマテリアライズドビューが増分になる」 | いいえ。作成処理は明示的に指定された `refresh_mode` プロパティのみを読み取ります。この設定を変更すると、マテリアライズドビューが「自身を増分だと報告しながら実際には PCT を使用する」状態になる可能性があり、同時にクエリの書き換えも無効になります。 |
| 「`ALTER ... REFRESH ASYNC` を使ってマテリアライズドビューをスケジュールリフレッシュからベーステーブル変更トリガーに変更しても、情報の損失はない」 | いいえ。この操作は増分位置をクリアし、次回のリフレッシュで完全なブートストラップをトリガーします。 |
| 「`mv_max_rows_per_refresh` を増やすと、各リフレッシュで処理されるデータ量を制御できる」 | これは現在、内部テーブルをベーステーブルとする場合には効果がありません。各リフレッシュは蓄積されたウィンドウ全体を消費します。 |
| 「増分リフレッシュは常に安価である」                             | ワークロード次第です。書き込みが多数の小さなパーティションに自然に分散している場合や、履歴パーティションがもはや変更されない場合は、PCT がすでに低コストであることがあります。自分のワークロードでベンチマークを行ってください。 |

## パラメータリファレンス

### テーブルプロパティ（ベーステーブル）

#### `enable_change_data_capture`

- 型: Boolean
- デフォルト: `false`
- 説明: シェアードデータクラスターの PRIMARY KEY テーブルにのみ設定できます。テーブルを増分マテリアライズドビューのベーステーブルとして使用できるようにし、`UPDATE` / `DELETE` の取り消し（Retraction）をサポートします。非同期メタデータジョブとして `ALTER TABLE ... SET(...)` を使用してオンラインで有効化できます。

### マテリアライズドビュープロパティ

#### `refresh_mode`

- 型: Enum
- デフォルト: `PCT`
- 説明: 増分マテリアライズドビューを宣言するには `INCREMENTAL` に設定します。作成時に明示的に指定する必要があり、`ALTER` ステートメントによってモード間で切り替えることはできません。

#### `excluded_trigger_tables`

- 型: String
- デフォルト: 空の文字列
- 説明: ベーステーブル変更トリガーから除外するベーステーブルのテーブル名のリストです。`REFRESH ASYNC` の場合のみ有効です。

#### `partition_refresh_number` / `partition_refresh_strategy` / `auto_refresh_partitions_limit`

- 説明: これらのプロパティは増分マテリアライズドビューには効果がありません。

#### `enable_query_rewrite` およびその他の書き換え関連プロパティ

- 説明: これらのプロパティは増分マテリアライズドビューには効果がありません。クエリの書き換えは常に無効です。

### FE 設定項目

#### `default_mv_refresh_mode`

- 型: String
- デフォルト: `pct`
- 説明: 有効な値は `PCT` / `INCREMENTAL` です。**新規作成されるマテリアライズドビューが実際に増分メンテナンスを使用するかどうかを決定するものではありません**。作成処理は、明示的な `refresh_mode` プロパティのみをチェックします。これを `incremental` に変更すると、明示的なプロパティを持たないマテリアライズドビューが「自身を増分だと報告しながら実際には PCT を使用する」状態になる可能性があります。デフォルト値のままにしてください。


#### `mv_max_rows_per_refresh`

- 型: long
- デフォルト: `100000000`
- 説明: 1回の増分バッチあたりの最大行数です。**現在、ネイティブテーブルには効果がありません。**

#### `mv_max_bytes_per_refresh`

- 型: long
- デフォルト: `21474836480`
- 説明: 1回の増分バッチあたりの最大バイト数（20 GB）です。**現在、ネイティブテーブルには効果がありません。**

#### `mv_refresh_try_lock_timeout_ms`

- 型: int
- デフォルト: `30000`
- 説明: リフレッシュプランを生成する際のロック取得タイムアウトです。

#### `max_mv_refresh_try_lock_failure_retry_times`

- 型: int
- デフォルト: `3`
- 説明: ロックタイムアウト後のリトライ回数です。

#### `max_mv_refresh_failure_retry_times`

- 型: int
- デフォルト: `1`
- 説明: **PCT リフレッシュにのみ適用されます**。増分リフレッシュは常に1回リトライします。

#### `max_task_consecutive_fail_count`

- 型: int
- デフォルト: `10`
- 説明: 連続した失敗がこの値を超えると、タスクを一時停止し、マテリアライズドビューを非アクティブ化します。

#### `max_mv_task_run_meta_message_values_length`

- 型: int
- デフォルト: `8`
- 説明: `EXTRA_MESSAGE` 内のコレクション型フィールドの切り詰め長です。**ベーステーブルが8個を超える場合、オブザーバビリティデータは不完全になります。**

#### `materialized_view_min_refresh_interval`

- 型: int
- デフォルト: `60` 秒
- 説明: `EVERY(INTERVAL ...)` の最小間隔です。

#### `enable_mv_automatic_active_check`

- 型: boolean
- デフォルト: `true`
- 説明: バックグラウンドでの自動再アクティブ化を有効にします。増分チェーンの破壊が原因の場合は対象外です。

#### `bookmark_reference_max_ttl_ms`

- 型: long
- デフォルト: `-1`（無効）
- 説明: クラスターレベルでのブックマーク参照の最大生存期間です。**正の値を設定すると、マテリアライズドビューに必要なブックマークが回収され、リフレッシュ失敗を引き起こす可能性があります。使用には注意してください。**

#### `enable_bookmark_meta_functions`

- 型: boolean
- デフォルト: `false`
- 説明: `bookmark_create()` / `bookmark_release()` などの診断用関数を有効にします。トラブルシューティング目的専用です。

### BE 設定項目

#### `cloud_native_tablet_metadata_ancestors_recorded`

- 型: int
- デフォルト: `5`
- 説明: 各タブレットに対して保持される履歴メタデータの祖先バージョン数です。増分リフレッシュの追跡範囲を決定します。この値を増やすと、追加のメタデータストレージを代償に、リフレッシュがより過去まで遡れるようになります。

## 機能境界

### 前提条件

増分マテリアライズドビューを作成するには、以下のすべてが必要です。

- **シェアードデータクラスター**であり、ベーステーブルがクラウドネイティブテーブルまたは Iceberg テーブルであること。
- **サポートされているベーステーブルタイプ**であること。DUPLICATE KEY テーブルは直接サポートされ、PRIMARY KEY テーブルには CDC が必要で、AGGREGATE KEY テーブルは厳密なロールアップのみをサポートし、UNIQUE KEY はサポートされません。
- クエリがサポートされている演算子のみを使用していること。[Supported Operators](#supported-operators) を参照してください。
- すべての集計関数が許可リストに含まれていること。[Supported Aggregation Functions](#supported-aggregation-functions) を参照してください。

### ベーステーブルタイプ

| ベーステーブルタイプ                       | サポート | 説明                                                           |
| ------------------------------------------ | -------- | ------------------------------------------------------------ |
| シェアードデータクラスターのクラウドネイティブテーブル | はい     | 本ドキュメントの主な対象です。                                 |
| シェアードナッシングクラスターのネイティブテーブル | いいえ   | エラー: `IVMAnalyzer does not support table type: OLAP`       |
| Iceberg テーブル（追記専用）                | はい     | [CREATE MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW.md) を参照してください。 |
| Hive / Hudi / Delta Lake / Paimon / JDBC   | いいえ   | エラー: `IVMAnalyzer does not support table type: <TYPE>`     |
| 論理ビュー                                  | いいえ   | エラー: `IVMAnalyzer does not support inner relation type: ViewRelation` |
| マテリアライズドビュー（MV on MV）          | いいえ   | エラー: `IVMAnalyzer does not support table type: CLOUD_NATIVE_MATERIALIZED_VIEW` |
| CTE（WITH 句）                              | いいえ   | エラー: `IVMAnalyzer does not support inner relation type: CTERelation` |
| 一時テーブル                                | いいえ   | エラー: `Materialized view can't base on temporary table`     |

### データモデル

#### DUPLICATE KEY ベーステーブル

**追記専用（append-only）の書き込みがサポートされます。** 追加の設定は不要です。

ベーステーブルに対して `DELETE` を実行すると、リフレッシュが失敗し、マテリアライズドビューが非アクティブ化されます。エラーメッセージには次が含まれます。

```Plain
CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): CDC for DUP_KEYS does not support delete
```

これは、DUPLICATE KEY テーブルには削除ベクター（delete vector）がないためです。行にはアドレス指定可能な行識別子がないため、システムは個々の行の消失をマテリアライズドビューに伝播できません。

#### PRIMARY KEY ベーステーブル

**`INSERT` / `UPDATE` / `DELETE` に対する行レベルの増分メンテナンスがサポートされます。** これは、ネイティブテーブル上の増分マテリアライズドビューを Iceberg と差別化する最も重要な機能です。

ベーステーブルは、テーブルプロパティ `enable_change_data_capture` を `true` に設定して CDC を有効にする必要があります。

```SQL
-- テーブル作成時に CDC を有効にします
CREATE TABLE pk_base (
    id INT NOT NULL,
    g  INT,
    v  BIGINT
) PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES ('enable_change_data_capture' = 'true');

-- または、既存のテーブルに対して有効にします（非同期のメタデータ変更ジョブです。完了を待ってください）
ALTER TABLE pk_base SET ('enable_change_data_capture' = 'true');
```

CDC が有効になっていない場合、増分マテリアライズドビューの作成は次のエラーで失敗します。

```Plain
IVM on cloud-native PRIMARY KEY base 'pk_base' requires change data capture to be
enabled on the base table: ... Enable it with
ALTER TABLE pk_base SET ('enable_change_data_capture' = 'true').
```

PRIMARY KEY ベーステーブルには、以下の追加の制限があります。

| 制限事項                                                       | 主要なエラーメッセージ                                          |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| 純粋な射影／フィルタクエリにおいて、非 PRIMARY KEY ベーステーブルと混在させることはできない | `IVM over a cloud-native PRIMARY KEY base requires every base to be a cloud-native PRIMARY KEY table, but '<t>' is not` |
| 非 PRIMARY KEY ベーステーブルと混在させた上で集計することはできない | `IVM retractable aggregate requires every base to be a cloud-native PRIMARY KEY table` |
| マテリアライズドビューの定義内で `ORDER BY (...)` はサポートされない | `IVMAnalyzer does not yet support ORDER BY for a materialized view over a cloud-native PRIMARY KEY base` |
| JOIN を含むマテリアライズドビューの出力列は順序付け可能（orderable）である必要がある。JSON / MAP / BITMAP / HLL / PERCENTILE / VARIANT はサポートされない | `IVMAnalyzer does not support a join materialized view with a non-orderable output column type` |
| 派生テーブルに対する `SELECT *` はサポートされない             | `IVMAnalyzer does not support SELECT * over a derived table on a cloud-native PRIMARY KEY base; list the columns explicitly` |
| 派生テーブルに対する `t(a, b)` のような明示的な列エイリアスリストはサポートされない | `IVMAnalyzer does not support a derived table with an explicit column alias list` |
| `UNION ALL` は取り消し可能（retractable）なブランチと追記専用のブランチを混在させることはできない | `IVMAnalyzer does not support a UNION ALL that mixes a retractable cloud-native PRIMARY KEY branch with an append-only branch` |
| `UNION ALL` のブランチ内に `GROUP BY` / `DISTINCT` を含めることはできない | `IVMAnalyzer does not support a GROUP BY or DISTINCT branch in a UNION ALL materialized view` |

以下の PRIMARY KEY の形式がサポート済みとして検証されています。単一テーブルクエリ、PK INNER JOIN PK、PK CROSS JOIN PK、PK テーブルに対する集計、PK テーブルに対する1階層の派生テーブル、および PK UNION ALL PK。

#### AGGREGATE KEY ベーステーブル

**厳密なロールアップのみがサポートされます。** このデータモデルには最も多くの制限があります。

これは、増分データストリームには**集計前**の生の行が含まれる一方、通常のクエリはマージ済みの結果を参照するためです。増分メンテナンスは、マテリアライズドビューの各グループが完全なマージ済みグループに対応しており、かつ集計がマージ操作に対して不変である場合にのみ安全です。

以下の8つの制約が適用されます。

| 制約                                                           | 主要なエラーメッセージ                                          |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| 唯一の `FROM` ソースである必要がある。JOIN / UNION / サブクエリは許可されない | `is only supported when the base table is the sole FROM source (no JOIN, UNION, or subquery)` |
| `GROUP BY` を含む必要がある。純粋な射影マテリアライズドビューはサポートされない | `requires GROUP BY: the CDC delta stream emits raw pre-merge rowset rows` |
| `GROUP BY` には**単純な列参照**のみを含めることができる         | `only supports GROUP BY plain column references, got: <expr>` |
| `GROUP BY` の列は、**集計キー列のサブセット**である必要がある    | `requires GROUP BY columns to be a subset of the aggregate-key columns` |
| `WHERE` はキー列のみをフィルタできる                            | `does not support a WHERE predicate on value column '<c>': the predicate is evaluated on raw pre-merge delta rows` |
| 集計の引数は**単一の単純な列参照**である必要がある（`SUM(s * 2)` は DUPLICATE KEY テーブルでは有効であっても拒否される） | `requires aggregate over a single plain column reference, got: <expr>` |
| 集計関数は、ベースカラムの集計タイプと列ごとに一致している必要がある | `requires MV aggregate to match the base column's aggregation type` |
| 集計はキー列または `REPLACE` / `REPLACE_IF_NOT_NULL` 列には適用できない | `does not support aggregate on aggregate-key column '<c>'`; `does not support aggregate on REPLACE/REPLACE_IF_NOT_NULL column '<c>': replace semantics is order-dependent` |

AGGREGATE KEY ベーステーブルでサポートされる集計関数は、`sum`、`max`、`min`、`bitmap_union`、`hll_union`、`percentile_union` の6つのみです。

`count` / `count(*)` / `avg` / `ndv` / `approx_count_distinct` / `array_agg` / `bool_or` / `bitmap_agg` は、DUPLICATE KEY ベーステーブルではサポートされていても、AGGREGATE KEY ベーステーブルでは拒否されます。

```Plain
IVM on cloud-native AGGREGATE KEY base '<t>' does not support aggregate 'count';
AGG_KEYS base only accepts delta-rollup-compatible aggregates [sum, max, min,
bitmap_union, hll_union, percentile_union]
```

#### UNIQUE KEY ベーステーブル

**サポートされていません**

```Plain
IVM on cloud-native UNIQUE_KEYS base '<t>' is not supported: the CDC delta stream is
append-only and cannot maintain replace/upsert semantics, so the MV would retain
stale rows after a key update
```

#### 混在するデータモデルの JOIN

| 組み合わせ                                          | サポート |
| --------------------------------------------------- | -------- |
| PRIMARY KEY ⋈ PRIMARY KEY（両方で CDC が有効）        | はい     |
| DUPLICATE KEY ⋈ DUPLICATE KEY                        | はい     |
| PRIMARY KEY ⋈ DUPLICATE KEY                          | いいえ   |
| PRIMARY KEY ⋈ DUPLICATE KEY + 集計                    | いいえ   |
| AGGREGATE KEY ⋈ 任意のテーブル                        | いいえ   |

### サポートされる演算子

| 演算子                                                        | サポート | 説明                                                           |
| ------------------------------------------------------------ | -------- | ------------------------------------------------------------ |
| `SELECT` 内のスカラー式                                       | はい     | `CAST`、`CASE WHEN`、算術演算、文字列・日付関数など              |
| `WHERE` フィルタリング                                        | はい     | AGGREGATE KEY ベーステーブルの場合はキー列に限定される          |
| `GROUP BY` 集計                                               | はい     | `GROUP BY` が必須。`GROUP BY` のないグローバル集計はサポートされない |
| `GROUP BY` の序数指定（`GROUP BY 1`）                          | はい     | 自動的に処理される                                              |
| `GROUP BY` 式                                                 | はい     | AGGREGATE KEY ベーステーブルの場合は単純な列参照に限定される     |
| 集計を伴わない `GROUP BY`（重複排除）                          | はい     | キーによる重複排除と等価                                        |
| `SELECT DISTINCT`                                             | はい     | AGGREGATE KEY ベーステーブルを除き、等価な `GROUP BY` に自動的に書き換えられる |
| グルーピングキーのみを参照する `HAVING`                        | はい     |                                                                |
| INNER JOIN                                                    | はい     | 片側または両側の変更を正しく処理する                            |
| CROSS JOIN                                                    | はい     |                                                                |
| UNION ALL                                                     | はい     | ブランチに集計を含めることはできない                            |
| 派生テーブル／サブクエリ                                       | 部分的   | すべてのベーステーブルモデルで1階層のみサポートされる。内側のクエリには射影／フィルタリング／JOIN のみを含めることができる。内側のクエリ内の集計、`DISTINCT`、`UNION` は拒否される |
| **LEFT / RIGHT / FULL OUTER JOIN**                            | いいえ   | `IVMAnalyzer does not support join type: LEFT OUTER JOIN`     |
| **SEMI / ANTI JOIN**                                          | いいえ   | `IVMAnalyzer does not support join type: LEFT SEMI JOIN`      |
| **UNION（distinct）**                                          | いいえ   | `IVMAnalyzer only supports UNION ALL, but got: DISTINCT`      |
| **INTERSECT / EXCEPT**                                        | いいえ   | `IVMAnalyzer can only handle UnionRelation, but got: IntersectRelation` |
| **ウィンドウ関数／分析関数**                                    | いいえ   | `IVMAnalyzer does not support window functions`               |
| **クエリ内の `ORDER BY`**                                      | いいえ   | `IVMAnalyzer does not support order by clause`                |
| **`LIMIT`**                                                   | いいえ   | 作成が `IVM rewrite failed to fully resolve incremental markers` で失敗する |
| **集計関数を含む `HAVING`**                                    | いいえ   | `IVMAnalyzer does not support HAVING with aggregate functions` |
| **GROUPING SETS / ROLLUP / CUBE / GROUP BY ALL**              | いいえ   | `IVMAnalyzer does not support <GROUPING_SETS｜ROLLUP｜CUBE｜GROUP_BY_ALL> for incremental view maintenance` |
| **`GROUP BY` のないグローバル集計**                            | いいえ   | `IVMAnalyzer requires group by expressions for incremental view maintenance.` |
| **Distinct 集計**（`count(distinct x)` など）                  | いいえ   | `IVMAnalyzer does not support distinct aggregate functions`  |
| **CTE（`WITH`）**                                              | いいえ   | `IVMAnalyzer does not support inner relation type: CTERelation` |
| **非決定的関数**（`rand()` / `now()` / `uuid()`）               | いいえ   | マテリアライズドビュー全般の制限                                |
| **タイムトラベルクエリ**                                        | いいえ   | マテリアライズドビュー全般の制限                                |

### サポートされる集計関数

| 関数                     | サポートされる引数の型                                          |
| ------------------------ | ------------------------------------------------------------ |
| `count`                  | `count(*)` または `count(col)`（任意の型、引数は最大1つ）        |
| `sum`                    | 整数型、浮動小数点型、DECIMAL                                    |
| `avg`                    | 整数型、浮動小数点型、DECIMAL                                    |
| `min` / `max`            | 整数型、浮動小数点型、DECIMAL、DATE、DATETIME、文字列型          |
| `array_agg`              | 引数1つ。`array_agg(col ORDER BY k)` は**サポートされない**       |
| `bool_or`                | BOOLEAN                                                       |
| `approx_count_distinct`  | 引数1つ                                                       |
| `ndv`                    | 引数1つ                                                       |
| `bitmap_agg`             | 引数1つ                                                       |
| `bitmap_union`           | BITMAP。例: `bitmap_union(to_bitmap(col))`                     |
| `hll_union`              | HLL。例: `hll_union(hll_hash(col))`                            |
| `percentile_union`       | PERCENTILE。例: `percentile_union(percentile_hash(col))`       |

サポートされていない代表的な関数には、`count(distinct x)`、`multi_distinct_count`、`stddev`、`variance`、`stddev_samp`、`percentile_approx`、`group_concat`、`any_value`、`max_by`、`min_by`、`retention`、`window_funnel`、`bitmap_union_count`、`hll_union_agg`、`covar_*`、`corr`、およびすべての集計 UDF が含まれます。

エラーの形式は次のとおりです。

```Plain
IVMAnalyzer does not support aggregate function: stddev. Supported functions:
[count, sum, avg, min, max, array_agg, bool_or, approx_count_distinct, ndv,
bitmap_agg, bitmap_union, hll_union, percentile_union]
```

### マテリアライズドビューの構造

増分マテリアライズドビューは**常に PRIMARY KEY テーブル**です。ユーザーは他のモデルを選択できません。これにより、ユーザーから見えるいくつかの副作用が生じます。

- **隠しカラム `__ROW_ID__`**: マテリアライズドビューの主キーであり、各行を識別します。次の2つの生成元があります。

  - **クエリ由来**: `GROUP BY` / `DISTINCT` が存在する場合、またはベーステーブルが PRIMARY KEY の場合、値はグルーピングキーまたは主キー列からエンコードされ、型は `VARCHAR` になります。
  - **自動採番**: 純粋な追記専用の射影／フィルタシナリオでは、`BIGINT AUTO_INCREMENT` となり、ストレージエンジンによって値が設定されます。
- **隠しカラム `__AGG_STATE_<集計式>`**: ファイナライズ可能な各集計関数は、追加の中間状態カラムを保存します。`bitmap_union`、`hll_union`、`percentile_union` は、出力カラムとして直接使用される場合には畳み込まれ、追加のカラムは作成されません。
- **ストレージ増幅**: これらの隠しカラムはストレージ使用量を増加させます。ベンチマークでは、ストレージは PCT の117 MB に対して318 MB と、約**2.7倍**でした。それに応じて追加の容量を確保してください。

パーティショニング、分散、ソートキー:

| 句               | 説明                                                           |
| ---------------- | ------------------------------------------------------------ |
| `PARTITION BY`   | サポートされます。標準のマテリアライズドビューのパーティション検証を使用し、ベーステーブルとは異なる粒度を使用できます。例えば、ベーステーブルが日次パーティションで、マテリアライズドビューが月次パーティションであるといった構成が可能です。 |
| `DISTRIBUTED BY` | **自動的に正規化されます**。省略され、かつシェアードデータクラスターで `enable_range_distribution` が有効な場合、Range 分散が使用されます。Range 分散にはユーザーが記述可能な構文はありません。それ以外の場合は、明示的に指定されたバケット数のみを保持したまま、**すべてのキー列**に対する HASH 分散に正規化されます。明示的なハッシュ列や `RANDOM` は無視されます。 |
| `ORDER BY (...)` | 非 PRIMARY KEY ベーステーブルではサポートされますが、**PRIMARY KEY ベーステーブルではサポートされません**。 |

## エラーリファレンス

| エラーメッセージに含まれる文字列                                | 意味と対処                                                     |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| `does not support table type: OLAP`                          | シェアードナッシングクラスターはサポートされていません。回避策はありません。 |
| `does not support table type: CLOUD_NATIVE_MATERIALIZED_VIEW` | MV on MV はサポートされていません。                            |
| `does not support inner relation type: ViewRelation` / `CTERelation` | 論理ビュー／CTE をベースリレーションとして使用することはできません。テーブル参照に展開してください。 |
| `requires change data capture to be enabled`                 | PRIMARY KEY ベーステーブルには `ALTER TABLE ... SET ('enable_change_data_capture' = 'true')` が必要です。 |
| `UNIQUE_KEYS base ... is not supported`                      | UNIQUE KEY ベーステーブルはサポートされていません。代わりに PRIMARY KEY モデルを検討してください。 |
| `AGGREGATE KEY base ... requires GROUP BY`                   | AGGREGATE KEY ベーステーブルはロールアップを使用する必要があります。純粋な射影マテリアライズドビューはサポートされていません。 |
| `AGGREGATE KEY base ... does not support aggregate 'count'`  | AGGREGATE KEY ベーステーブルは `sum` / `max` / `min` / `bitmap_union` / `hll_union` / `percentile_union` のみをサポートします。 |
| `does not support join type: LEFT OUTER JOIN`                | INNER / CROSS JOIN のみがサポートされます。                    |
| `only supports UNION ALL`                                    | `UNION`（distinct）、`INTERSECT`、`EXCEPT` はサポートされていません。 |
| `does not support window functions`                          | ウィンドウ関数はサポートされていません。                        |
| `does not support order by clause`                           | クエリ本体に `ORDER BY` を含めることはできません。              |
| `does not support HAVING with aggregate functions`           | `HAVING` はグルーピングキーのみを参照できます。                 |
| `requires group by expressions`                              | 集計には `GROUP BY` が必要です。グローバル集計はサポートされていません。 |
| `does not support distinct aggregate functions`              | `count(distinct col)` の代わりに `bitmap_union(to_bitmap(col))` を使用してください。 |
| `does not support aggregate function: <name>`                | この関数は許可リストに含まれていません。[Supported Aggregation Functions](#supported-aggregation-functions) を参照してください。 |
| `every base to be a cloud-native PRIMARY KEY table`          | PRIMARY KEY ベーステーブルは、非 PRIMARY KEY ベーステーブルと混在させることはできません。 |
| `IVM rewrite failed to fully resolve incremental markers`    | クエリに、`LIMIT` や未解決の相関サブクエリなど、増分的に維持できない演算子が含まれています。 |
| `Failed to generate IVM refresh plan at CREATE time`         | 作成時にコンパイルが失敗しました。実際の理由はこのメッセージの後に表示されます。 |
| `Invalid refresh_mode`                                       | `refresh_mode` は `PCT` / `INCREMENTAL` のみを受け付けます。   |
| `Altering refresh_mode from ... is not supported`            | モード間の `ALTER` はサポートされていません。マテリアライズドビューをドロップして再作成してください。 |
| `Partition refresh is not supported` / `FORCE refresh is not supported` | 増分マテリアライズドビューは、パーティションリフレッシュや FORCE リフレッシュをサポートしていません。 |
| `do not support non-append-only base changes`                | 増分チェーンが恒久的に破壊されています。マテリアライズドビューをドロップして再作成してください。 |
| `CDC-ERROR-1 (CHANGE_NOT_TRACKABLE)`                         | DUPLICATE KEY テーブルに対する `DELETE`、CDC のギャップ、回収済みの祖先チェーンなど、変更を追跡できません。増分チェーンは恒久的に破壊されています。マテリアライズドビューをドロップして再作成してください。 |
| `column schema not compatible`                               | ベーステーブルの列型の変更により、スキーマの非互換性が発生しました。マテリアライズドビューをドロップして再作成してください。 |

## 関連ドキュメント

- [CREATE MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW.md)
- [REFRESH MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/REFRESH_MATERIALIZED_VIEW.md)
- [ALTER MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/ALTER_MATERIALIZED_VIEW.md)
- [非同期マテリアライズドビュー](./async_mv.mdx)
- [information_schema.materialized_views](../../sql-reference/information_schema/materialized_views.md)
- [information_schema.materialized_view_refresh_jobs](../../sql-reference/information_schema/materialized_view_refresh_jobs.md)

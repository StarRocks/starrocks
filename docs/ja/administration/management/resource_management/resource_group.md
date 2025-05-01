---
displayed_sidebar: docs
sidebar_position: 10
---

# リソースグループ

このトピックでは、StarRocks のリソースグループ機能について説明します。

![resource group](../../../_assets/resource_group.png)

この機能を使用すると、短いクエリ、アドホッククエリ、ETL ジョブなど、複数のワークロードを単一のクラスターで同時に実行でき、複数のクラスターを展開する余分なコストを節約できます。技術的な観点から見ると、実行エンジンはユーザーの指定に従って同時にワークロードをスケジュールし、それらの間の干渉を隔離します。

リソースグループのロードマップ：

- v2.2 以降、StarRocks はクエリのリソース消費を制限し、同じクラスター内のテナント間でのリソースの隔離と効率的な使用を実現します。
- StarRocks v2.3 では、大規模クエリのリソース消費をさらに制限し、過大なクエリ要求によってクラスターリソースが枯渇するのを防ぎ、システムの安定性を保証します。
- StarRocks v2.5 は、データロード（INSERT）の計算リソース消費を制限することをサポートします。

|  | Internal Table | External Table | Big Query Restriction | Short Query | INSERT INTO | Broker Load  | Routine Load, Stream Load, Schema Change |
|---|---|---|---|---|---|---|---|
| 2.2 | √ | × | × | × | × | × | × |
| 2.3 | √ | √ | √ | √ | × | × | × |
| 2.5 | √ | √ | √ | √ | √ | × | × |
| 3.1 and later | √ | √ | √ | √ | √ | √ | × |

## 用語

このセクションでは、リソースグループ機能を使用する前に理解しておくべき用語について説明します。

### リソースグループ

各リソースグループは、特定の BE からの計算リソースのセットです。クラスターの各 BE を複数のリソースグループに分割できます。クエリがリソースグループに割り当てられると、StarRocks は指定したリソースクォータに基づいてリソースグループに CPU およびメモリリソースを割り当てます。

BE 上のリソースグループに対して、次のパラメータを使用して CPU およびメモリリソースのクォータを指定できます。

- `cpu_core_limit`

  このパラメータは、BE 上のリソースグループに割り当てられる CPU コアのソフトリミットを指定します。有効な値: 0 以外の正の整数。範囲: (1, `avg_be_cpu_cores`]、ここで `avg_be_cpu_cores` はすべての BE における平均 CPU コア数を表します。

  実際のビジネスシナリオでは、リソースグループに割り当てられる CPU コアは、BE 上の CPU コアの可用性に基づいて比例的にスケールします。

  > **注意**
  >
  > たとえば、16 CPU コアを提供する BE に 3 つのリソースグループ rg1、rg2、rg3 を構成します。3 つのリソースグループの `cpu_core_limit` の値はそれぞれ `2`、`6`、`8` です。
  >
  > BE のすべての CPU コアが占有されている場合、3 つのリソースグループに割り当てられる CPU コアの数はそれぞれ 2、6、8 です。計算は次のとおりです。
  >
  > - rg1 の CPU コア数 = BE の総 CPU コア数 × (2/16) = 2
  > - rg2 の CPU コア数 = BE の総 CPU コア数 × (6/16) = 6
  > - rg3 の CPU コア数 = BE の総 CPU コア数 × (8/16) = 8
  >
  > BE のすべての CPU コアが占有されていない場合、たとえば rg1 と rg2 がロードされているが rg3 がロードされていない場合、rg1 と rg2 に割り当てられる CPU コアの数はそれぞれ 4 と 12 です。計算は次のとおりです。
  >
  > - rg1 の CPU コア数 = BE の総 CPU コア数 × (2/8) = 4
  > - rg2 の CPU コア数 = BE の総 CPU コア数 × (6/8) = 12

- `mem_limit`

  このパラメータは、BE が提供する総メモリのうち、クエリに使用できるメモリの割合を指定します。有効な値: (0, 1)。

  > **注意**
  >
  > クエリに使用できるメモリの量は、`query_pool` パラメータによって示されます。

- `concurrency_limit`

  このパラメータは、リソースグループ内の同時クエリの上限を指定します。これは、過剰な同時クエリによるシステムの過負荷を避けるために使用されます。このパラメータは、0 より大きく設定された場合にのみ有効です。デフォルト: 0。

- `max_cpu_cores`

  FE でクエリキューをトリガーするための CPU コアのしきい値です。これは、`0` より大きく設定された場合にのみ有効です。範囲: [0, `avg_be_cpu_cores`]、ここで `avg_be_cpu_cores` はすべての BE ノードにおける平均 CPU コア数を表します。デフォルト: 0。

- `spill_mem_limit_threshold`

  リソースグループが中間結果のスピリングをトリガーするメモリ使用量のしきい値（パーセンテージ）です。有効な範囲は (0, 1) です。デフォルト値は 1 で、しきい値が有効でないことを示します。このパラメータは v3.1.7 で導入されました。
  - 自動スピリングが有効（つまり、システム変数 `spill_mode` が `auto` に設定されている）で、リソースグループ機能が無効な場合、クエリのメモリ使用量が `query_mem_limit` の 80% を超えるとスピリングがトリガーされます。ここで、`query_mem_limit` は単一のクエリが使用できる最大メモリであり、システム変数 `query_mem_limit` によって制御され、デフォルト値は 0 で、制限がないことを示します。
  - 自動スピリングが有効で、クエリがリソースグループにヒットした場合（すべてのシステム組み込みリソースグループを含む）、クエリが次のいずれかの条件を満たすとスピリングがトリガーされます。
    - 現在のリソースグループ内のすべてのクエリが使用するメモリが `current BE node memory limit * mem_limit * spill_mem_limit_threshold` を超える。
    - 現在のクエリが `query_mem_limit` の 80% を超えるメモリを消費する。

上記のリソース消費制限に基づいて、次のパラメータを使用して大規模クエリのリソース消費をさらに制限できます。

- `big_query_cpu_second_limit`: このパラメータは、単一の BE での大規模クエリの CPU 上限時間を指定します。同時クエリが時間を加算します。単位は秒です。このパラメータは、0 より大きく設定された場合にのみ有効です。デフォルト: 0。
- `big_query_scan_rows_limit`: このパラメータは、単一の BE での大規模クエリのスキャン行数の上限を指定します。このパラメータは、0 より大きく設定された場合にのみ有効です。デフォルト: 0。
- `big_query_mem_limit`: このパラメータは、単一の BE での大規模クエリのメモリ使用量の上限を指定します。単位はバイトです。このパラメータは、0 より大きく設定された場合にのみ有効です。デフォルト: 0。

> **注意**
>
> リソースグループで実行されているクエリが上記の大規模クエリ制限を超えると、クエリはエラーで終了します。エラーメッセージは FE ノードの **fe.audit.log** の `ErrorCode` 列でも確認できます。

リソースグループの `type` を `short_query` または `normal` に設定できます。

- デフォルト値は `normal` です。パラメータ `type` に `normal` を指定する必要はありません。
- クエリが `short_query` リソースグループにヒットした場合、BE ノードは `short_query.cpu_core_limit` に指定された CPU リソースを予約します。`normal` リソースグループにヒットしたクエリに予約される CPU リソースは `BE core - short_query.cpu_core_limit` に制限されます。
- `short_query` リソースグループにヒットするクエリがない場合、`normal` リソースグループのリソースには制限が課されません。

> **注意**
>
> - StarRocks クラスターでは、`short_query` リソースグループを最大で 1 つ作成できます。
> - StarRocks は、`short_query` リソースグループに対して CPU リソースのハード上限を設定していません。

#### システム定義のリソースグループ

各 StarRocks インスタンスには、`default_wg` と `default_mv_wg` の 2 つのシステム定義リソースグループがあります。

##### default_wg

`default_wg` は、リソースグループの管理下にあるが、クラシファイア（分類器）に一致しない通常のクエリに割り当てられます。`default_wg` のリソース制限は次のとおりです。

- `cpu_core_limit`: 1（v2.3.7 以前の場合）または BE の CPU コア数（v2.3.7 より後のバージョンの場合）。
- `mem_limit`: 100%。
- `concurrency_limit`: 0。
- `big_query_cpu_second_limit`: 0。
- `big_query_scan_rows_limit`: 0。
- `big_query_mem_limit`: 0。
- `spill_mem_limit_threshold`: 1。

##### default_mv_wg

`default_mv_wg` は、マテリアライズドビュー作成時にプロパティ `resource_group` にリソースグループが割り当てられていない場合、非同期マテリアライズドビューのリフレッシュタスクに割り当てられます。`default_mv_wg` のリソース制限は次のとおりです。

- `cpu_core_limit`: 1。
- `mem_limit`: 80%。
- `concurrency_limit`: 0。
- `spill_mem_limit_threshold`: 80%。

`default_mv_wg` の CPU コア制限、メモリ制限、同時実行制限、およびスピリングしきい値は、BE 設定項目 `default_mv_resource_group_cpu_limit`、`default_mv_resource_group_memory_limit`、`default_mv_resource_group_concurrency_limit`、`default_mv_resource_group_spill_mem_limit_threshold` を変更することで構成できます。

### クラシファイア（分類器）

各クラシファイアは、クエリのプロパティに一致する 1 つ以上の条件を保持します。StarRocks は、各クエリに最も適したクラシファイアを一致条件に基づいて特定し、クエリを実行するためのリソースを割り当てます。

クラシファイアは次の条件をサポートします。

- `user`: ユーザーの名前。
- `role`: ユーザーの役割。
- `query_type`: クエリのタイプ。`SELECT` と `INSERT`（v2.5 からサポート）がサポートされています。INSERT INTO または BROKER LOAD タスクが `query_type` を `insert` とするリソースグループにヒットした場合、BE ノードはタスクに指定された CPU リソースを予約します。
- `source_ip`: クエリが開始される CIDR ブロック。
- `db`: クエリがアクセスするデータベース。カンマ `,` で区切られた文字列で指定できます。
- `plan_cpu_cost_range`: クエリの推定 CPU コスト範囲。形式は `(DOUBLE, DOUBLE]` です。デフォルト値は NULL で、そのような制限がないことを示します。`PlanCpuCost` 列は、クエリの CPU コストに対するシステムの推定値を表します。このパラメータは v3.1.4 以降でサポートされています。
- `plan_mem_cost_range`: クエリのシステム推定メモリコスト範囲。形式は `(DOUBLE, DOUBLE]` です。デフォルト値は NULL で、そのような制限がないことを示します。`PlanMemCost` 列は、クエリのメモリコストに対するシステムの推定値を表します。このパラメータは v3.1.4 以降でサポートされています。

クラシファイアは、クラシファイアの条件のいずれかまたはすべてがクエリに関する情報と一致する場合にのみクエリに一致します。複数のクラシファイアがクエリに一致する場合、StarRocks はクエリと各クラシファイアの一致度を計算し、一致度が最も高いクラシファイアを特定します。

> **注意**
>
> クエリが属するリソースグループは、FE ノードの **fe.audit.log** の `ResourceGroup` 列で確認するか、[クエリのリソースグループを表示する](#view-the-resource-group-of-a-query) で説明されているように `EXPLAIN VERBOSE <query>` を実行して確認できます。

StarRocks は、次のルールを使用してクエリとクラシファイアの一致度を計算します。

- クラシファイアがクエリと同じ `user` の値を持つ場合、クラシファイアの一致度は 1 増加します。
- クラシファイアがクエリと同じ `role` の値を持つ場合、クラシファイアの一致度は 1 増加します。
- クラシファイアがクエリと同じ `query_type` の値を持つ場合、クラシファイアの一致度は 1 増加し、次の計算から得られる数値も加算されます: 1/クラシファイア内の `query_type` フィールドの数。
- クラシファイアがクエリと同じ `source_ip` の値を持つ場合、クラシファイアの一致度は 1 増加し、次の計算から得られる数値も加算されます: (32 - `cidr_prefix`)/64。
- クラシファイアがクエリと同じ `db` の値を持つ場合、クラシファイアの一致度は 10 増加します。
- クエリの CPU コストが `plan_cpu_cost_range` 内に収まる場合、クラシファイアの一致度は 1 増加します。
- クエリのメモリコストが `plan_mem_cost_range` 内に収まる場合、クラシファイアの一致度は 1 増加します。

複数のクラシファイアがクエリに一致する場合、条件の数が多いクラシファイアの方が一致度が高くなります。

```Plain
-- クラシファイア B はクラシファイア A よりも多くの条件を持っています。したがって、クラシファイア B はクラシファイア A よりも一致度が高くなります。

classifier A (user='Alice')

classifier B (user='Alice', source_ip = '192.168.1.0/24')
```

複数の一致するクラシファイアが同じ数の条件を持っている場合、条件がより正確に記述されているクラシファイアの方が一致度が高くなります。

```Plain
-- クラシファイア B に指定された CIDR ブロックは、クラシファイア A よりも範囲が小さいです。したがって、クラシファイア B はクラシファイア A よりも一致度が高くなります。
classifier A (user='Alice', source_ip = '192.168.1.0/16')
classifier B (user='Alice', source_ip = '192.168.1.0/24')

-- クラシファイア C はクラシファイア D よりも指定されたクエリタイプが少ないです。したがって、クラシファイア C はクラシファイア D よりも一致度が高くなります。
classifier C (user='Alice', query_type in ('select'))
classifier D (user='Alice', query_type in ('insert','select'))
```

複数のクラシファイアが同じ一致度を持つ場合、クラシファイアのうちの1つがランダムに選択されます。

```Plain
-- クエリが同時に db1 と db2 をクエリし、クラシファイア E と F がヒットしたクラシファイアの中で最も一致度が高い場合、E と F のいずれかがランダムに選択されます。
classifier E (db='db1')
classifier F (db='db2')
```

## 計算リソースの分離

リソースグループとクラシファイアを構成することで、クエリ間の計算リソースを分離できます。

### リソースグループの有効化

リソースグループを使用するには、StarRocks クラスターで Pipeline Engine を有効にする必要があります。

```SQL
-- 現在のセッションで Pipeline Engine を有効にします。
SET enable_pipeline_engine = true;
-- グローバルに Pipeline Engine を有効にします。
SET GLOBAL enable_pipeline_engine = true;
```

ロードタスクの場合、FE 設定項目 `enable_pipeline_load` を設定して、ロードタスク用の Pipeline エンジンを有効にする必要があります。この項目は v2.5.0 以降でサポートされています。

```sql
ADMIN SET FRONTEND CONFIG ("enable_pipeline_load" = "true");
```

> **注意**
>
> v3.1.0 以降、リソースグループはデフォルトで有効になっており、セッション変数 `enable_resource_group` は非推奨です。

### リソースグループとクラシファイアの作成

次のステートメントを実行して、リソースグループを作成し、クラシファイアと関連付け、リソースグループに計算リソースを割り当てます。

```SQL
CREATE RESOURCE GROUP <group_name> 
TO (
    user='string', 
    role='string', 
    query_type in ('select'), 
    source_ip='cidr'
) --クラシファイアを作成します。複数のクラシファイアを作成する場合、クラシファイアをカンマ（`,`）で区切ります。
WITH (
    "cpu_core_limit" = "INT",
    "mem_limit" = "m%",
    "concurrency_limit" = "INT",
    "type" = "str" --リソースグループのタイプ。値を normal に設定します。
);
```

例:

```SQL
CREATE RESOURCE GROUP rg1
TO 
    (user='rg1_user1', role='rg1_role1', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user2', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user3', source_ip='192.168.x.x/24'),
    (user='rg1_user4'),
    (db='db1')
WITH (
    'cpu_core_limit' = '10',
    'mem_limit' = '20%',
    'big_query_cpu_second_limit' = '100',
    'big_query_scan_rows_limit' = '100000',
    'big_query_mem_limit' = '1073741824'
);
```

### リソースグループの指定（オプション）

現在のセッションに対して直接リソースグループを指定できます。

```SQL
SET resource_group = 'group_name';
```

### リソースグループとクラシファイアの表示

次のステートメントを実行して、すべてのリソースグループとクラシファイアをクエリします。

```SQL
SHOW RESOURCE GROUPS ALL;
```

次のステートメントを実行して、ログインしているユーザーのリソースグループとクラシファイアをクエリします。

```SQL
SHOW RESOURCE GROUPS;
```

次のステートメントを実行して、指定されたリソースグループとそのクラシファイアをクエリします。

```SQL
SHOW RESOURCE GROUP group_name;
```

例:

```plain
mysql> SHOW RESOURCE GROUPS ALL;
+------+--------+--------------+----------+------------------+--------+------------------------------------------------------------------------------------------------------------------------+
| Name | Id     | CPUCoreLimit | MemLimit | ConcurrencyLimit | Type   | Classifiers                                                                                                            |
+------+--------+--------------+----------+------------------+--------+------------------------------------------------------------------------------------------------------------------------+
| rg1  | 300039 | 10           | 20.0%    | 11               | NORMAL | (id=300040, weight=4.409375, user=rg1_user1, role=rg1_role1, query_type in (SELECT), source_ip=192.168.2.1/24)         |
| rg1  | 300039 | 10           | 20.0%    | 11               | NORMAL | (id=300041, weight=3.459375, user=rg1_user2, query_type in (SELECT), source_ip=192.168.3.1/24)                         |
| rg1  | 300039 | 10           | 20.0%    | 11               | NORMAL | (id=300042, weight=2.359375, user=rg1_user3, source_ip=192.168.4.1/24)                                                 |
| rg1  | 300039 | 10           | 20.0%    | 11               | NORMAL | (id=300043, weight=1.0, user=rg1_user4)                                                                                |
+------+--------+--------------+----------+------------------+--------+------------------------------------------------------------------------------------------------------------------------+
```

> **注意**
>
> 上記の例では、`weight` は一致度を示します。

### リソースグループとクラシファイアの管理

各リソースグループのリソースクォータを変更できます。また、リソースグループからクラシファイアを追加または削除できます。

既存のリソースグループのリソースクォータを変更するには、次のステートメントを実行します。

```SQL
ALTER RESOURCE GROUP group_name WITH (
    'cpu_core_limit' = 'INT',
    'mem_limit' = 'm%'
);
```

リソースグループを削除するには、次のステートメントを実行します。

```SQL
DROP RESOURCE GROUP group_name;
```

リソースグループにクラシファイアを追加するには、次のステートメントを実行します。

```SQL
ALTER RESOURCE GROUP <group_name> ADD (user='string', role='string', query_type in ('select'), source_ip='cidr');
```

リソースグループからクラシファイアを削除するには、次のステートメントを実行します。

```SQL
ALTER RESOURCE GROUP <group_name> DROP (CLASSIFIER_ID_1, CLASSIFIER_ID_2, ...);
```

リソースグループのすべてのクラシファイアを削除するには、次のステートメントを実行します。

```SQL
ALTER RESOURCE GROUP <group_name> DROP ALL;
```

## リソースグループの監視

### クエリのリソースグループの表示

クエリがヒットするリソースグループは、**fe.audit.log** の `ResourceGroup` 列、または `EXPLAIN VERBOSE <query>` を実行した後に返される `RESOURCE GROUP` 列から確認できます。これらは、特定のクエリタスクが一致するリソースグループを示します。

- クエリがリソースグループの管理下にない場合、列の値は空の文字列 `""` です。
- クエリがリソースグループの管理下にあるが、クラシファイアに一致しない場合、列の値は空の文字列 `""` です。ただし、このクエリはデフォルトのリソースグループ `default_wg` に割り当てられます。

### リソースグループの監視

リソースグループのために [監視とアラート](../monitoring/Monitor_and_Alert.md) を設定できます。

リソースグループ関連の FE および BE メトリクスは次のとおりです。以下のすべてのメトリクスには、対応するリソースグループを示す `name` ラベルがあります。

### FE メトリクス

次の FE メトリクスは、現在の FE ノード内の統計のみを提供します。

| Metric                                          | Unit | Type          | Description                                                        |
| ----------------------------------------------- | ---- | ------------- | ------------------------------------------------------------------ |
| starrocks_fe_query_resource_group               | Count | Instantaneous | このリソースグループで過去に実行されたクエリの数（現在実行中のものを含む）。 |
| starrocks_fe_query_resource_group_latency       | ms    | Instantaneous | このリソースグループのクエリ遅延パーセンタイル。ラベル `type` は、`mean`、`75_quantile`、`95_quantile`、`98_quantile`、`99_quantile`、`999_quantile` を含む特定のパーセンタイルを示します。 |
| starrocks_fe_query_resource_group_err           | Count | Instantaneous | このリソースグループでエラーが発生したクエリの数。 |
| starrocks_fe_resource_group_query_queue_total   | Count | Instantaneous | このリソースグループで過去にキューに入れられたクエリの総数（現在実行中のものを含む）。このメトリクスは v3.1.4 以降でサポートされています。クエリキューが有効な場合にのみ有効です。 |
| starrocks_fe_resource_group_query_queue_pending | Count | Instantaneous | このリソースグループのキューに現在あるクエリの数。このメトリクスは v3.1.4 以降でサポートされています。クエリキューが有効な場合にのみ有効です。 |
| starrocks_fe_resource_group_query_queue_timeout | Count | Instantaneous | このリソースグループでキューに入れられたままタイムアウトしたクエリの数。このメトリクスは v3.1.4 以降でサポートされています。クエリキューが有効な場合にのみ有効です。 |

### BE メトリクス

| Metric                                      | Unit     | Type          | Description                                                        |
| ----------------------------------------- | -------- | ------------- | ------------------------------------------------------------------ |
| resource_group_running_queries            | Count    | Instantaneous | このリソースグループで現在実行中のクエリの数。   |
| resource_group_total_queries              | Count    | Instantaneous | このリソースグループで過去に実行されたクエリの数（現在実行中のものを含む）。 |
| resource_group_bigquery_count             | Count    | Instantaneous | このリソースグループで大規模クエリ制限をトリガーしたクエリの数。 |
| resource_group_concurrency_overflow_count | Count    | Instantaneous | このリソースグループで `concurrency_limit` 制限をトリガーしたクエリの数。 |
| resource_group_mem_limit_bytes            | Bytes    | Instantaneous | このリソースグループのメモリ制限。                         |
| resource_group_mem_inuse_bytes            | Bytes    | Instantaneous | このリソースグループで現在使用中のメモリ。               |
| resource_group_cpu_limit_ratio            | Percentage | Instantaneous | このリソースグループの `cpu_core_limit` がすべてのリソースグループの `cpu_core_limit` の合計に対する割合。 |
| resource_group_inuse_cpu_cores            | Count     | Average     | このリソースグループで使用中の CPU コアの推定数。この値はおおよその推定値です。これは、2 つの連続したメトリクス収集からの統計に基づいて計算された平均値を表します。このメトリクスは v3.1.4 以降でサポートされています。 |
| resource_group_cpu_use_ratio              | Percentage | Average     | **非推奨** このリソースグループによって使用された Pipeline スレッドタイムスライスがすべてのリソースグループによって使用された Pipeline スレッドタイムスライスの合計に対する割合。これは、2 つの連続したメトリクス収集からの統計に基づいて計算された平均値を表します。 |
| resource_group_connector_scan_use_ratio   | Percentage | Average     | **非推奨** このリソースグループによって使用された外部テーブルスキャンスレッドタイムスライスがすべてのリソースグループによって使用された Pipeline スレッドタイムスライスの合計に対する割合。これは、2 つの連続したメトリクス収集からの統計に基づいて計算された平均値を表します。 |
| resource_group_scan_use_ratio             | Percentage | Average     | **非推奨** このリソースグループによって使用された内部テーブルスキャンスレッドタイムスライスがすべてのリソースグループによって使用された Pipeline スレッドタイムスライスの合計に対する割合。これは、2 つの連続したメトリクス収集からの統計に基づいて計算された平均値を表します。 |

### リソースグループの使用情報の表示

v3.1.4 以降、StarRocks は SQL ステートメント [SHOW USAGE RESOURCE GROUPS](../../../sql-reference/sql-statements/cluster-management/resource_group/SHOW_USAGE_RESOURCE_GROUPS.md) をサポートしており、BE 全体での各リソースグループの使用情報を表示するために使用されます。各フィールドの説明は次のとおりです。

- `Name`: リソースグループの名前。
- `Id`: リソースグループの ID。
- `Backend`: BE の IP または FQDN。
- `BEInUseCpuCores`: この BE 上でこのリソースグループによって現在使用されている CPU コアの数。この値はおおよその推定値です。
- `BEInUseMemBytes`: この BE 上でこのリソースグループによって現在使用されているメモリバイト数。
- `BERunningQueries`: この BE 上でまだ実行中のこのリソースグループからのクエリの数。

注意点：

- BE は `report_resource_usage_interval_ms` に指定された間隔でリソース使用情報を Leader FE に定期的に報告します。デフォルトでは 1 秒に設定されています。
- 結果には、`BEInUseCpuCores`/`BEInUseMemBytes`/`BERunningQueries` のいずれかが正の数である行のみが表示されます。つまり、リソースグループが BE 上で積極的にリソースを使用している場合にのみ情報が表示されます。

例:

```Plain
MySQL [(none)]> SHOW USAGE RESOURCE GROUPS;
+------------+----+-----------+-----------------+-----------------+------------------+
| Name       | Id | Backend   | BEInUseCpuCores | BEInUseMemBytes | BERunningQueries |
+------------+----+-----------+-----------------+-----------------+------------------+
| default_wg | 0  | 127.0.0.1 | 0.100           | 1               | 5                |
+------------+----+-----------+-----------------+-----------------+------------------+
| default_wg | 0  | 127.0.0.2 | 0.200           | 2               | 6                |
+------------+----+-----------+-----------------+-----------------+------------------+
| wg1        | 0  | 127.0.0.1 | 0.300           | 3               | 7                |
+------------+----+-----------+-----------------+-----------------+------------------+
| wg2        | 0  | 127.0.0.1 | 0.400           | 4               | 8                |
+------------+----+-----------+-----------------+-----------------+------------------+
```
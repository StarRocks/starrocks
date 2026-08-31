---
displayed_sidebar: docs
description: "v2.5以降、StarRocks はクエリキューで同時実行やリソース制限に達時にクエリを自動的にキューイングします。"
sidebar_position: 20
---

# クエリキュー

このトピックでは、StarRocks におけるクエリキューの管理方法について説明します。

v2.5 から、StarRocks はクエリキューをサポートしています。クエリキューが有効になると、同時実行のしきい値やリソース制限に達した場合に、StarRocks は自動的に受信クエリをキューに入れ、過負荷の悪化を避けます。保留中のクエリは、実行を開始するのに十分な計算リソースが利用可能になるまでキューで待機します。

クエリキューには 2 つのバージョンがあります。

- [**Query Queue v1**](#query-queue-v1): クエリの同時実行数、BE のメモリ使用率、および BE の CPU 使用率に基づいてキューイングをトリガーします。このトピックの既存のクエリキュー設定と動作は v1 に属します。v3.1.4 以降、v1 はリソースグループレベルでのクエリキュー設定をサポートしています。
- [**Query Queue v2**](#query-queue-v2): v3.3 以降でサポートされています。v2 は各 Query が消費する BE リソースを見積もり、BE リソースを論理 slot として表現し、各 Query が必要とする slot 数に基づいてキューイングとスケジューリングを行います。

## Query Queue v1

Query Queue v1 では、CPU 使用率、メモリ使用率、クエリの同時実行数にしきい値を設定して、クエリキューをトリガーできます。

**ロードマップ**:

| Version | Global query queue | Resource group-level query queue | Collective concurrency management | Dynamic concurrency adjustment  |
| ------  | ------------------ | -------------------------------- | --------------------------------- | ------------------------------- |
| v2.5    | ✅                 | ❌                                | ❌                                | ❌                              |
| v3.1.4  | ✅                 | ✅                                | ✅                                | ✅                              |

### Query Queue v1 を有効にする

クエリキューはデフォルトで無効になっています。INSERT ロード、SELECT クエリ、および統計クエリに対して、対応するグローバルセッション変数を設定することで、グローバルまたはリソースグループレベルのクエリキューを有効にできます。

#### グローバルクエリキューを有効にする

- ロードタスクのクエリキューを有効にする:

```SQL
SET GLOBAL enable_query_queue_load = true;
```

- SELECT クエリのクエリキューを有効にする:

```SQL
SET GLOBAL enable_query_queue_select = true;
```

- 統計クエリのクエリキューを有効にする:

```SQL
SET GLOBAL enable_query_queue_statistic = true;
```

#### リソースグループレベルのクエリキューを有効にする

v3.1.4 以降、StarRocks はリソースグループレベルでのクエリキューの設定をサポートしています。

リソースグループレベルのクエリキューを有効にするには、上記のグローバルセッション変数に加えて `enable_group_level_query_queue` を設定する必要があります。

```SQL
SET GLOBAL enable_group_level_query_queue = true;
```

:::note

共有データクラスターでは、これらのグローバルセッション変数でクエリキューを有効にすることはできません。クエリキューはウェアハウスごとに個別に有効化する必要があり、`enable_group_level_query_queue` はウェアハウスプロパティ `enable_query_queue` を `true` に設定した後にのみ有効になります。詳細は [共有データクラスターにおける Query Queue v2](#共有データクラスターにおける-query-queue-v2) を参照してください。

:::

### リソースしきい値を指定する

#### グローバルクエリキューのリソースしきい値を指定する

次のグローバルセッション変数を使用して、クエリキューをトリガーするしきい値を設定できます。

| **Variable**                        | **Default** | **Description**                                              |
| ----------------------------------- | ----------- | ------------------------------------------------------------ |
| query_queue_concurrency_limit       | 0           | BE 上の同時クエリの上限。`0` より大きく設定された場合にのみ有効になります。`0` に設定すると、制限がないことを示します。 |
| query_queue_mem_used_pct_limit      | 0           | BE 上のメモリ使用率の上限。`0` より大きく設定された場合にのみ有効になります。`0` に設定すると、制限がないことを示します。範囲: [0, 1] |
| query_queue_cpu_used_permille_limit | 0           | BE 上の CPU 使用率のパーミル (CPU 使用率 * 1000) の上限。`0` より大きく設定された場合にのみ有効になります。`0` に設定すると、制限がないことを示します。範囲: [0, 1000] |

:::note

- Query Queue v2 を有効にすると、`query_queue_mem_used_pct_limit` および `query_queue_cpu_used_permille_limit` によるキューイングのトリガーはサポートされません。
- デフォルトでは、BE は 1 秒間隔でリソース使用状況を FE に報告します。この間隔は、BE の設定項目 `report_resource_usage_interval_ms` を設定することで変更できます。

:::

#### リソースグループレベルのクエリキューのリソースしきい値を指定する

v3.1.4 以降、リソースグループを作成する際に、個別の同時実行制限 (`concurrency_limit`) と CPU コア制限 (`max_cpu_cores`) を設定できます。また、リソースグループのメモリ使用率しきい値 (`mem_used_pct_limit`) を設定することもできます。クエリが開始されると、グローバルまたはリソースグループレベルのいずれかでリソース消費がリソースしきい値を超えた場合、すべてのリソース消費がしきい値内に収まるまでクエリはキューに入れられます。

| **Variable**       | **Default** | **Description**                                                                                                                                                                       |
|--------------------|-------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| concurrency_limit  | 0           | 単一の BE ノード上のリソースグループの同時実行制限。`0` より大きく設定された場合にのみ有効になります。                                                                                    |
| max_cpu_cores      | 0           | 単一の BE ノード上のこのリソースグループの CPU コア制限。`0` より大きく設定された場合にのみ有効になります。範囲: [0, `avg_be_cpu_cores`]、ここで `avg_be_cpu_cores` はすべての BE ノードの平均 CPU コア数を表します。 |
| mem_used_pct_limit | 0           | 単一の BE ノード上のこのリソースグループのメモリ使用率制限。`0` より大きく設定された場合にのみ有効になります。範囲: [0, 1]                                                                |

`mem_used_pct_limit` は Query Queue v1 にのみ適用されます。Query Queue v2 を有効にする（`enable_query_queue_v2` を `true` に設定する）と、このパラメータは有効になりません。

各 BE ノードのリソースグループごとのリソース使用情報を表示するには、[View Resource Group Usage Information](./resource_group.md#view-resource-group-usage-information) を参照してください。

#### クエリの同時実行数を管理する

実行中のクエリの数 (`num_running_queries`) がグローバルまたはリソースグループの `concurrency_limit` を超えると、受信クエリはキューに入れられます。`num_running_queries` を取得する方法は、バージョン < v3.1.4 と > v3.1.4 の間で異なります。

- バージョン < v3.1.4 では、`num_running_queries` は `report_resource_usage_interval_ms` で指定された間隔で BEs によって報告されます。したがって、`num_running_queries` の変化の特定に遅延が生じる可能性があります。たとえば、BEs によって報告された `num_running_queries` がグローバルまたはリソースグループの `concurrency_limit` を超えていない場合でも、次の報告前に受信クエリが到着して `concurrency_limit` を超えた場合、これらの受信クエリはキューで待機せずに実行されます。

- バージョン > v3.1.4 では、すべての実行中のクエリは Leader FE によって集中的に管理されます。各 Follower FE はクエリの開始または終了時に Leader FE に通知し、StarRocks が `concurrency_limit` を超えるクエリの急増に対応できるようにします。

### Query Queue v1 を設定する

クエリキューの容量とキュー内のクエリの最大タイムアウトを次のグローバルセッション変数を使用して設定できます。

| **Variable**                       | **Default** | **Description**                                              |
| ---------------------------------- | ----------- | ------------------------------------------------------------ |
| query_queue_max_queued_queries     | 1024        | キュー内のクエリの上限。このしきい値に達すると、受信クエリは拒否されます。`0` より大きく設定された場合にのみ有効になります。 |
| query_queue_pending_timeout_second | 300         | キュー内の保留中のクエリの最大タイムアウト。このしきい値に達すると、対応するクエリは拒否されます。単位: 秒。 |

### クエリ同時実行数の動的調整を設定する

バージョン v3.1.4 以降、クエリキューによって管理され、Pipeline Engine によって実行されるクエリに対して、StarRocks は現在の実行中のクエリの数 `num_running_queries`、フラグメントの数 `num_fragments`、およびクエリの同時実行数 `pipeline_dop` に基づいて、受信クエリのクエリの同時実行数 `pipeline_dop` を動的に調整できます。これにより、スケジューリングのオーバーヘッドを最小限に抑えながらクエリの同時実行数を動的に制御し、最適な BE リソースの利用を確保できます。フラグメントとクエリの同時実行数 `pipeline_dop` についての詳細は、[Query Management - Adjusting Query Concurrency](./Query_management.md) を参照してください。

クエリキューの下での各クエリに対して、StarRocks はドライバーの概念を維持します。これは、単一の BE 上のクエリの同時フラグメントを表します。その論理値 `num_drivers` は、単一の BE 上のそのクエリのすべてのフラグメントの総同時実行数を表し、`num_fragments * pipeline_dop` に等しいです。新しいクエリが到着すると、StarRocks は次のルールに基づいてクエリの同時実行数 `pipeline_dop` を調整します。

- 実行中のドライバーの数 `num_drivers` がクエリの同時ドライバーの低水位制限 `query_queue_driver_low_water` を超えるほど、クエリの同時実行数 `pipeline_dop` は低く調整されます。
- StarRocks は、クエリの同時ドライバーの高水位制限 `query_queue_driver_high_water` を下回るように実行中のドライバーの数 `num_drivers` を抑制します。

クエリの同時実行数 `pipeline_dop` の動的調整を次のグローバルセッション変数を使用して設定できます。

| **Variable**                  | **Default** | **Description**                                             |
| ----------------------------- | ----------- | ----------------------------------------------------------- |
| query_queue_driver_high_water | -1          | クエリの同時ドライバーの高水位制限。非負の値に設定された場合にのみ有効になります。`0` に設定すると、`avg_be_cpu_cores * 16` に相当します。ここで `avg_be_cpu_cores` はすべての BE ノードの平均 CPU コア数を表します。`0` より大きい値に設定すると、その値が直接使用されます。 |
| query_queue_driver_low_water  | -1          | クエリの同時ドライバーの低水位制限。非負の値に設定された場合にのみ有効になります。`0` に設定すると、`avg_be_cpu_cores * 8` に相当します。`0` より大きい値に設定すると、その値が直接使用されます。 |

## Query Queue v2

v3.3 以降、StarRocks は Query Queue v2 をサポートしています。v2 は、クエリの同時実行数、BE のメモリ使用率、または BE の CPU 使用率の固定しきい値に基づいてキューイングをトリガーしません。代わりに、各 Query が必要とする BE リソースを見積もり、論理 slot に基づいてキューイングとスケジューリングを行います。利用可能なスロットが不足している場合、クエリは十分なスロットが解放されるまでキューで待機します。

### Query Queue v2 を設定する

共有なしクラスターでは、Query Queue v2 は FE 設定項目で有効化および調整します。`enable_query_queue_v2` の変更を有効にするには、FE ノードの再起動が必要です。

:::note

共有データクラスターでは、`enable_query_queue_v2` は使用されません。Query Queue v2 はウェアハウスごとに個別に有効化および調整します。詳細は [共有データクラスターにおける Query Queue v2](#共有データクラスターにおける-query-queue-v2) を参照してください。

:::

| 設定項目 | デフォルト | 説明 |
| -------- | ---------- | ---- |
| `enable_query_queue_v2` | `false` (v3.3 から v4.0)<br />`true` (v4.1 以降) | Query Queue v2 を有効にするかどうか。`true` に設定すると、StarRocks は v2 の slot ベースのクエリスケジューリングメカニズムを使用します。この設定項目は共有なしクラスターにのみ適用されます。 |
| `query_queue_v2_concurrency_level` | `4` | Query Queue v2 がクラスタ全体の slot 総数を計算するときに使用する論理同時実行レベル。値が大きいほど、システムが受け入れられる Query が増えます。これは相対的な調整パラメータです。 |
| `query_queue_v2_concurrency_level` | `4` | Query Queue V2 がクラスター全体のスロット数を計算する際に使用する論理的な同時実行レベルです。この値を大きくすると、より多くのクエリを同時に受け入れられるようになります。この値は相対的なチューニングパラメータです。 |
| `query_queue_slots_estimator_strategy` | `PBE` | キューイングされたクエリに対して使用するスロット推定方式を指定します。有効な値は `PBE`（Parallelism-Based、デフォルト）、`MBE`（Memory-Based Estimation）、`CBE`（CPU-Based Estimation）です。PBE は、スキャン並列度に基づいて必要なスロット数を推定し、その上限をワーカー数とします。OLAP テーブルでは、プルーニング後に残ったスキャンレンジ数を使用して推定するため、ごく小規模なクエリのみがワーカー数未満のスロット数になります。Connector や外部テーブルのスキャンは、単一スロットのクエリではなく、ワーカー数と同じ並列度を持つスキャンとして扱われます。MBE は、クエリのメモリコストを `query_queue_v2_mem_bytes_per_slot` で割ってスロット数を推定します。CBE は、実行プランの CPU コストを `query_queue_v2_cpu_costs_per_slot` で割ってスロット数を推定します。MBE および CBE で算出されたスロット数は、さらに `number_of_workers * max(1, pipeline_dop / 2)` を上限として制限されます。従来の `MAX` および `MIN` も前方互換性のため引き続き指定できますが、いずれもデフォルトの推定方式として扱われます。それ以外の値を指定した場合は、設定の検証時に拒否されます。 |
| `query_queue_v2_schedule_strategy` | `SWRR` | Query Queue V2 が待機中のクエリを実行順に並べる際のスケジューリングポリシーを指定します。指定可能な値（大文字・小文字は区別されません）は、`SWRR`（Smooth Weighted Round Robin、デフォルト）と `SJF`（Short Job First + Aging）です。`SWRR` は、重み付けを考慮した公平なスケジューリングを行うため、混在したワークロードに適しています。`SJF` は、短時間で終了するクエリを優先しつつ、エージングによってスターベーションを防止します。認識できない値を指定した場合はエラーがログに記録され、デフォルトのスケジューリングポリシーが使用されます。この設定は Query Queue V2 が有効な場合にのみ有効であり、`query_queue_v2_concurrency_level` などの V2 の容量設定と組み合わせて動作します。 |
| `query_queue_v2_mem_bytes_per_slot` | `0` | メモリベース推定方式（MBE）で使用する、1 スロットあたりのメモリ目標値です。`query_queue_slots_estimator_strategy` が `MBE` の場合、総スロット数はウェアハウス全体のメモリ予算から算出され、各クエリのスロット数はクエリ全体のメモリコストをこの値で割って推定されます。その後、`number_of_workers * max(1, pipeline_dop / 2)` を上限として制限されます。この値が 0 以下の場合、Query Queue V2 はワーカーあたりの平均コアメモリ容量を使用します。 |
| `query_queue_v2_cpu_costs_per_slot` | `1000000000` | CPU ベース推定方式（CBE）で使用する、1 スロットあたりの CPU コストしきい値です。スケジューラは `ceil(plan_cpu_costs / query_queue_v2_cpu_costs_per_slot)` によって必要スロット数を算出し、その結果を `[1, min(totalSlots, number_of_workers * max(1, pipeline_dop / 2))]` の範囲に制限します。この値が 0 以下の場合は `1` として扱われます。この値を大きくすると、各クエリに割り当てられるスロット数が減少し、同時実行性が高くなります。逆に値を小さくすると、各クエリがより多くのスロットを使用するため、同時実行性は低くなります。 |
| `query_queue_concurrency_limit` | `0` | BE ごとの同時実行クエリ数の上限を指定します。この設定は `0` より大きい値を指定した場合のみ有効です。`0` を指定すると、同時実行数に制限はありません。 |

:::note

`query_queue_mem_used_pct_limit` および `query_queue_cpu_used_permille_limit` は Query Queue v1 にのみ適用されます。Query Queue v2 を有効にすると、これらのパラメータは有効になりません。

:::

### リソース Slot

Query Queue v2 は BE リソースを論理 Slot として表現します。

- **クラスタ全体の Slot 総数**: StarRocks はクラスタ全体に対して論理的な Slot 総数を設定します。この総数は BE 数と BE CPU Core 数に正の相関があり、`query_queue_v2_concurrency_level` の影響も受けます。
- **Query が必要とする Slot 数**: StarRocks は各 Query が必要とする Slot 数を見積もります。見積もりは、統計情報、クエリの複雑度、Fragment 数、複雑なオペレーターの入力および出力データ量の推定、DOP などに基づきます。

### キューイングロジック

ある Query が必要とする Slot 数が現在の残り Slot 数を超える場合、その Query はキューで待機します。Query Queue v2 は、必要な Slot 数が少ない Query を優先的に満たすことで、小さいクエリが先にリソースを取得できるようにし、大きいクエリがキューの先頭に長時間とどまって後続の小さいクエリをブロックするヘッドオブラインブロッキング (Head-of-line blocking) を避けます。

キューイングロジック全体は FE 上で完了します。これには、クラスタ全体の Slot 総数の設定、Query が必要とする Slot 数の見積もり、およびどの Query の Slot 要求を優先的に満たすかの決定が含まれます。Query Queue v2 は、BE の実際のリソース使用状況に基づいてスケジューリングを行いません。

### 推定方式を選択する

#### PBE

Parallel-Based Estimation（PBE）は、次のようなワークロードに適しています。

- 一般的なレポートクエリ
- ポイントルックアップと大規模クエリが混在するワークロード
- コストモデルの詳細を意識せず運用したいユーザー
- シンプルで予測しやすいキューイング動作を重視する DBA

PBE を使用すると、次のような動作が期待できます。

- プルーニング後のスキャン対象が少ないポイントルックアップや小規模クエリは、より少ないスロットを使用します。
- 広範囲をスキャンするクエリは、より多くのスロットを使用します。
- ピーク時でも、小規模クエリは実行リソースを確保しやすくなります。

以下の例では、PBE を推定方式として設定します。

```SQL
ADMIN SET FRONTEND CONFIG ("query_queue_slots_estimator_strategy" = "PBE");
```

#### MBE

Memory-Based Estimation（MBE）は、大規模 JOIN、大規模集約、高カーディナリティ集約など、メモリ負荷が高いワークロードに適しています。

以下の例では、MBE を推定方式として設定し、各スロットに 2 GB のメモリを割り当てます。

```SQL
ADMIN SET FRONTEND CONFIG ("query_queue_slots_estimator_strategy" = "MBE");
ADMIN SET FRONTEND CONFIG ("query_queue_v2_mem_bytes_per_slot" = "2147483648");
```

MBE では、クエリ全体のメモリコストをこの値で割ってクエリごとのスロット数を算出し、ウェアハウス全体のメモリ予算をこの値で割って総スロット数を算出します。

MBE のチューニングでは、次の点を参考にしてください。

**症状: メモリは相変わらずすぐにいっぱいになってしまう**

- **調整**: `query_queue_v2_concurrency_level` を小さくします。
- **効果**: MBE が使用する総メモリ予算を直接減らします。

**症状: キューが長すぎるが、BEメモリにはまだ空きがある**

- **調整**: `query_queue_v2_concurrency_level` を大きくします。
- **効果**: MBE が使用する総メモリ予算を直接増やします。

**症状: `max_slots` の値が非常に小さく、整数の丸め誤差が目立つ**

- **調整**: `query_queue_v2_mem_bytes_per_slot` を小さくします。
- **効果**: メモリスロットの粒度が細かくなり、整数丸めによる誤差を軽減できます。

#### CBE

CPU-Based Estimation（CBE）は、CPU 集約型の SQL、複雑な式を含むクエリ、スキャン後の CPU 処理負荷が高いワークロードに適しています。

以下の例では、CBE を推定方式として設定し、各スロットの CPU コストしきい値を `1000000000` に設定します。

```SQL
ADMIN SET FRONTEND CONFIG ("query_queue_slots_estimator_strategy" = "CBE");
ADMIN SET FRONTEND CONFIG ("query_queue_v2_cpu_costs_per_slot" = "1000000000");
```

**症状: CPU が頻繁にフル稼働状態になる**

- **調整**: `query_queue_v2_cpu_costs_per_slot` を小さくします。
- **効果**: 同じ CPU コストに対してより多くのスロットを割り当てるため、同時実行性が抑えられます。

**症状: クエリの待ち行列は目立って長くなっているが、CPU にはまだ余裕がある**

- **調整**: `query_queue_v2_cpu_costs_per_slot` を大きくします。
- **効果**: 同じ CPU コストに対して必要スロット数が減るため、同時実行性が高くなります。

### 同時実行容量を調整する

全体の同時実行性のみを調整したい場合は、PBE、MBE、CBE を切り替える前に、まず総スロット容量を調整してください。

```SQL
ADMIN SET FRONTEND CONFIG ("query_queue_v2_concurrency_level" = "<value>");
```

推奨される調整手順は次のとおりです。

1. まずデフォルト値 `4` を使用します。
2. `remain_slots`、`max_slots`、`query_pending_length`、CPU 使用率、メモリ使用率、およびクエリレイテンシを監視します。
3. リソースに余裕があるにもかかわらず待機クエリが多い場合は、`query_queue_v2_concurrency_level` を段階的に増やします。
4. リソースが頻繁に飽和する、またはクエリ間のリソース競合が大きい場合は、`query_queue_v2_concurrency_level` を段階的に減らします。
5. 一度に変更する値は 10%～25% 程度に留め、次の調整を行う前に少なくとも 1 回の業務ピーク時間帯の挙動を確認してください。

**チューニングの優先順位**: まず `query_queue_v2_concurrency_level` を使用して全体の容量を調整してください。その後、必要に応じて MBE や PBE への切り替えを検討します。初期段階では複数のパラメータを同時に変更しないでください。どのパラメータが効果をもたらしたのか判断しにくくなります。

#### フォールバック同時実行数上限

`query_queue_concurrency_limit` はフォールバックとして機能する同時実行数の上限であり、PBE、MBE、および CBE のすべてに適用されます。Query Queue V2 は、まず現在の推定方式でクエリに必要なスロット数を算出し、十分なスロットがあるかを確認します。その後、現在実行中のクエリ数が `query_queue_concurrency_limit` に達しているかを確認します。

デフォルト値 `0` は無制限を意味します。実行中のクエリ数に絶対的な上限を設けたい場合にのみ設定してください。

```SQL
ALTER WAREHOUSE default_warehouse SET ("query_queue_concurrency_limit" = "8");
```

まず `query_queue_v2_concurrency_level` を使用してリソース容量を調整してください。`query_queue_concurrency_limit` は、同時に実行できるクエリ数を明示的に制限する必要がある場合にのみ使用してください。

## 共有データクラスターにおける Query Queue v2

共有データクラスターでは、コンピュートリソースはウェアハウスとして構成され、各ウェアハウスが独自のクエリキューを持ちます。そのため、Query Queue v2 は FE 設定項目ではなく、ウェアハウスごとに有効化および調整します。

:::warning

ウェアハウスのクエリキューは、その `enable_query_queue` プロパティが `true` に設定されるまで無効のままです。このプロパティが `false`（デフォルト）である間、そのウェアハウスのクエリはキューイングされず、リソースグループの `concurrency_limit` を超えたクエリは、キューで待機せずに `Exceed concurrency limit` エラーで拒否されます。

:::

### ウェアハウスのクエリキューを有効にする

ウェアハウスのクエリキューはデフォルトで無効です。ウェアハウスプロパティ `enable_query_queue` を設定して有効にします。

```SQL
ALTER WAREHOUSE <warehouse_name> SET ("enable_query_queue" = "true");
```

この変更は即座に反映されます。FE ノードの再起動は不要です。

`enable_query_queue` が `true` になると、SELECT クエリは常にキューイングされます。SELECT クエリ専用のプロパティはありません。ロードタスクと統計クエリもキューイングする場合は、対応するプロパティを設定します。

```SQL
ALTER WAREHOUSE <warehouse_name> SET ("enable_query_queue_load" = "true");
ALTER WAREHOUSE <warehouse_name> SET ("enable_query_queue_statistic" = "true");
```

ウェアハウス単位に加えてリソースグループ単位でもキューイングする場合は、グローバルセッション変数 `enable_group_level_query_queue` を設定します。この変数は `enable_query_queue` が `true` になって初めて有効になります。

```SQL
SET GLOBAL enable_group_level_query_queue = true;
```

### クエリキューに関するウェアハウスプロパティ

| プロパティ | デフォルト | 説明 |
| ---------- | ---------- | ---- |
| `enable_query_queue` | `false` | このウェアハウスでクエリキューを有効にするかどうか。マスタースイッチであり、`false` の場合、このウェアハウスのクエリはキューイングされません。 |
| `enable_query_queue_load` | `false` | このウェアハウスのロードタスクをキューイングするかどうか。`enable_query_queue` が `true` の場合にのみ有効です。 |
| `enable_query_queue_statistic` | `false` | このウェアハウスの統計クエリをキューイングするかどうか。`enable_query_queue` が `true` の場合にのみ有効です。 |
| `query_queue_concurrency_limit` | `-1` | このウェアハウスで同時に実行できるクエリ数の上限。`0` より大きい値を設定した場合にのみ有効です。`0` 以下の値は無制限を意味します。 |
| `query_queue_max_queued_queries` | `1024` | このウェアハウスのキューで待機できるクエリ数の上限。この数に達した後に到着したクエリは、キューイングされずに拒否されます。 |
| `query_queue_pending_timeout_second` | `600` | クエリがこのウェアハウスのキューで待機できる最大時間（秒）。 |
| `query_queue_slots_estimator_strategy` | 同名の FE 設定項目に従う | v4.1 以降でサポートされます。このウェアハウスで使用するスロット推定方式。有効な値: `PBE`、`MBE`、`CBE`。詳細は [推定方式を選択する](#推定方式を選択する) を参照してください。 |
| `query_queue_v2_concurrency_level` | 同名の FE 設定項目に従う | v4.1 以降でサポートされます。このウェアハウスの総スロット数を計算する際に使用する論理的な同時実行レベル。詳細は [同時実行容量を調整する](#同時実行容量を調整する) を参照してください。 |
| `query_queue_v2_mem_bytes_per_slot` | 同名の FE 設定項目に従う | v4.1 以降でサポートされます。このウェアハウスの MBE 推定方式で使用する 1 スロットあたりのメモリ目標値。 |
| `query_queue_v2_cpu_costs_per_slot` | 同名の FE 設定項目に従う | v4.1 以降でサポートされます。このウェアハウスの CBE 推定方式で使用する 1 スロットあたりの CPU コストしきい値。 |
| `query_queue_v2_schedule_strategy` | 同名の FE 設定項目に従う | v4.1 以降でサポートされます。このウェアハウスの待機中のクエリを並べ替える際に使用するスケジューリングポリシー。有効な値: `SWRR`、`SJF`。 |

最後の 5 つのプロパティは、このウェアハウスに限り同名の FE 設定項目を上書きします。設定されていない場合は、FE レベルの値が使用されます。このうち 2 つの文字列プロパティは、一度設定すると未設定の状態には戻せません。上書きを解除するには、FE レベルの値を明示的に設定し直してください。

複数のプロパティを 1 つのステートメントで設定できます。

```SQL
ALTER WAREHOUSE <warehouse_name> SET (
    "enable_query_queue" = "true",
    "query_queue_v2_concurrency_level" = "8",
    "query_queue_v2_schedule_strategy" = "SJF"
);
```

### クエリキューが機能していることを確認する

ウェアハウスのクエリキューを有効にした後、次の方法でクエリがキューイングされていることを確認できます。

- [SHOW RUNNING QUERIES](#show-running-queries) を実行します。キューイングされたクエリの状態は `PENDING` になり、`Slots` 列には各クエリに必要と推定されたスロット数が表示されます。
- [SHOW PROCESSLIST](#show-processlist) を実行します。キューイングされたクエリの `IsPending` 列は `true` になります。
- `SHOW WAREHOUSES` を実行します。`Property` 列にはそのウェアハウスで有効なクエリキュープロパティが表示されます。`RunningSql` と `QueuedSql` の各列は未実装で常に `0` を返すため、現在の負荷は以下の `warehouse_metrics` で確認してください。
- `information_schema` の [warehouse_metrics](../../../sql-reference/information_schema/warehouse_metrics.md) ビューをクエリします。`QUEUE_PENDING_LENGTH` は現在待機中のクエリ数、`REMAIN_SLOTS` と `MAX_SLOTS` はそのウェアハウスの残りスロット数と総スロット数です。クエリキューが有効なウェアハウスのみがこのビューに表示されるため、結果が空であること自体が `enable_query_queue` がまだ `false` であることを示します。

  ```SQL
  SELECT WAREHOUSE_NAME, QUEUE_PENDING_LENGTH, QUEUE_RUNNING_LENGTH, REMAIN_SLOTS, MAX_SLOTS
  FROM information_schema.warehouse_metrics;
  ```

- **fe.audit.log** の `PendingTimeMs` フィールドを確認します。`0` より大きい値は、そのクエリがキューで待機したことを示します。
- FE の HTTP ポートで公開される `starrocks_fe_warehouse_query_queue` メトリクスをモニタリングします（例: `starrocks_fe_warehouse_query_queue{field="query_pending_length"}`）。詳細は [ウェアハウスのモニタリングメトリクス](../monitoring/metrics-warehouse_queue.md) を参照してください。

クエリが一度もキューイングされない場合は、クエリを実行するウェアハウスで `enable_query_queue` が `true` に設定されているかを確認し、テストクエリが実際にテーブルをスキャンしているかも確認してください。スキャンノードを持たないクエリはキューに入らないため、待機もせず、スロットも消費しません。`SELECT sleep(10)`、`SELECT 1`、および `information_schema` のみを読み取るクエリはいずれもこれに該当し、クエリキューの動作確認には適していません。

### ウェアハウスを調整する

[推定方式を選択する](#推定方式を選択する) および [同時実行容量を調整する](#同時実行容量を調整する) のチューニング指針は、共有データクラスターにも適用されます。ただし、変更をクラスター全体ではなく単一のウェアハウスに適用するため、`ADMIN SET FRONTEND CONFIG` ではなく `ALTER WAREHOUSE` を使用します。

たとえば、単一のウェアハウスのスロット容量を増やすには、次のようにします。

```SQL
ALTER WAREHOUSE <warehouse_name> SET ("query_queue_v2_concurrency_level" = "8");
```

単一のウェアハウスをメモリコストベースの推定方式に切り替え、各スロットに 2 GB のメモリを割り当てるには、次のようにします。

```SQL
ALTER WAREHOUSE <warehouse_name> SET (
    "query_queue_slots_estimator_strategy" = "MBE",
    "query_queue_v2_mem_bytes_per_slot" = "2147483648"
);
```

## Monitor query queues

次の方法を使用して、クエリキューに関連する情報を表示できます。

### SHOW PROC

[SHOW PROC](../../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_PROC.md) を使用して、BE ノードでの実行中のクエリの数、メモリおよび CPU 使用率を確認できます。

```Plain
mysql> SHOW PROC '/backends'\G
*************************** 1. row ***************************
...
    NumRunningQueries: 0
           MemUsedPct: 0.79 %
           CpuUsedPct: 0.0 %
```

### SHOW PROCESSLIST

[SHOW PROCESSLIST](../../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_PROCESSLIST.md) を使用して、クエリがキューに入っているかどうか (`IsPending` が `true` の場合) を確認できます。共有データクラスターでは、`Warehouse` 列にそのクエリが実行されるウェアハウスが表示されます。

```Plain
mysql> SHOW PROCESSLIST;
+---------------------------------+----------+------+---------------------+------+---------+---------------------+------+-------+------------------+-----------+-------------------+---------------------+-----------------+--------------------------------------+
| ServerName                      | Id       | User | Host                | Db   | Command | ConnectionStartTime | Time | State | Info             | IsPending | Warehouse         | CNGroup             | Catalog         | QueryId                              |
+---------------------------------+----------+------+---------------------+------+---------+---------------------+------+-------+------------------+-----------+-------------------+---------------------+-----------------+--------------------------------------+
| 127.00.00.01_9010_1787542926940 | 33554554 | root | xxx.xx.xxx.xx:xxxxx |      | Query   | 2026-08-24 15:08:08 |    0 | OK    | SHOW PROCESSLIST | false     | default_warehouse | _builtin_cngroup_0_ | default_catalog | 01a03299-1521-77ee-ab7e-ec1387a3beb6 |
+---------------------------------+----------+------+---------------------+------+---------+---------------------+------+-------+------------------+-----------+-------------------+---------------------+-----------------+--------------------------------------+
```

### FE audit log

FE の監査ログファイル **fe.audit.log** を確認できます。フィールド `PendingTimeMs` は、クエリがキューで待機していた時間を示し、その単位はミリ秒です。

### Monitoring metrics

[Monitor and Alert](../monitoring/monitoring.md) 機能を使用して、StarRocks のクエリキューのメトリクスを取得できます。次の FE メトリクスは、各 FE ノードの統計データから導出されます。

| Metric                                          | Unit | Type    | Description                                                    |
| ----------------------------------------------- | ---- | ------- | -------------------------------------------------------------- |
| starrocks_fe_query_queue_pending                | Count | Instantaneous | 現在キューにあるクエリの数。                                  |
| starrocks_fe_query_queue_total                  | Count | Instantaneous | キューに入れられたクエリの総数 (現在実行中のものを含む)。      |
| starrocks_fe_query_queue_timeout                | Count | Instantaneous | キュー内でタイムアウトしたクエリの総数。                      |
| starrocks_fe_resource_group_query_queue_total   | Count | Instantaneous | このリソースグループでキューに入れられたクエリの総数 (現在実行中のものを含む)。`name` ラベルはリソースグループの名前を示します。このメトリクスは v3.1.4 以降でサポートされています。 |
| starrocks_fe_resource_group_query_queue_pending | Count | Instantaneous | このリソースグループのキューに現在あるクエリの数。`name` ラベルはリソースグループの名前を示します。このメトリクスは v3.1.4 以降でサポートされています。 |
| starrocks_fe_resource_group_query_queue_timeout | Count | Instantaneous | このリソースグループのキュー内でタイムアウトしたクエリの数。`name` ラベルはリソースグループの名前を示します。このメトリクスは v3.1.4 以降でサポートされています。 |

### SHOW RUNNING QUERIES

v3.1.4 以降、StarRocks は SQL ステートメント `SHOW RUNNING QUERIES` をサポートしており、各クエリのキュー情報を表示するために使用されます。各フィールドの意味は次のとおりです。

- `QueryId`: クエリの ID。
- `WarehouseId`: クエリが実行されるウェアハウスの ID。デフォルトウェアハウスの場合は "-" と表示されます。
- `ResourceGroupId`: クエリがヒットしたリソースグループの ID。ユーザー定義のリソースグループにヒットしない場合は "-" と表示されます。
- `StartTime`: クエリの開始時間。
- `PendingTimeout`: キュー内で PENDING クエリがタイムアウトする時間。
- `QueryTimeout`: クエリがタイムアウトする時間。
- `State`: クエリのキュー状態。"PENDING" はキューに入っていることを示し、"RUNNING" は現在実行中であることを示します。
- `Slots`: クエリによって要求される論理リソース量。Query Queue v1 では通常 `1` です。Query Queue v2 では、その Query について見積もられた slot 数です。
- `Fragments`: クエリの実行プランに含まれるフラグメント数。
- `DOP`: クエリの同時実行数 (`pipeline_dop`)。`0` は同時実行数が適応的であり、実行時に決定されることを意味します。
- `Frontend`: クエリを開始した FE ノード。
- `FeStartTime`: クエリを開始した FE ノードの開始時間。

例:

```Plain
MySQL [(none)]> SHOW RUNNING QUERIES;
+--------------------------------------+-------------+-----------------+---------------------+---------------------+---------------------+---------+-------+-----------+------+---------------------------------+---------------------+
| QueryId                              | WarehouseId | ResourceGroupId | StartTime           | PendingTimeout      | QueryTimeout        | State   | Slots | Fragments | DOP  | Frontend                        | FeStartTime         |
+--------------------------------------+-------------+-----------------+---------------------+---------------------+---------------------+---------+-------+-----------+------+---------------------------------+---------------------+
| a46f68c6-3b49-11ee-8b43-00163e10863a | -           | 12003           | 2023-08-15 16:56:37 | 2023-08-15 17:01:37 | 2023-08-15 17:01:37 | RUNNING | 3     | 2         | 0    | 127.00.00.01_9010_1692069711535 | 2023-08-15 16:37:03 |
| a6935989-3b49-11ee-935a-00163e13bca3 | -           | 12003           | 2023-08-15 16:56:40 | 2023-08-15 17:01:40 | 2023-08-15 17:01:40 | PENDING | 3     | 2         | 0    | 127.00.00.02_9010_1692069658426 | 2023-08-15 16:37:03 |
| a7b5e137-3b49-11ee-8b43-00163e10863a | -           | 12003           | 2023-08-15 16:56:42 | 2023-08-15 17:01:42 | 2023-08-15 17:01:42 | PENDING | 3     | 2         | 0    | 127.00.00.03_9010_1692069711535 | 2023-08-15 16:37:03 |
+--------------------------------------+-------------+-----------------+---------------------+---------------------+---------------------+---------+-------+-----------+------+---------------------------------+---------------------+
```

---
displayed_sidebar: docs
keywords: ['profile', 'query']
---

# Overview

Query Profile は、StarRocks 内での SQL クエリの実行に関する洞察を提供する詳細なレポートです。クエリのパフォーマンスに関する包括的なビューを提供し、各操作に費やされた時間、処理されたデータ量、その他の関連するメトリクスを含みます。この情報は、クエリパフォーマンスの最適化、ボトルネックの特定、問題のトラブルシューティングに非常に役立ちます。

:::tip Why this matters
実際の遅いクエリの 80% は、3 つの警告メトリクスのいずれかを見つけることで解決されます。このチートシートは、数字に溺れる前にあなたをそこに導きます。
:::

## Quick-Start

最近のクエリをプロファイルする:

### 1. 最近のクエリ ID をリストする

クエリプロファイルを分析するには、クエリ ID が必要です。`SHOW PROFILELIST;` を使用します:

```sql
SHOW PROFILELIST;
```

:::tip
`SHOW PROFILELIST` は [Text-based Query Profile Visualized Analysis](./query_profile_text_based_analysis.md) で詳しく説明されています。始めたばかりの方はそのページをご覧ください。
:::

### 2. SQL と並べてプロファイルを開く

`ANALYZE PROFILE FOR <query_id>\G` を実行するか、CelerData Web UI で **Profile** をクリックします。

### 3. 「Execution Overview」バナーをざっと見る

3 つの重要なメトリクスを確認します: **QueryExecutionWallTime**（総実行時間）、**QueryPeakMemoryUsagePerNode**—BE メモリの約 80% を超える値はスピルや OOM を示唆し、比率 **QueryCumulativeCpuTime / WallTime**。この比率が CPU コア数の約半分を下回る場合、クエリは主に I/O やネットワークを待機しています。

どれも該当しない場合、通常クエリは問題ありません—ここで終了します。

### 4. さらに深く掘り下げる

最も時間やメモリを消費するオペレーターを特定し、そのメトリクスを分析して、パフォーマンスボトルネックの根本原因を特定します。

「Operator Metrics」セクションは、パフォーマンス問題の根本原因を特定するのに役立つ多くのガイドラインを提供します。

## Core Concepts

### Query Execution Flow

SQL クエリの包括的な実行フローは、以下のステージを含みます:
1. **Planning**: クエリは解析、分析、最適化を経て、クエリプランの生成に至ります。
2. **Scheduling**: スケジューラとコーディネーターが協力して、クエリプランをすべての参加するバックエンドノードに分配します。
3. **Execution**: クエリプランはパイプライン実行エンジンを使用して実行されます。

![SQL Execution Flow](../../_assets/Profile/execution_flow.png)

### Query Plan Structure

StarRocks のプランは階層的です: **Fragment** は作業の最上位スライスであり、各フラグメントは異なるバックエンドノードで実行される複数の **FragmentInstances** を生成します。インスタンス内では、**Pipeline** がオペレーターを連結し、複数の **PipelineDrivers** が異なる CPU コアで同じパイプラインを並行して実行します。**Operator** はデータを実際に処理するスキャン、ジョイン、集計などの基本的なステップです。

![profile-3](../../_assets/Profile/profile-3.png)

### Pipeline Execution Engine Concepts

Pipeline Engine は StarRocks の実行エンジンの重要なコンポーネントです。クエリプランを並行かつ効率的に実行する役割を担っています。Pipeline Engine は複雑なクエリプランと大量のデータを処理するように設計されており、高いパフォーマンスとスケーラビリティを保証します。

パイプラインエンジンでは、**Operator** が 1 つのアルゴリズムを実装し、**Pipeline** はオペレーターの順序付けられたチェーンであり、複数の **PipelineDrivers** がそのチェーンを並行して忙しく保ちます。軽量なユーザースペーススケジューラが時間をスライスし、ドライバーがブロックされずに進行します。

![pipeline_opeartors](../../_assets/Profile/pipeline_operators.png)

### Metric Merging Strategy

デフォルトでは、StarRocks は FragmentInstance と PipelineDriver レイヤーをマージしてプロファイルのボリュームを削減し、簡略化された 3 層構造を生成します:
- Fragment
- Pipeline
- Operator

このマージ動作はセッション変数 `pipeline_profile_level` で制御できます:
- `1` (デフォルト): マージされた 3 層構造
- `2`: 元の 5 層構造
- その他の値: `1` として扱われます

プロファイル圧縮がオンの場合、エンジンは **時間メトリクスを平均化** し（例: `OperatorTotalTime`、`__MAX_OF_` / `__MIN_OF_` は極値を記録）、`PullChunkNum` などの **カウンターを合計** し、`DegreeOfParallelism` などの **定数属性** を変更せずに残します。

MIN と MAX の値の間に大きな違いがある場合、特に集計やジョイン操作においてデータの偏りを示すことがよくあります。
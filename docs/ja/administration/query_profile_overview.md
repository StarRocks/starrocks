---
displayed_sidebar: docs
keywords: ['profile', 'query']
---

# Query Profile 概要

このトピックでは、Query Profile の表示と分析方法を紹介します。Query Profile は、クエリに関与するすべての作業ノードの実行情報を記録します。Query Profile を使用すると、クエリパフォーマンスに影響を与えるボトルネックを迅速に特定できます。

## Query Profile の有効化

Query Profile を有効にするには、変数 `enable_profile` を `true` に設定します。

```SQL
SET enable_profile = true;
```

### 遅いクエリに対する Query Profile の有効化

本番環境で Query Profile をグローバルかつ長期間にわたって有効にすることは推奨されません。これは、Query Profile のデータ収集と処理がシステムに追加の負担をかける可能性があるためです。しかし、遅いクエリをキャプチャして分析する必要がある場合は、遅いクエリに対してのみ Query Profile を有効にすることができます。これは、変数 `big_query_profile_threshold` を `0s` より大きい時間に設定することで実現できます。例えば、この変数を `30s` に設定すると、実行時間が 30 秒を超えるクエリのみが Query Profile をトリガーします。これにより、システムパフォーマンスを確保しつつ、遅いクエリを効果的に監視できます。

```SQL
-- 30 秒
SET global big_query_profile_threshold = '30s';

-- 500 ミリ秒
SET global big_query_profile_threshold = '500ms';

-- 60 分
SET global big_query_profile_threshold = '60m';
```

### 実行時 Query Profile の有効化

一部のクエリは、実行に数秒から数時間かかることがあります。多くの場合、クエリがまだ進行中なのか、システムがクエリ完了前にクラッシュしたのかを判断するのは難しいです。この問題に対処するために、StarRocks は v3.1 以降で実行時 Query Profile 機能を導入しました。この機能により、クエリ実行中に固定時間間隔で Query Profile データを収集し、報告することができます。これにより、クエリの実行進捗と潜在的なボトルネックをリアルタイムで把握でき、クエリが終了するのを待たずに監視と最適化が可能になります。

Query Profile が有効になっている場合、この機能は自動的にアクティブ化され、デフォルトの報告間隔は 10 秒です。変数 `runtime_profile_report_interval` を変更することで間隔を調整できます。

```SQL
SET runtime_profile_report_interval = 30;
```

実行時 Query Profile は、通常の Query Profile と同じ形式と内容を持っています。実行時 Query Profile を分析することで、クラスター内で実行中のクエリのパフォーマンス指標を理解することができます。

### Query Profile の動作を設定する

| 設定タイプ | 設定項目 | 有効な値 | デフォルト値 | 説明 |
| -- | -- | -- | -- | -- |
| セッション変数 | enable_profile | true/false | false | Query Profile を有効にするかどうか。`true` はこの機能を有効にすることを意味します。 |
| セッション変数 | pipeline_profile_level | 1/2 | 1 | Query Profile のレベルを設定します。`1` は Query Profile のメトリクスをマージすることを示し、`2` は Query Profile の元の構造を保持することを示します。この項目を `2` に設定すると、すべての可視化分析ツールが適用されなくなるため、一般的にはこの値を変更することは推奨されません。 |
| セッション変数 | runtime_profile_report_interval | 正の整数 | 10 | 実行時 Query Profile の報告間隔。単位: 秒。 |
| セッション変数 | big_query_profile_threshold | 文字列 | `0s` | 大規模クエリの実行時間がこの値を超える場合、そのクエリに対して Query Profile が自動的に有効になります。この項目を `0s` に設定すると、この機能は無効になります。その値は、単位を伴う整数で表すことができ、単位は `ms`、`s`、`m` です。 |
| FE 動的設定項目 | enable_statistics_collect_profile | true/false | false | 統計収集関連のクエリに対して Query Profile を有効にするかどうか。`true` はこの機能を有効にすることを意味します。 |

### Web UI を通じて Query Profile を取得する

以下の手順に従って Query Profile を取得します。

1. ブラウザで `http://<fe_ip>:<fe_http_port>` にアクセスします。
2. 表示されたページで、上部ナビゲーションの **queries** をクリックします。
3. **Finished Queries** リストで、分析したいクエリを選択し、**Profile** 列のリンクをクリックします。

![img](../_assets/profile-1.png)

選択した Query Profile の詳細ページにリダイレクトされます。

![img](../_assets/profile-2.png)

### get_query_profile を通じて Query Profile を取得する

以下の例は、関数 get_query_profile を通じて Query Profile を取得する方法を示しています。

```Plain
-- プロファイリング機能を有効にします。
set enable_profile = true;
-- 単純なクエリを実行します。
select 1;
-- クエリの query_id を取得します。
select last_query_id();
+--------------------------------------+
| last_query_id()                      |
+--------------------------------------+
| bd3335ce-8dde-11ee-92e4-3269eb8da7d1 |
+--------------------------------------+
-- クエリプロファイルを取得します。
select get_query_profile('502f3c04-8f5c-11ee-a41f-b22a2c00f66b')\G
```

## Query Profile の分析

Query Profile によって生成された生の内容には、多くのメトリクスが含まれている場合があります。これらのメトリクスの詳細な説明については、[Query Profile Structure and Detailed Metrics](./query_profile_details.md) を参照してください。

しかし、ほとんどのユーザーは、この生のテキストを直接分析するのは容易ではないと感じるかもしれません。この問題に対処するために、StarRocks は [Text-based Query Profile Visualized Analysis](./query_profile_text_based_analysis.md) メソッドを提供しています。この機能を使用して、複雑な Query Profile をより直感的に理解することができます。
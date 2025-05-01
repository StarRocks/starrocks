---
displayed_sidebar: docs
keywords: ['profile', 'query']
---

# Query Profile 概要

このトピックでは、Query Profile の表示と分析方法を紹介します。Query Profile は、クエリに関与するすべての作業ノードの実行情報を記録します。Query Profile を通じて、クエリパフォーマンスに影響を与えるボトルネックを迅速に特定できます。

v3.3.0 以降、StarRocks は INSERT INTO FILES() および Broker Load を使用したデータロードに対して Query Profile を提供することをサポートしています。関連するメトリクスの詳細については、 [OlapTableSink Operator](./query_profile_details.md#olaptablesink-operator) を参照してください。

## Query Profile を有効にする

`enable_profile` 変数を `true` に設定することで、Query Profile を有効にできます。

```SQL
SET enable_profile = true;
```

### 遅いクエリに対して Query Profile を有効にする

本番環境で Query Profile をグローバルかつ長期間にわたって有効にすることは推奨されません。これは、Query Profile のデータ収集と処理がシステムに追加の負担をかける可能性があるためです。しかし、遅いクエリをキャプチャして分析する必要がある場合は、遅いクエリに対してのみ Query Profile を有効にすることができます。これは、`big_query_profile_threshold` 変数を `0s` より大きい時間に設定することで実現できます。たとえば、この変数を `30s` に設定すると、実行時間が 30 秒を超えるクエリのみが Query Profile をトリガーします。これにより、システムパフォーマンスを確保しつつ、遅いクエリを効果的に監視できます。

```SQL
-- 30 秒
SET global big_query_profile_threshold = '30s';

-- 500 ミリ秒
SET global big_query_profile_threshold = '500ms';

-- 60 分
SET global big_query_profile_threshold = '60m';
```

### 実行時 Query Profile を有効にする

一部のクエリは、秒から数時間にわたって実行されることがあります。多くの場合、クエリがまだ進行中なのか、システムがクラッシュしたのかを判断するのは難しいです。この問題に対処するために、StarRocks は v3.1 以降で Runtime Query Profile 機能を導入しました。この機能により、クエリの実行中に固定時間間隔で Query Profile データを収集して報告できます。これにより、クエリの実行進捗と潜在的なボトルネックをリアルタイムで把握でき、クエリが終了するのを待たずに監視と最適化が可能になります。

Query Profile が有効になっている場合、この機能は自動的にアクティブ化され、デフォルトの報告間隔は 10 秒です。間隔を調整するには、`runtime_profile_report_interval` 変数を変更します。

```SQL
SET runtime_profile_report_interval = 30;
```

Runtime Query Profile は通常の Query Profile と同じ形式と内容を持っています。クラスター内で実行中のクエリのパフォーマンスメトリクスを理解するために、Runtime Query Profile を通常の Query Profile と同様に分析できます。

### Query Profile の動作を設定する

設定項目はセッション変数または FE 動的設定項目です。

#### セッション変数

- **設定項目**:  enable_profile 
- **有効な値**:  true/false 
- **デフォルト値**:  false 
- **説明**:  Query Profile を有効にするかどうか。`true` はこの機能を有効にすることを意味します。

#### セッション変数

- **設定項目**:  pipeline_profile_level 
- **有効な値**:  1/2 
- **デフォルト値**:  1 
- **説明**:  Query Profile のレベルを設定します。`1` は Query Profile のメトリクスをマージすることを示し、`2` は Query Profile の元の構造を保持することを示します。この項目を `2` に設定すると、すべての可視化分析ツールが適用されなくなるため、一般的にはこの値を変更することは推奨されません。

#### セッション変数

- **設定項目**:  runtime_profile_report_interval 
- **有効な値**:  正の整数 
- **デフォルト値**:  10 
- **説明**:  Runtime Query Profile の報告間隔。単位: 秒。

#### セッション変数

- **設定項目**:  big_query_profile_threshold 
- **有効な値**:  文字列 
- **デフォルト値**:  `0s` 
- **説明**:  大規模クエリの実行時間がこの値を超える場合、そのクエリに対して Query Profile が自動的に有効になります。この項目を `0s` に設定すると、この機能は無効になります。その値は単位を伴う整数で表すことができ、単位は `ms`、`s`、`m` です。

#### FE 動的設定項目

- **設定項目**:  enable_statistics_collect_profile 
- **有効な値**:  true/false 
- **デフォルト値**:  false 
- **説明**:  統計収集関連のクエリに対して Query Profile を有効にするかどうか。`true` はこの機能を有効にすることを意味します。

### Web UI で Query Profile を取得する

Query Profile を取得するには、次の手順に従います。

1. ブラウザで `http://<fe_ip>:<fe_http_port>` にアクセスします。
2. 表示されたページで、上部のナビゲーションで **queries** をクリックします。
3. **Finished Queries** リストで、分析したいクエリを選択し、**Profile** 列のリンクをクリックします。

![img](../_assets/profile-1.png)

選択した Query Profile の詳細ページにリダイレクトされます。

![img](../_assets/profile-2.png)

### get_query_profile で Query Profile を取得する

次の例は、get_query_profile 関数を使用して Query Profile を取得する方法を示しています。

```sql
-- プロファイリング機能を有効にします。
set enable_profile = true;
-- 簡単なクエリを実行します。
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

## Query Profile を分析する

Query Profile によって生成される生の内容には、多数のメトリクスが含まれている場合があります。これらのメトリクスの詳細な説明については、 [Query Profile Structure and Detailed Metrics](./query_profile_details.md) を参照してください。

しかし、ほとんどのユーザーは、この生のテキストを直接分析するのは簡単ではないと感じるかもしれません。この問題に対処するために、StarRocks は [Text-based Query Profile Visualized Analysis](./query_profile_text_based_analysis.md) メソッドを提供しています。この機能を使用して、複雑な Query Profile をより直感的に理解できます。
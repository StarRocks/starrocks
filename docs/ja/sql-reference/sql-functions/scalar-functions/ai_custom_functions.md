---
displayed_sidebar: docs
description: "登録済み AI プロバイダーを呼び出し、チャット補完とテキスト埋め込みを実行します。"
sidebar_position: 23
---

# AI プロバイダー関数

`ai_custom_query` と `ai_custom_embedding` は、既存のクラスター全体の [AI プロバイダーレジストリ](../../sql-statements/cluster-management/ai_provider/CREATE_AI_PROVIDER.md)を使用します。行ごとに変わるリモートモデル名ではなく、登録済みプロバイダーを名前で選択します。使用前に、すべての FE と BE をアップグレードしてください。

:::warning
呼び出しは入力データをクラスター外に送信し、プロバイダー料金が発生する場合があります。この機能は AI 関数やプロバイダーオブジェクトの呼び出し権限を追加しません。プロバイダー管理には引き続き SYSTEM OPERATE が必要で、既存のテーブル、列、ビューの権限チェックも維持されますが、プロバイダーごとの呼び出し分離は提供しません。信頼できるクエリユーザーにのみ、データと費用の要件に合う承認済みプロバイダーの使用を許可してください。
:::

## 構文

```sql
ai_custom_query(provider_name, text)
ai_custom_query(provider_name, text, options)
ai_custom_embedding(provider_name, text)
ai_custom_embedding(provider_name, text, options)
```

- `provider_name`: NULL でも空白でもない定数 VARCHAR 式。大文字と小文字を区別する正確なプロバイダー名を指定します。
- `text`: VARCHAR 式。URL もテキストとして扱い、StarRocks は内容を取得しません。FILE、画像、マルチモーダル入力はサポートしません。
- `options`: 省略可能な定数 MAP。型付き NULL MAP は空の MAP として扱います。チャットは [ai_complete のオプションルール](ai_complete.md#options-map-のルール)、埋め込みは [ai_embed](ai_embed.md#構文)に従います。選択したプロバイダー、エンドポイント、認証情報、リモートモデルはオプションで変更できません。

`ai_custom_query` にはタイプ `chat` のプロバイダーが必要で、NULL 許容の VARCHAR を返します。`ai_custom_embedding` にはタイプ `embedding` が必要で、NULL 許容の `ARRAY<FLOAT>` を返します。両関数は現在 `openai` プロトコルを必要とします。プロバイダーが存在しない場合、タイプが一致しない場合 (`rerank` を含む)、プロトコルが未対応の場合、または実行設定が無効な場合は、リクエスト送信前に失敗します。SYSTEM チャットや埋め込みのデフォルト設定は不要で、SYSTEM 認証情報へのフォールバックもありません。

テキストが NULL の場合は、リクエストせず NULL を返します。行レベルの失敗は [ai_function_on_error](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_on_error) に従います。設定エラー、キャンセル、期限切れは NULL に変換されません。[SQL 配置制限とプリペアドステートメントのルール](ai_complete.md#制限とセキュリティ)も適用されます。

## プロバイダー設定

既存の [CREATE](../../sql-statements/cluster-management/ai_provider/CREATE_AI_PROVIDER.md)、[ALTER](../../sql-statements/cluster-management/ai_provider/ALTER_AI_PROVIDER.md)、[SHOW](../../sql-statements/cluster-management/ai_provider/SHOW_AI_PROVIDERS.md)、[DESC](../../sql-statements/cluster-management/ai_provider/DESC_AI_PROVIDER.md) ステートメントでプロバイダーを登録・管理します。これらの関数での動作は次のとおりです。

| プロパティ | 呼び出し時の動作 |
|------------|------------------|
| `endpoint` | OpenAI 互換のチャット補完または埋め込みリクエストの完全な URL。StarRocks はパスを追加しません。認証情報を送信する場合は HTTPS が必要です。API キーがないプロバイダーは HTTP(S) を使用できます。URL 検証、アドレス制限、DNS ピンニングは引き続き適用されます。 |
| `model` | リクエストに送信するリモートモデル名。 |
| `protocol` | `chat` と `embedding` のデフォルトである `openai` が必要です。レジストリは `anthropic` と `cohere` も受け付けますが、これらの関数では実行できません。 |
| `api_key` | プロバイダーメタデータに保存する、省略可能な Bearer 認証情報。省略時は Authorization ヘッダーを送信しません。名前付き呼び出しは BE ローカルの SYSTEM 認証情報を使用しません。 |
| `dimensions` | 埋め込みリクエストのデフォルトオプション。SQL options に同じキーを明示すると、その値を優先します。プロバイダーとモデルが指定値をサポートする必要があります。 |
| `timeout_ms` | HTTP attempt ごとのタイムアウトであり、論理リクエスト全体の時間上限を置き換えません。各 attempt には残りの論理時間枠と現在のクエリ deadline も適用され、リトライで論理時間枠はリセットされません。 |

プロバイダーの API キーは FE のメタデータジャーナルとイメージに保存され、既存の内部 RPC で実行 BE に送信されます。メタデータファイルとクラスターネットワークへのアクセスを保護してください。SHOW と DESC はキーをマスクしますが、メタデータや内部 RPC の暗号化を意味するものではありません。関数の引数や options に認証情報を含めないでください。

## クエリスナップショットと SYSTEM 互換性

ステートメントは計画時に参照先プロバイダーをまとめて取得し、すべての AI 実行ノードがその値を使用します。後の ALTER や DROP は実行中のプランを変更しません。新しいクエリや次のプリペアド EXECUTE は現在のメタデータを取得します。同じ名前で削除・再作成すると別のプロバイダー ID になります。プロバイダーを削除しても実行中のクエリはキャンセルされません。

Query dump はプロバイダーメタデータを記録しません。名前付きプロバイダー呼び出しのオフライン再生はサポートされず、取得していないスナップショットを現在の同名プロバイダーで置き換えることもできません。

Spark/Flink が使用する `/api/{db}/{table}/_query_plan` エクスポートは、既存の単一テーブルのフィルター、プルーニング、スキャン（filter-prune-scan）のみをサポートし、AI 関数はサポートしません。これは実行プラン内のプロバイダー認証情報がクライアントにエクスポートされることを防ぐための機能上の制限であり、AI 関数やプロバイダーの RBAC 権限を追加するものではありません。

既存のタイプごとのデフォルトプロバイダー機構は変更しません。デフォルトを設定しても SYSTEM `ai_complete`、[テキスト補助関数](ai_functions.md)、[ai_embed](ai_embed.md) のルーティングは変わりません。既存のオーバーロード、FE 設定、BE ローカル認証情報は維持され、SYSTEM の明示的な `model` 引数は引き続きリモートモデル名を表します。

## 例

管理者がタイプ `chat` の `support_chat` とタイプ `embedding` の `search_embedding` を、ともに `openai` プロトコルで登録した後、次の例は HTTP リクエストを送信せずクエリを計画します。

```sql
EXPLAIN SELECT ai_custom_query('support_chat', 'A local test prompt.');
EXPLAIN SELECT ai_custom_query('support_chat', 'A local test prompt.', map{'temperature': 0.0});
EXPLAIN SELECT ai_custom_embedding('search_embedding', 'A local test sentence.');
EXPLAIN SELECT ai_custom_embedding('search_embedding', 'A local test sentence.', map{'dimensions': 128});
```

EXPLAIN は選択した設定を検証しますが、外部サービスは呼び出しません。代わりに SELECT を実行すると、NULL でない入力行ごとにリモートリクエストが発生し、リトライによってさらに増える場合があります。

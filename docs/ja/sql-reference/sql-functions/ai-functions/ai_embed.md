---
displayed_sidebar: docs
description: "`openai` プロトコルの `embedding` Provider でテキストを埋め込み、NULL 許容の `ARRAY<FLOAT>` を返します。"
sidebar_position: 22
---

# ai_embed

`openai` プロトコルの `embedding` Provider でテキストを埋め込み、NULL 許容の `ARRAY<FLOAT>` を返します。

デフォルト Provider を含め、`USAGE ON AI FUNCTION <function_name>` と `USAGE ON AI PROVIDER <provider_name>` の両方が必要です。[AI 関数の権限](ai_functions.mdx#ai-function-privileges)を参照してください。

## 構文

```sql
ai_embed(input)
ai_embed(input, options)
ai_embed(file)
ai_embed(file, options)
ai_embed(model, input)
ai_embed(model, input, options)
ai_embed(model, file)
ai_embed(model, file, options)
ai_embed(provider, model, input)
ai_embed(provider, model, input, options)
ai_embed(provider, model, file)
ai_embed(provider, model, file, options)
```

## パラメータ

`provider` を省略すると、補完とテキスト補助関数は `chat`、埋め込み関数は `embedding` のクラスターデフォルト Provider を使います。`model` を省略すると Provider の必須デフォルトモデルを使います。明示モデルが優先され、行ごとに変更できます。`provider` は空白でない定数文字列である必要があり、定数の明示モデルも空白にはできません。明示 Provider の呼び出しにはクラスターデフォルトの設定は不要です。

テキスト、明示的な model、FILE 入力が NULL の場合、定数でも実行時の列値でもリクエストせず NULL を返します。NULL の options MAP はオプションなしを意味し、NULL の翻訳ソース言語は自動検出を要求します。Provider 名とヘルパー関数の配列には、それぞれの引数検証が適用されます。options 内部のネストした NULL は JSON 互換の値として扱われます。型なし NULL が複数のオーバーロードで異なる引数型に一致する場合は、`CAST(NULL AS VARCHAR)` や `CAST(NULL AS MAP<VARCHAR, JSON>)` のように型を明示してください。

`input` は VARCHAR 式です。URL 文字列は取得せずテキストとして埋め込みます。FILE オーバーロードは型と内容の検証を保持しますが、公開している OpenAI embedding プロトコルはテキストのみを受け付けます。FILE の埋め込みは生成できず、非対応入力は `ai_function_on_error` に従います。参照 FILE は `NotSupported` になります。

`dimensions` は Provider の値を継承し、SQL options で上書きできます。サービスとモデルが指定次元に対応する必要があります。[ai_complete の再帰 MAP ルール](ai_complete.md#options-map-のルール)が適用されますが、予約キー `messages`、`stream` は `input`、`encoding_format` に置き換わります。接続、モデル、プロトコル関連の予約キーは共通です。

レスポンスは index 0 の埋め込みを一つだけ含み、空でない有限 FLOAT 配列である必要があります。不正なレスポンスは `ai_function_on_error` に従います。[ランタイム制御](ai_complete.md#ランタイム制限とリトライ)と [SQL 制限](ai_complete.md#制限とセキュリティ)が適用され、埋め込みは `ai_function_rate_limit_qps_embedding` を使用します。

## 設定

`protocol = "openai"` の Provider を作成します。API key の有無にかかわらず HTTP と HTTPS をサポートします。空でない key は `Authorization: Bearer ...` ヘッダーになります。URL 形式、制御文字、DNS、アドレスの検証は引き続き適用されます。信頼できるエンドポイントと送信が許可されたデータを使用してください。

`SET <provider_name> AS DEFAULT AI PROVIDER` でデフォルトを設定します。該当するデフォルトが未設定の場合、暗黙 Provider の呼び出しは `EXPLAIN` を含め FE でエラーになります。各実行プランは選択した Provider の設定を固定します。ALTER とデフォルト変更は新たに計画するクエリに反映され、プリペアド文の各 `EXECUTE` も現在のメタデータで再計画します。

## 例

```sql
CREATE AI PROVIDER text_embedding TYPE embedding
PROPERTIES ("endpoint" = "http://localhost:8000/v1/embeddings",
            "model" = "embedding-model", "protocol" = "openai", "dimensions" = "256");
SET text_embedding AS DEFAULT AI PROVIDER;

EXPLAIN SELECT ai_embed('A local test sentence.');
EXPLAIN SELECT ai_embed('another-model', 'A local test sentence.');
EXPLAIN SELECT ai_embed('text_embedding', 'another-model', 'A local test sentence.',
                       map{'dimensions': 128});
EXPLAIN SELECT ai_embed('text_embedding', 'another-model', CAST(NULL AS VARCHAR));
EXPLAIN SELECT ai_embed('another-model', 'text', CAST(NULL AS MAP<VARCHAR, JSON>));
```

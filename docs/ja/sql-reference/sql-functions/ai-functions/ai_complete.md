---
displayed_sidebar: docs
description: "AI Provider を通じて OpenAI チャットモデルを呼び出します。"
---

# ai_complete

AI Provider を通じて OpenAI チャット補完エンドポイントを呼び出し、生成されたテキストを返します。プロンプト、モデル、options、FILE の内容は選択したエンドポイントに送信されます。

デフォルト Provider を含め、`USAGE ON AI FUNCTION <function_name>` と `USAGE ON AI PROVIDER <provider_name>` の両方が必要です。[AI 関数の権限](ai_functions.mdx#ai-function-privileges)を参照してください。

## 構文

```sql
ai_complete(prompt)
ai_complete(prompt, options)
ai_complete(prompt, file)
ai_complete(prompt, file, options)
ai_complete(model, prompt)
ai_complete(model, prompt, options)
ai_complete(model, prompt, file)
ai_complete(model, prompt, file, options)
ai_complete(provider, model, prompt)
ai_complete(provider, model, prompt, options)
ai_complete(provider, model, prompt, file)
ai_complete(provider, model, prompt, file, options)
```

## パラメータ

`provider` を省略すると、補完とテキスト補助関数は `chat`、埋め込み関数は `embedding` のクラスターデフォルト Provider を使います。`model` を省略すると Provider の必須デフォルトモデルを使います。明示モデルが優先され、行ごとに変更できます。`provider` は空白でない定数文字列である必要があり、定数の明示モデルも空白にはできません。明示 Provider の呼び出しにはクラスターデフォルトの設定は不要です。

テキスト、明示的な model、FILE 入力が NULL の場合、定数でも実行時の列値でもリクエストせず NULL を返します。NULL の options MAP はオプションなしを意味し、NULL の翻訳ソース言語は自動検出を要求します。Provider 名とヘルパー関数の配列には、それぞれの引数検証が適用されます。options 内部のネストした NULL は JSON 互換の値として扱われます。型なし NULL が複数のオーバーロードで異なる引数型に一致する場合は、`CAST(NULL AS VARCHAR)` や `CAST(NULL AS MAP<VARCHAR, JSON>)` のように型を明示してください。

### Options MAP のルール

`options` は省略可能な定数 MAP です。追加オプションがなければ省略するか `map{}` を使います。各階層の MAP キーは一意かつ非 NULL、空でない VARCHAR である必要があります。値は NULL、BOOLEAN、有限数値、文字列、JSON、ARRAY、MAP、STRUCT をサポートします。大文字と小文字を区別するトップレベルキー `model`、`endpoint`、`key`、`api_key`、`protocol`、`provider`、`messages`、`stream` は予約されています。接続設定と認証情報は Provider から取得します。

### FILE 入力

FILE オーバーロードは OpenAI チャット用の inline JPEG、PNG、WebP 画像を受け付けます。モデルは画像入力に対応する必要があります。動画、音声、文書はサポートしません。URL や VARCHAR から FILE への暗黙変換や URI の取得は行いません。内容は `inline` のみで渡し、`uri`、`offset`、`size` は NULL にします。MIME はバイトから検出し、指定した `content_type` は検出結果と一致する必要があります。

`ai_function_max_input_file_bytes` はコピーと Base64 エンコード前のバイト数を制限し、デフォルトは 10485760 バイト（10 MiB）です。不正または上限超過の inline データは `ai_function_on_error` に従います。参照 FILE は `on_error=ignore` でも `NotSupported` になります。FILE 列の実行時 NULL はリクエストせず NULL を返します。

## 戻り値

`choices[0].message.content` を含む NULL 許容 VARCHAR を返します。結果は非決定的です。行エラーは `ai_function_on_error` に従い、デフォルトの `ignore` は NULL、`fail` はクエリ中止になります。解析・設定エラー、キャンセル、期限切れは無視されません。

## 設定

`protocol = "openai"` の Provider を作成します。API key の有無にかかわらず HTTP と HTTPS をサポートします。空でない key は `Authorization: Bearer ...` ヘッダーになります。URL 形式、制御文字、DNS、アドレスの検証は引き続き適用されます。信頼できるエンドポイントと送信が許可されたデータを使用してください。

`SET <provider_name> AS DEFAULT AI PROVIDER` でデフォルトを設定します。該当するデフォルトが未設定の場合、暗黙 Provider の呼び出しは `EXPLAIN` を含め FE でエラーになります。各実行プランは選択した Provider の設定を固定します。ALTER とデフォルト変更は新たに計画するクエリに反映され、プリペアド文の各 `EXECUTE` も現在のメタデータで再計画します。

### ランタイム制限とリトライ

- [`ai_function_rate_limit_qps_chat`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_rate_limit_qps_chat)
  と
  [`ai_function_max_inflight`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_inflight)
  は BE ごとに独立して適用されます。QPS 制限は endpoint、credential、capability ごとの bucket で管理され、in-flight 制限はプロセス全体に適用されます。WorkGroup とクエリを考慮した公平な admission により、待機中のリクエスト間で該当する制限が共有されます。最初の HTTP attempt とリトライ attempt は、いずれも両方の admission permit を取得する必要があります。
- StarRocks はモデルプロバイダーでの exactly-once 実行を保証できません。タイムアウトなどで失敗した attempt がすでにプロバイダーに到達している可能性があるため、リトライによって処理が重複し、追加料金が発生することがあります。プロバイダーの動作と課金ポリシーに合わせて
  [`ai_function_max_retries`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_retries)
  と
  [`ai_function_max_retries_on_throttle`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_retries_on_throttle)
  を設定してください。
- リクエストとレスポンスの payload はクエリのメモリトラッカーに計上され、outstanding リクエストがある間は実行 Pipeline がバックプレッシャーを適用します。
  [`ai_function_max_response_bytes`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_response_bytes)
  は各 HTTP レスポンス body に対するハードリミットです。
- 独立した task timeout は task の作成後に固定され、retry ごとに再開されません。クエリのキャンセルと
  deadline の更新は非同期実行全体を通して反映されます。タイムアウト、ワーカー、スケジューリング粒度の制御については、その他の
  [BE AI 関数パラメータ](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_request_timeout_ms)
  を参照してください。

## 制限とセキュリティ

- `ai_complete` は、`GROUP BY`、`SELECT DISTINCT`、集約関数の引数、ウィンドウ関数式では使用できません。
- `IF`、`IFNULL`、`NULLIF`、`COALESCE`、`CASE` の条件式内、またはテーブル関数の引数としては使用できません。
- マテリアライズドビューの定義や生成列の式では使用できません。
- ラムダ式の body または SQL UDF の body では使用できません。
- `ai_complete` を含むステートメントでは SQL Plan Baseline を作成またはバインドできません。また、この関数を含む他のクエリにも
  SPM rewrite は適用されません。
- `PREPARE` はサポートされますが、`ai_complete` を含むステートメントは `EXECUTE` ごとに完全に再計画され、実行プランは再利用されません。
- 相関サブクエリのクエリブロックでは、AI 式自体がローカル列のみを参照する場合でも、`SELECT` リスト、`WHERE`、`HAVING`、
  `ORDER BY`、`JOIN ON` 内の AI 式は拒否されます。結合条件内の AI 式は `INNER JOIN` と `CROSS JOIN` でのみサポートされます。
- NULL ではない入力行ごとにリモートリクエストが発生する可能性があります。多数の行に使用する前に、ネットワーク遅延、プロバイダークォータ、クエリ期限、費用、データ送出ポリシーを考慮してください。

## 例

```sql
CREATE AI PROVIDER support_chat TYPE chat
PROPERTIES ("endpoint" = "http://localhost:8000/v1/chat/completions",
            "model" = "chat-model", "protocol" = "openai");
SET support_chat AS DEFAULT AI PROVIDER;

EXPLAIN SELECT ai_complete('Summarize this text.');
EXPLAIN SELECT ai_complete('another-model', 'Summarize this text.');
EXPLAIN SELECT ai_complete('support_chat', 'another-model', 'Summarize this text.', map{});
EXPLAIN SELECT ai_complete('support_chat', 'vision-model', 'Describe this image.', image_file)
FROM media_inputs;
EXPLAIN SELECT ai_complete('support_chat', 'another-model', CAST(NULL AS VARCHAR));
EXPLAIN SELECT ai_complete('another-model', 'text', CAST(NULL AS MAP<VARCHAR, JSON>));
```

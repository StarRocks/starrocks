---
displayed_sidebar: docs
description: "設定済みの SYSTEM OpenAI 互換チャットエンドポイントを呼び出し、生成されたテキストを返します。"
---

# ai_complete

プロンプトと省略可能な inline FILE を使って管理者が設定した SYSTEM チャットモデルを呼び出し、モデルが生成したテキストを返します。StarRocks は BE から非ストリーミングの OpenAI 互換チャット補完リクエストを送信します。

:::warning
この関数は、モデル名、プロンプト、指定した FILE の内容、オプションを設定済みのエンドポイントに送信します。信頼できる HTTPS endpoint のみを使用してください。プロバイダーが受信を許可されていない機密情報や秘密情報は送信しないでください。リクエストが StarRocks クラスターの外部に送信され、プロバイダー料金が発生したり、プロバイダーのデータ処理ポリシーに従って保持されたりする可能性があります。
:::

## 構文

```sql
ai_complete(<prompt>)
ai_complete(<prompt>, <options>)
ai_complete(<model>, <prompt>)
ai_complete(<model>, <prompt>, <options>)
ai_complete(<prompt>, <file>)
ai_complete(<prompt>, <file>, <options>)
ai_complete(<model>, <prompt>, <file>)
ai_complete(<model>, <prompt>, <file>, <options>)
```

## パラメータ

- `prompt`：ユーザープロンプトを含む VARCHAR 式。空文字列も有効です。
- `file`：対応する inline 画像または動画を含む、省略可能な FILE 型の式。[FILE 入力](#file-入力)を参照してください。
- `model`：この呼び出しで使用するモデルを選択する VARCHAR 式。省略した場合、StarRocks は
  [`ai_default_chat_model`](../../../administration/configuration/FE_parameters/user_query_loading.md#ai_default_chat_model)
  を使用します。明示的なモデルは行ごとに変更できますが、定数モデルを空文字列または空白文字だけにすることはできません。
- `options`：プロバイダーリクエストにフィールドを追加する、省略可能な定数 MAP。型付きの NULL MAP は空の MAP として扱われます。

### Options MAP のルール

- MAP は定数である必要があります。トップレベルとネストされた各 MAP 内のキーは、一意で、NULL でも空でもない VARCHAR 値である必要があります。
- オプション値は JSON 互換である必要があります。使用できる値は、NULL、BOOLEAN、有限の数値、文字列、JSON、ARRAY、MAP、STRUCT です。ネストされた MAP 値のキーも VARCHAR である必要があります。
- 大文字と小文字が区別されるトップレベルのキー `model`、`messages`、`stream` は予約済みであり、指定できません。StarRocks がこれらのフィールドを構築し、常に非ストリーミングリクエストを送信します。
- 2 引数形式の型なし NULL は `ai_complete(<model>, NULL)` として解決されます。NULL を `options` として渡すには、たとえば
  `CAST(NULL AS MAP<VARCHAR, JSON>)` のように MAP 型へキャストします。

### FILE 入力

`file` は URL 文字列ではなく FILE 型の式です。FILE オーバーロードは inline JPEG、PNG、WebP 画像を受け付け、
prompt と画像を content part として送信します。`qwen_compatible` チャットプロトコルを明示的に設定した場合は
inline MP4 動画も利用できます。モデル側で入力のモダリティに対応する必要があります。`openai_compatible` は動画を拒否します。
FILE 型の変更、FILE コンストラクターの追加、URI 参照の取得は行いません。
FILE と URL や VARCHAR 入力の間の暗黙の変換や、媒体からテキストへのフォールバックはありません。

内容は FILE の `inline` フィールドに格納し、`uri`、`offset`、`size` は NULL にします。
`content_type` が指定されている場合は検出した形式と一致する必要があります。検出は有限長のコンテナーヘッダーのみを
確認し、完全なデコードの有効性はプロバイダーが判断します。`checksum` はメタデータとして扱い、検証しません。
BE パラメーター [ai_function_max_input_file_bytes](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_input_file_bytes)
は AI が保持するペイロードのコピーと Base64 エンコードの前に FILE ごとの生バイト数を制限します。デフォルトは 10485760 バイト（10 MiB）です。
不正または過大な inline 内容と、非対応の媒体・プロトコルの組み合わせは行レベルの失敗として `ai_function_on_error` に従います。
認可された FILE reader が利用可能になるまで、参照型 FILE は `on_error=ignore` でも `NotSupported` エラーになります。
NULL FILE はリクエストせず NULL を返します。
互換性のため、`ai_complete(model, prompt, NULL)` は従来のテキストオーバーロードを選択し、型なし NULL を NULL FILE ではなく options（空の MAP と同じ扱い）として解釈します。FILE 型の列は FILE オーバーロードを選択し、その NULL 値はリクエストせず NULL を返します。
FILE オーバーロードを使用する前にすべての FE と BE をアップグレードしてください。
ファイル内容は設定済みモデルエンドポイントに送信されるため、prompt と同じデータ送信の注意事項が適用されます。

## 戻り値

OpenAI 互換レスポンスが成功した場合、`choices[0].message.content` を含む NULL 許容の VARCHAR を返します。

- `prompt` が NULL の場合、プロバイダーリクエストを送信せずに NULL を返します。
- `model` を明示するオーバーロードでは、`model` が NULL の場合もリクエストを送信せずに NULL を返します。
- BE 設定
  [`ai_function_on_error`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_on_error)
  は行単位のエラーを制御します。デフォルトの `ignore` では、失敗した行に NULL を返してクエリを続行します。`fail` ではクエリを中止します。
- `ignore` は解析エラーや設定エラーを抑止せず、クエリのキャンセル、期限切れ、BE のシャットダウンを NULL に変換しません。

この関数は非決定的です。同じ引数でも、プロバイダーの状態、モデルの動作、実行時の条件によって、異なるテキストが返されたり、異なる方法で失敗したりすることがあります。

AI クエリの最適化については、[AI 入力行数の削減](ai_functions.mdx#reducing-ai-input-rows) を参照してください。

## 設定

### FE SYSTEM モデル設定

管理者は、次の動的に変更可能な FE パラメータを使用して SYSTEM モデルを設定します。endpoint と provider はすべての呼び出しで必須です。FILE オーバーロードを含め、モデルを明示しないすべてのオーバーロードでデフォルトモデルが必須です。

| パラメータ | デフォルト | 要件 |
|------------|------------|------|
| [`ai_default_chat_endpoint`](../../../administration/configuration/FE_parameters/user_query_loading.md#ai_default_chat_endpoint) | 空文字列 | 必須。チャット補完エンドポイントの完全な HTTPS POST URL。 |
| [`ai_default_chat_model`](../../../administration/configuration/FE_parameters/user_query_loading.md#ai_default_chat_model) | 空文字列 | FILE オーバーロードを含め、モデルを明示しない場合は必須。すべての呼び出しでモデルを指定する場合は省略可能。 |
| [`ai_default_chat_provider`](../../../administration/configuration/FE_parameters/user_query_loading.md#ai_default_chat_provider) | 空文字列 | 必須。`openai_compatible` または `qwen_compatible`。 |

各クエリプランは、これらの値のスナップショットを取得します。動的更新は、更新後に新しく解析および計画されたクエリにのみ適用されます。すでに構築されたプランは、取得済みのスナップショットを保持します。

### BE ローカル認証情報

AI クエリを実行する可能性があるすべての BE のローカルプロセス環境に `AI_FUNCTION_MODEL_API_KEY` を設定します。BE はこの値をローカルで読み取り、Bearer 認証情報として送信します。これは FE 設定項目ではなく、クエリプランにも含まれません。さらに、各 BE の `AI_FUNCTION_MODEL_ENDPOINT` に `ai_default_chat_endpoint` と完全に同一の HTTPS URL を設定します。プラン内の endpoint がこのローカルバインディングと一致しない場合、BE は実行を拒否します。BE はすべての DNS アドレスを検証し、link-local アドレスを拒否して、検証済みの DNS スナップショットをリクエストに固定します。完全一致するローカルバインディングにより、プライベートネットワーク上のモデル endpoint を明示的に許可できます。いずれかの BE ローカル環境変数を変更した場合、その BE を再起動する必要があります。認証情報を SQL テキストや options MAP に記述しないでください。

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

次の例では `EXPLAIN` を使用します。`EXPLAIN` はステートメントを解析して計画するだけで、関数を実行せず、HTTP リクエストも送信しません。解析時には、有効な SYSTEM 設定が必要です。

```sql
EXPLAIN SELECT ai_complete('Summarize this local test prompt.');

EXPLAIN SELECT ai_complete(
    'Classify this local test prompt.',
    map{'temperature': 0.0}
);

EXPLAIN SELECT ai_complete(
    'local-test-model',
    'Summarize this local test prompt.'
);

EXPLAIN SELECT ai_complete(
    'local-test-model',
    'Return a JSON object for this local test prompt.',
    map{'response_format': map{'type': 'json_object'}}
);
```

既存のテーブル `media_inputs` の FILE 型列 `image_file` に inline 画像が格納されている場合は、画像入力に対応するモデルを使用します。

```sql
EXPLAIN SELECT ai_complete('Describe this image.', image_file) FROM media_inputs;
```

NULL プロンプトはプロバイダーリクエストを送信しません。

```sql
SELECT ai_complete(CAST(NULL AS VARCHAR)) AS answer;
```

## 関連する関数

タスク別のチャット補助関数は [AI テキスト関数](ai_text_functions.md)、テキストまたは画像の埋め込みは [ai_embed](ai_embed.md)、登録済みプロバイダーによる呼び出しは [AI プロバイダー関数](ai_custom_functions.md)を参照してください。既存の `ai_complete` の 4 つのテキスト専用形式と SYSTEM チャットルーティングは変更されません。デフォルト AI プロバイダーを設定しても `ai_complete` のルーティングは変わらず、明示的な `model` 引数はリモートモデル名を選択します。

## キーワード

AI_COMPLETE, AI, LLM

---
displayed_sidebar: docs
description: "AI 関数を使用して、テキストの生成、ベクトル化、分類、抽出、翻訳、要約、意味の評価を行います。"
sidebar_position: 21
toc_max_heading_level: 2
---

# AI テキスト関数

このページでは、テキスト処理用のすべての AI 関数を説明します。実行する前に AI Provider を設定し、関数と選択した Provider の両方に対する使用権限を取得してください。[AI 関数の概要](ai_functions.mdx)を参照してください。

| 関数 | 用途 | 戻り値の型 |
| --- | --- | --- |
| [ai_complete](#ai_complete) | プロンプトからテキストを生成 | VARCHAR |
| [ai_embed](#ai_embed) | テキストの埋め込みベクトルを生成 | `ARRAY<FLOAT>` |
| [ai_sentiment](#ai_sentiment) | テキストの感情を分析 | VARCHAR |
| [ai_classify](#ai_classify) | 指定されたカテゴリにテキストを分類 | JSON |
| [ai_extract](#ai_extract) | 指定された情報を抽出 | JSON |
| [ai_fix_grammar](#ai_fix_grammar) | 文法とスペルを修正 | VARCHAR |
| [ai_redact](#ai_redact) | カテゴリに応じて機密情報をマスキング | VARCHAR |
| [ai_translate](#ai_translate) | テキストを翻訳 | VARCHAR |
| [ai_similarity](#ai_similarity) | 2 つのテキストの意味的な類似度を評価 | FLOAT |
| [ai_summarize](#ai_summarize) | 要約を生成 | VARCHAR |
| [ai_filter](#ai_filter) | テキストが条件を満たすか判定 | BOOLEAN |

## 共通の呼び出しルール

### 引数の指定

関数は位置引数と、`引数名 => 値` 形式の名前付き引数をサポートします。名前付き引数では順序を変更でき、前の引数のデフォルト値を指定せずにオプション引数を直接指定できます。

```sql
-- デフォルトの chat AI Provider と、その Provider のデフォルトモデルを使用します。
SELECT ai_complete('列指向ストレージについて説明してください。');

-- 位置引数: chat_provider のデフォルトモデルを chat-model で上書きします。
SELECT ai_complete('列指向ストレージについて説明してください。', 'chat_provider', 'chat-model');

-- 名前付き引数: デフォルトの chat AI Provider のデフォルトモデルを上書きします。
SELECT ai_complete(prompt => '列指向ストレージについて説明してください。', model => 'chat-model');
```

例の `chat_provider` は、作成済みの chat AI Provider を表します。

### Provider とモデル

すべての関数には、オプションの `provider` 引数と `model` 引数があります。いずれも VARCHAR 型の定数式である必要があります。

| 引数 | 省略、空文字列、SQL NULL の場合 | 明示的に指定した場合 |
| --- | --- | --- |
| `provider` | 対応するタイプのクラスターデフォルト Provider を使用 | 指定された名前の Provider を使用。クラスターデフォルトの設定は不要 |
| `model` | 選択した Provider のデフォルトモデルを使用 | 選択したサービスが提供する指定モデルを使用 |

`ai_embed` は embedding Provider を必要とし、それ以外の関数は chat Provider を必要とします。

Provider の設定にはエンドポイントとオプションの認証情報が含まれ、HTTP と HTTPS をサポートします。各実行プランは、選択した Provider の設定のスナップショットを保持します。Provider の変更やデフォルトの切り替えは、新たに計画されるクエリに反映されます。管理構文については、[CREATE AI PROVIDER](../../sql-statements/cluster-management/ai_provider/CREATE_AI_PROVIDER.md) と [SET DEFAULT AI PROVIDER](../../sql-statements/cluster-management/ai_provider/SET_DEFAULT_AI_PROVIDER.md) を参照してください。

### JSON options {/* #json-options */}

オプションの `options` 引数をサポートするのは `ai_complete` と `ai_embed` のみです。JSON 型の定数式である必要があり、SQL NULL 以外の値は JSON オブジェクトである必要があります。省略時のデフォルトは空のオブジェクトです。`json_object()` と SQL NULL は、いずれも追加オプションを渡さないことを意味します。

```sql
SELECT ai_complete(
    prompt => '列指向ストレージについて一文で説明してください。',
    options => json_object('temperature', 0.2)
);
```

options は、モデルサービスがサポートする追加のリクエストパラメータを渡すために使用します。

## ai_complete

chat Provider のチャット補完機能を使用して、プロンプトに基づくテキストを生成します。

### 構文

```sql
ai_complete(prompt[, provider[, model[, options]]])
```

### 引数

- `prompt`: モデルに実行してほしいタスクを記述する VARCHAR 式。テキスト列も使用できます。
- `options`: オプションの定数 JSON オブジェクト。[JSON options](#json-options) を参照してください。

`provider` と `model` の型およびデフォルトの動作については、[共通の呼び出しルール](#共通の呼び出しルール)を参照してください。

### 戻り値

レスポンスボディの `choices[0].message.content` に含まれるテキストを VARCHAR として返します。

### 例

```sql
SELECT ai_complete('列指向ストレージとは何かを一文で説明してください。');

SELECT ai_complete(
    prompt => '書店のキャッチコピーを一つ考えてください。',
    provider => 'support_chat',
    model => 'chat-model',
    options => json_object('temperature', 0.2)
);
```

## ai_embed

embedding Provider を使用して、テキストを埋め込みベクトルに変換します。URL 文字列は通常のテキストとして処理され、URL が指すコンテンツは取得しません。

### 構文

```sql
ai_embed(input[, provider[, model[, options]]])
```

### 引数

- `input`: ベクトル化するテキストを含む VARCHAR 式。
- `options`: オプションの定数 JSON オブジェクト。`dimensions` はデフォルトで Provider の設定を継承し、options で上書きできます。モデルとサービスが指定された次元数をサポートしている必要があります。

`provider` と `model` の型およびデフォルトの動作については、[共通の呼び出しルール](#共通の呼び出しルール)を参照してください。

### 戻り値

NULL 許容の `ARRAY<FLOAT>` を返します。サービスのレスポンスには、インデックス 0 に空でないベクトルが一つ含まれ、その要素はすべて有限の FLOAT 値である必要があります。不正なレスポンスは[エラー処理](#エラー処理)のルールに従います。

### 例

```sql
SELECT ai_embed('列指向ストレージは分析クエリに適しています。');

SELECT ai_embed(
    input => '列指向ストレージは分析クエリに適しています。',
    provider => 'text_embedding',
    model => 'embedding-model',
    options => json_object('dimensions', 256)
);
```

## ai_sentiment

テキスト全体の感情を分析します。

### 構文

```sql
ai_sentiment(input[, provider[, model]])
```

### 引数

- `input`: 分析するテキストを含む VARCHAR 式。

`provider` と `model` の型およびデフォルトの動作については、[共通の呼び出しルール](#共通の呼び出しルール)を参照してください。

### 戻り値

NULL 許容の VARCHAR を返します。有効な結果は `positive`、`negative`、`neutral`、`mixed`、`unknown` のいずれかに正規化されます。それ以外のレスポンスは行単位のエラーとして処理されます。

### 例

```sql
SELECT ai_sentiment('The delivery was excellent.');

SELECT ai_sentiment(input => 'The delivery was excellent.', provider => 'support_chat');
```

## ai_classify

指定されたカテゴリのいずれかにテキストを分類するよう、モデルに要求します。

### 構文

```sql
ai_classify(input, categories[, provider[, model]])
```

### 引数

- `input`: 分類するテキストを含む VARCHAR 式。
- `categories`: 候補となるカテゴリを含む、空でない定数 `ARRAY<VARCHAR>`。

`provider` と `model` の型およびデフォルトの動作については、[共通の呼び出しルール](#共通の呼び出しルール)を参照してください。

### 戻り値

NULL 許容の JSON オブジェクトを返します。要求する構造は `{"labels":["選択されたカテゴリ"]}` で、例えば `{"labels":["support"]}` です。

StarRocks は結果が JSON オブジェクトであることを検証しますが、labels の数、指定カテゴリへの所属、分類の正確性は検証しません。

### 例

```sql
SELECT ai_classify('Please reset my password.', ['support', 'sales']);

SELECT ai_classify(
    input => 'Please reset my password.',
    categories => ['support', 'sales'],
    provider => 'support_chat'
);
```

## ai_extract

指定されたフィールド名に基づいて、テキストから情報を抽出します。

### 構文

```sql
ai_extract(input, labels[, provider[, model]])
```

### 引数

- `input`: 情報の抽出元となるテキストを含む VARCHAR 式。
- `labels`: 抽出するフィールド名を含む、空でない定数 `ARRAY<VARCHAR>`。

`provider` と `model` の型およびデフォルトの動作については、[共通の呼び出しルール](#共通の呼び出しルール)を参照してください。

### 戻り値

NULL 許容の JSON オブジェクトを返します。要求する構造は `{"response":{"フィールド名":"値"}}` で、見つからないフィールドには JSON null を返すよう要求します。結果の例: `{"response":{"name":"Alice","city":"Paris"}}`。

StarRocks は結果が JSON オブジェクトであることを検証しますが、response のフィールド構造や抽出内容の正確性は検証しません。

### 例

```sql
SELECT ai_extract('Alice lives in Paris.', ['name', 'city']);

SELECT ai_extract(
    input => 'Alice lives in Paris.',
    labels => ['name', 'city'],
    provider => 'support_chat'
);
```

## ai_fix_grammar

文法とスペルを修正し、元の意味と語調を維持するようモデルに要求します。

### 構文

```sql
ai_fix_grammar(input[, provider[, model]])
```

### 引数

- `input`: 修正するテキストを含む VARCHAR 式。

`provider` と `model` の型およびデフォルトの動作については、[共通の呼び出しルール](#共通の呼び出しルール)を参照してください。

### 戻り値

修正されたテキストを含む、NULL 許容の VARCHAR を返します。

### 例

```sql
SELECT ai_fix_grammar('She go to school every day.');

SELECT ai_fix_grammar(input => 'She go to school every day.', provider => 'support_chat');
```

## ai_redact

指定されたカテゴリの機密情報を識別し、カテゴリを示すマーカーに置き換えるようモデルに要求します。

### 構文

```sql
ai_redact(input, categories[, provider[, model]])
```

### 引数

- `input`: 処理する元のテキストを含む VARCHAR 式。
- `categories`: マスキングする情報のカテゴリを含む、空でない定数 `ARRAY<VARCHAR>`。例えば `name`、`email`、`phone` です。

`provider` と `model` の型およびデフォルトの動作については、[共通の呼び出しルール](#共通の呼び出しルール)を参照してください。

### 戻り値

NULL 許容の VARCHAR を返します。モデルには、識別した情報を `[EMAIL]` のような大文字のカテゴリ名を角括弧で囲んだ形式に置き換え、一致する情報がない場合は元のテキストを返すよう指示します。

元のテキストはモデルサービスに送信されます。この関数は送信前にローカルでマスキングを行うものではなく、モデルが機密情報を見落とす可能性もあります。

### 例

```sql
SELECT ai_redact('Contact Alice at alice@example.com.', ['name', 'email']);

SELECT ai_redact(
    input => 'Contact Alice at alice@example.com.',
    categories => ['name', 'email'],
    provider => 'support_chat'
);
```

## ai_translate

テキストをソース言語からターゲット言語に翻訳します。

### 構文

```sql
ai_translate(input, source_language, target_language[, provider[, model]])
```

### 引数

- `input`: 翻訳するテキストを含む VARCHAR 式。
- `source_language`: ソース言語を指定する VARCHAR 式。自動検出を要求するには、空文字列 `''` を使用します。
- `target_language`: ターゲット言語を指定する VARCHAR 式。

言語には `en`、`zh`、`fr` などの識別子、またはモデルが理解できる言語名を使用できます。StarRocks では、指定可能な言語の固定リストは定義していません。

`provider` と `model` の型およびデフォルトの動作については、[共通の呼び出しルール](#共通の呼び出しルール)を参照してください。

### 戻り値

翻訳されたテキストを含む、NULL 許容の VARCHAR を返します。入力テキストが実行時に NULL の場合は NULL を返します。ソース言語の実行時 NULL は自動検出として処理します。ターゲット言語が実行時に NULL、空文字列、または ASCII 空白文字のみの場合は、リクエストを送信せずに NULL を返します。

3 つの意味に関わる引数はすべて必須であり、型指定のない NULL リテラルを直接渡すことはできません。ソース言語を自動検出する場合は、空文字列を明示的に渡してください。

### 例

```sql
SELECT ai_translate('Hello', '', 'zh');

SELECT ai_translate(
    input => 'Bonjour',
    source_language => 'fr',
    target_language => 'en',
    provider => 'support_chat'
);
```

## ai_similarity

2 つのテキストの意味的な類似度を評価するよう、チャットモデルに要求します。

### 構文

```sql
ai_similarity(input, other[, provider[, model]])
```

### 引数

- `input`: 1 つ目のテキストを含む VARCHAR 式。
- `other`: 2 つ目のテキストを含む VARCHAR 式。

`provider` と `model` の型およびデフォルトの動作については、[共通の呼び出しルール](#共通の呼び出しルール)を参照してください。

### 戻り値

`[0, 1]` の範囲の NULL 許容の FLOAT を返します。値が大きいほど、モデルが意味的に類似していると判断したことを示します。解析できないレスポンスや範囲外の値は、行単位のエラーとして処理されます。

このスコアはチャットモデルが生成するものであり、埋め込みベクトル間の距離計算ではありません。

### 例

```sql
SELECT ai_similarity('A cat is sleeping.', 'A kitten is taking a nap.');

SELECT ai_similarity(
    input => 'A cat is sleeping.',
    other => 'A kitten is taking a nap.',
    provider => 'support_chat'
);
```

## ai_summarize

主要な情報を維持した短い要約を生成するよう、モデルに要求します。

### 構文

```sql
ai_summarize(input[, provider[, model]])
```

### 引数

- `input`: 要約するテキストを含む VARCHAR 式。

`provider` と `model` の型およびデフォルトの動作については、[共通の呼び出しルール](#共通の呼び出しルール)を参照してください。

### 戻り値

モデルが生成した要約を含む、NULL 許容の VARCHAR を返します。

### 例

```sql
SELECT ai_summarize('会議では、API テストを完了してから新バージョンをリリースすることが決まりました。テスト責任者は金曜日に進捗を報告します。');

SELECT ai_summarize(
    input => '会議では、API テストを完了してから新バージョンをリリースすることが決まりました。テスト責任者は金曜日に進捗を報告します。',
    provider => 'support_chat'
);
```

## ai_filter

自然言語で記述された条件をテキストが満たすかどうかを判定します。

### 構文

```sql
ai_filter(input, condition[, provider[, model]])
```

### 引数

- `input`: 判定するテキストを含む VARCHAR 式。
- `condition`: 判定条件を自然言語で記述する VARCHAR 式。

`provider` と `model` の型およびデフォルトの動作については、[共通の呼び出しルール](#共通の呼び出しルール)を参照してください。

### 戻り値

NULL 許容の BOOLEAN を返します。モデルのレスポンスは `true` または `false` に正規化できる必要があります。それ以外のレスポンスは行単位のエラーとして処理されます。

### 例

```sql
SELECT ai_filter('The package arrived broken.', 'The customer reports damaged goods.');

SELECT ai_filter(
    input => 'The package arrived broken.',
    condition => 'The customer reports damaged goods.',
    provider => 'support_chat'
);
```

## エラー処理

`ai_function_on_error` は行単位の失敗を制御します。デフォルトの `ignore` は失敗した行に NULL を返してクエリを続行し、`fail` はクエリを中止します。解析または設定のエラー、クエリのキャンセル、期限切れは無視されません。そのため、NULL が返っても入力が NULL だったとは限らず、モデル呼び出しやレスポンスの検証に失敗した可能性もあります。

## ランタイム制限とリトライ

- テキスト生成と補助関数は [`ai_function_rate_limit_qps_chat`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_rate_limit_qps_chat) を使用し、
  埋め込み関数は [`ai_function_rate_limit_qps_embedding`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_rate_limit_qps_embedding) を使用します。それぞれの QPS 制限
  と
  [`ai_function_max_inflight`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_inflight)
  は BE ごとに独立して適用されます。QPS 制限は endpoint、credential、capability ごとのバケットで管理され、in-flight 制限はプロセス全体に適用されます。WorkGroup と Query に基づく公平な受付制御により、待機中のリクエスト間で該当する制限を共有します。初回およびリトライの各 HTTP 試行では、両方の受付許可を取得する必要があります。
- StarRocks は、モデルプロバイダーがリクエストを一度だけ実行することを保証できません。タイムアウトやその他の失敗が発生した試行でも、すでにプロバイダーに到達している可能性があるため、リトライによってプロバイダー側の処理が重複し、追加料金が発生する場合があります。プロバイダーの動作と課金方針に応じて、
  [`ai_function_max_retries`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_retries)
  と
  [`ai_function_max_retries_on_throttle`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_retries_on_throttle) を設定してください。
- リクエストとレスポンスのペイロードは Query メモリトラッカーに計上されます。未完了のリクエストがある間、実行 Pipeline はバックプレッシャーを適用します。
  [`ai_function_max_response_bytes`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_response_bytes)
  は各 HTTP レスポンスボディの上限です。
- 個別の task timeout はタスク作成後に固定され、リトライでも計時はリセットされません。Query のキャンセルと deadline の更新は、非同期実行の全期間にわたって有効です。タイムアウト、ワーカースレッド、スケジューリング粒度の制御については、その他の
  [BE AI 関数設定項目](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_request_timeout_ms)を参照してください。

リクエスト数、リトライ、タイムアウトのメトリクスについては、[AI HTTP メトリクス](../../../administration/management/monitoring/metric_details/a-c.md#ai_http_requests_total)を参照してください。

## SQL の使用制限

- AI 関数は `GROUP BY`、`SELECT DISTINCT`、集約関数の引数、ウィンドウ関数の式では使用できません。
- `IF`、`IFNULL`、`NULLIF`、`COALESCE`、`CASE` の条件式内や、テーブル関数の引数として使用できません。
- 同期マテリアライズドビューの定義、LOAD 式、生成列の式では使用できません。
- Lambda 式の本体や SQL UDF の本体では使用できません。
- AI 関数を含むステートメントでは SQL Plan Baseline の作成やバインドはできません。その他の AI 関数を含むクエリにも SPM 書き換えは適用されません。
- `PREPARE` はサポートされますが、AI 関数を含むステートメントは `EXECUTE` ごとに完全に再計画され、実行プランは再利用されません。
- 相関サブクエリのクエリブロック内では、`SELECT` リスト、`WHERE`、`HAVING`、`ORDER BY`、`JOIN ON` に含まれる AI 式は、
  そのブロックの列のみを参照していても拒否されます。結合条件内の AI 式は、`INNER JOIN` と `CROSS JOIN` でのみサポートされます。
- NULL 以外の入力行ごとに、リモートリクエストが発生する可能性があります。大量の行に AI 関数を使用する前に、ネットワーク遅延、プロバイダーのクォータ、クエリの期限、費用、データ送信ポリシーを考慮してください。

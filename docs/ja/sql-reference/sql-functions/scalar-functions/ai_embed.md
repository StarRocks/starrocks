---
displayed_sidebar: docs
description: "SYSTEM OpenAI 互換エンドポイントから NULL 許容の FLOAT 配列テキスト埋め込みを生成します。"
sidebar_position: 22
---

# ai_embed

SYSTEM OpenAI 互換 embeddings エンドポイントでテキスト埋め込みを生成します。結果は NULL 許容の `ARRAY<FLOAT>` であり、Snowflake VECTOR 型ではありません。使用する前に、すべての FE と BE をアップグレードしてください。

:::warning
テキスト、モデル、オプションが設定済みのプロバイダーに送信されます。リクエストがクラスター外に送信され、料金が発生したり、プロバイダーに保持されたりする場合があります。承認済みのエンドポイントとデータのみを使用してください。
:::

## 構文

```sql
ai_embed(text)
ai_embed(text, options)
ai_embed(model, text)
ai_embed(model, text, options)
```

- `text`: VARCHAR 式。URL 文字列もテキストとして埋め込まれ、内容は取得されません。FILE と画像入力はサポートされません。
- `model`: 省略可能な VARCHAR 式で、行ごとに変更できます。定数モデルは空白にできません。省略すると SYSTEM のデフォルト埋め込みモデルを使います。
- `options`: プロバイダーのリクエストフィールドを追加する省略可能な定数 MAP。型付き NULL MAP は空の MAP として扱われます。

[ai_complete の再帰的な options MAP ルール](ai_complete.md#options-map-のルール)が適用されますが、大文字と小文字を区別するトップレベル予約キーは `model`、`input`、`encoding_format` です。StarRocks はこれらを構築して数値の埋め込みを要求します。値は JSON 互換である必要があり、DATE、BITMAP などの非対応の値型は拒否されます。

2 引数形式の型なし NULL は `ai_embed(model, NULL)` と解釈されます。NULL options を渡すには `CAST(NULL AS MAP<VARCHAR, JSON>)` を使用してください。

## 設定

埋め込み設定はチャット設定から独立しており、SYSTEM チャットのエンドポイント、モデル、認証情報にフォールバックしません。

| 動的に変更可能な FE パラメータ | デフォルト | 要件 |
|-------------------------------|------------|------|
| [ai_default_embedding_endpoint](../../../administration/configuration/FE_parameters/user_query_loading.md#ai_default_embedding_endpoint) | 空文字列 | embeddings エンドポイントの完全な HTTPS POST URL。 |
| [ai_default_embedding_model](../../../administration/configuration/FE_parameters/user_query_loading.md#ai_default_embedding_model) | 空文字列 | 関数でモデルを指定しない場合のみ必須。 |
| [ai_default_embedding_provider](../../../administration/configuration/FE_parameters/user_query_loading.md#ai_default_embedding_provider) | 空文字列 | `openai_compatible` が必須。 |

各プランは使用する設定のスナップショットを取得します。FE 設定の変更に FE の再起動は不要です。変更後に解析・計画されるクエリに適用され、既存のプランは以前のスナップショットを保持します。

埋め込みクエリを実行するすべての BE のローカルプロセス環境で、次を設定してください。

- `AI_FUNCTION_EMBEDDING_ENDPOINT`: FE エンドポイントと完全に同一の URL。
- `AI_FUNCTION_EMBEDDING_API_KEY`: ローカルの Bearer 認証情報。FE 設定、SQL、クエリプランには含めないでください。

どちらかの環境変数を変更した後は対象 BE を再起動します。認証情報がない場合やエンドポイントのバインドが一致しない場合、実行は失敗し、チャット認証情報に置き換えられることはありません。

[ai_function_rate_limit_qps_embedding](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_rate_limit_qps_embedding) のデフォルトは、各 BE でエンドポイント、認証情報、埋め込み capability ごとに分けた bucket あたり毎秒 128 HTTP attempt です。リトライにも admission permit が必要です。プロセス全体の in-flight 制限、タイムアウト、リトライ、応答サイズ制限、[その他の実行時制御](ai_complete.md#ランタイム制限とリトライ)は AI 実行基盤と共通です。

## 戻り値と制限

応答には index 0 の埋め込みが 1 つだけ必要です。それを有限の FLOAT 値からなる空でない配列として返します。複数の埋め込み、空配列、不正な数値、範囲外の数値は拒否されます。長さはモデルとプロバイダーがサポートするオプションによって決まり、StarRocks は次元数を固定しません。

テキストまたは明示的なモデルが NULL の場合、リクエストせず NULL を返します。行レベルの失敗は [ai_function_on_error](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_on_error) に従います。解析・設定エラー、キャンセル、期限切れは無視されません。[SQL 配置制限とプリペアドステートメントの再計画ルール](ai_complete.md#制限とセキュリティ)も適用されます。

## 例

有効な SYSTEM 埋め込み設定がある場合、次の例は HTTP リクエストを送信せずにクエリを計画します。

```sql
EXPLAIN SELECT ai_embed('A local test sentence.');
EXPLAIN SELECT ai_embed('A local test sentence.', map{'dimensions': 256});
EXPLAIN SELECT ai_embed('approved-embedding-model', 'A local test sentence.');
EXPLAIN SELECT ai_embed(
    'approved-embedding-model', 'A local test sentence.', map{'dimensions': 256}
);
```

`dimensions` を含む指定オプションはプロバイダーとモデルがサポートする必要があります。SYSTEM 設定ではなく名前付き AI モデルを選択する場合は、[ai_custom_embedding](ai_model.md) を使用してください。

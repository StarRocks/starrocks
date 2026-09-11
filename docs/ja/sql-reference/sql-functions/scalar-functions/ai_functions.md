---
displayed_sidebar: docs
description: "SYSTEM チャットモデルでテキストの分類、抽出、翻訳、要約、変換を行います。"
sidebar_position: 21
---

# AI テキスト関数

これらの関数は [ai_complete](ai_complete.md#設定) と同じ SYSTEM チャットエンドポイントと認証情報を使用し、タスク用のプロンプトを作成して型付きの結果を返します。新しい関数を使用する前に、すべての FE と BE をアップグレードしてください。

:::warning
入力テキストはモデルプロバイダーに送信され、料金が発生する場合があります。承認済みのエンドポイントとデータのみを使用してください。特に、`ai_redact` は未編集の元のテキストをプロバイダーに送信するため、ローカルのプライバシーフィルターではありません。
:::

## 構文

各関数には、デフォルトのチャットモデルを使う形式と、最初の引数でモデルを指定する形式の 2 つだけがあります。これらの補助関数は options MAP を受け付けません。

```sql
ai_sentiment(text)
ai_sentiment(model, text)
ai_classify(text, categories)
ai_classify(model, text, categories)
ai_extract(text, keys)
ai_extract(model, text, keys)
ai_fix_grammar(text)
ai_fix_grammar(model, text)
ai_redact(text, categories)
ai_redact(model, text, categories)
ai_translate(text, source_language, target_language)
ai_translate(model, text, source_language, target_language)
ai_similarity(text1, text2)
ai_similarity(model, text1, text2)
ai_summarize(text)
ai_summarize(model, text)
ai_filter(text, condition)
ai_filter(model, text, condition)
```

テキスト、モデル、条件、言語の引数は VARCHAR 式です。明示的なモデルは行ごとに変更できます。定数モデルは空白にできません。モデルを省略する場合は、`ai_default_chat_model` が必要です。

`categories` と `keys` は空でない定数 `ARRAY<VARCHAR>` で、NULL または空白の要素を含めることはできません。URL を含む VARCHAR もテキストとして扱われ、StarRocks は URL の内容を取得しません。FILE、画像、マルチモーダル入力はサポートされません。

## 戻り値

すべての結果は NULL を許容し、非決定的です。

| 関数 | 型 | 意味 |
|------|----|------|
| `ai_sentiment` | VARCHAR | 正規化された `positive`、`negative`、`neutral`、`mixed`、`unknown` のいずれか。 |
| `ai_classify` | JSON | `{"labels":["category"]}` のように、1 つの分類を含むオブジェクトを要求します。 |
| `ai_extract` | JSON | `{"response":{"key":"value"}}` 形式のオブジェクトを要求し、不明な値には NULL を使います。 |
| `ai_fix_grammar` | VARCHAR | 文法とスペルを修正したテキスト。 |
| `ai_redact` | VARCHAR | 検出した値を `[EMAIL]` などの分類マーカーに置き換えたテキスト。 |
| `ai_translate` | VARCHAR | 翻訳したテキスト。ソース言語が NULL または空文字列の場合は自動検出を要求します。ターゲット言語が NULL または空文字列の場合はリクエストせず NULL を返します。 |
| `ai_similarity` | FLOAT | モデルが生成する 0 から 1 の意味的類似度スコア。ベクトル距離の計算ではありません。 |
| `ai_summarize` | VARCHAR | テキストの要約。 |
| `ai_filter` | BOOLEAN | モデルがテキストを指定条件に合致すると判断するかどうか。 |

分類と抽出の応答は有効な JSON オブジェクトでなければなりません。StarRocks は要求したオブジェクトのスキーマやモデル出力の事実の正確性までは検証しません。

テキストまたは明示的なモデルが NULL の場合は、プロバイダーにリクエストせず NULL を返します。翻訳の NULL ソース言語は上記の例外です。不正な型付き応答やその他の行レベルの失敗は [ai_function_on_error](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_on_error) に従います。`ignore` は NULL を返し、`fail` はクエリを中止します。設定エラー、キャンセル、期限切れは NULL に変換されません。

[ai_complete の SQL 配置制限、プリペアドステートメントの再計画、実行時の制限](ai_complete.md#制限とセキュリティ)も適用されます。

## 例

以下はクエリの計画のみを行います。`EXPLAIN` にも有効な SYSTEM チャット設定が必要ですが、プロバイダーにはリクエストしません。

```sql
EXPLAIN SELECT ai_sentiment('The delivery was excellent.');
EXPLAIN SELECT ai_classify('Please reset my password.', ['support', 'sales']);
EXPLAIN SELECT ai_extract('Order 42 ships Friday.', ['order', 'ship_date']);
EXPLAIN SELECT ai_translate('Hello', NULL, 'Chinese');
EXPLAIN SELECT ai_similarity('A quick reply', 'A fast response');
EXPLAIN SELECT ai_filter('The package arrived damaged.', 'describes a damaged item');
EXPLAIN SELECT ai_summarize('approved-chat-model', 'A local test paragraph.');
```

テキスト埋め込みは [ai_embed](ai_embed.md)、名前付きモデルで選択するチャットと埋め込みは [AI モデル](ai_model.md) を参照してください。

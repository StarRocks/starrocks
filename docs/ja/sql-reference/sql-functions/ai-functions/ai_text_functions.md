---
displayed_sidebar: docs
description: "AI Provider でテキストの分類、抽出、翻訳、要約、変換を行います。"
sidebar_position: 21
---

# AI テキスト関数

テキスト補助関数は `openai` プロトコルの `chat` Provider を使い、タスク別のプロンプトを作ります。各関数はデフォルトモデル、明示モデル、明示 Provider とモデルの三形式を持ち、options MAP は受け付けません。

デフォルト Provider を含め、`USAGE ON AI FUNCTION <function_name>` と `USAGE ON AI PROVIDER <provider_name>` の両方が必要です。[AI 関数の権限](ai_functions.mdx#ai-function-privileges)を参照してください。

## 構文

```sql
ai_sentiment(text)
ai_sentiment(model, text)
ai_sentiment(provider, model, text)
ai_classify(text, categories)
ai_classify(model, text, categories)
ai_classify(provider, model, text, categories)
ai_extract(text, keys)
ai_extract(model, text, keys)
ai_extract(provider, model, text, keys)
ai_fix_grammar(text)
ai_fix_grammar(model, text)
ai_fix_grammar(provider, model, text)
ai_redact(text, categories)
ai_redact(model, text, categories)
ai_redact(provider, model, text, categories)
ai_translate(text, source_language, target_language)
ai_translate(model, text, source_language, target_language)
ai_translate(provider, model, text, source_language, target_language)
ai_similarity(text1, text2)
ai_similarity(model, text1, text2)
ai_similarity(provider, model, text1, text2)
ai_summarize(text)
ai_summarize(model, text)
ai_summarize(provider, model, text)
ai_filter(text, condition)
ai_filter(model, text, condition)
ai_filter(provider, model, text, condition)
```

## パラメータ

`provider` を省略すると、補完とテキスト補助関数は `chat`、埋め込み関数は `embedding` のクラスターデフォルト Provider を使います。`model` を省略すると Provider の必須デフォルトモデルを使います。明示モデルが優先され、行ごとに変更できます。`provider` は空白でない定数文字列である必要があり、定数の明示モデルも空白にはできません。明示 Provider の呼び出しにはクラスターデフォルトの設定は不要です。

テキスト、モデル、条件、言語は VARCHAR 式です。`categories` と `keys` は NULL や空白要素を含まない、空でない定数 `ARRAY<VARCHAR>` である必要があります。FILE は非対応です。`ai_redact` は元のテキストをモデルに送信し、ローカルのプライバシーフィルターではありません。

テキスト、明示的な model、FILE 入力が NULL の場合、定数でも実行時の列値でもリクエストせず NULL を返します。NULL の options MAP はオプションなしを意味し、NULL の翻訳ソース言語は自動検出を要求します。Provider 名とヘルパー関数の配列には、それぞれの引数検証が適用されます。options 内部のネストした NULL は JSON 互換の値として扱われます。型なし NULL が複数のオーバーロードで異なる引数型に一致する場合は、`CAST(NULL AS VARCHAR)` や `CAST(NULL AS MAP<VARCHAR, JSON>)` のように型を明示してください。

言語には `en`、`zh`、`fr` などのコードを使います。NULL または空のソース言語（`''`）は自動検出を要求します。ターゲット言語が NULL の場合はリクエストせず NULL を返します。空または ASCII 空白のみのターゲット言語も同様です。固定の言語許可リストや `return_error_details` パラメータはありません。

## 戻り値

| 関数 | 型 | 意味 |
|------|----|------|
| `ai_sentiment` | VARCHAR | 正規化された `positive`、`negative`、`neutral`、`mixed`、`unknown` のいずれか。 |
| `ai_classify` | JSON | `{"labels":["category"]}` のように、1 つの分類を含むオブジェクトを要求します。 |
| `ai_extract` | JSON | `{"response":{"key":"value"}}` 形式のオブジェクトを要求し、不明な値には NULL を使います。 |
| `ai_fix_grammar` | VARCHAR | 文法とスペルを修正したテキスト。 |
| `ai_redact` | VARCHAR | 検出した値を `[EMAIL]` などの分類マーカーに置き換えたテキスト。 |
| `ai_translate` | VARCHAR | 翻訳されたテキスト。言語と NULL のルールは以下を参照してください。 |
| `ai_similarity` | FLOAT | モデルが生成する 0 から 1 の意味的類似度スコア。ベクトル距離の計算ではありません。 |
| `ai_summarize` | VARCHAR | テキストの要約。 |
| `ai_filter` | BOOLEAN | モデルがテキストを指定条件に合致すると判断するかどうか。 |

分類と抽出は JSON オブジェクトを要求しますが、要求した構造や事実の正確性は検証しません。結果は NULL 許容で非決定的です。行エラーは `ai_function_on_error` に従い、[SQL 制限とランタイム制御](ai_complete.md#制限とセキュリティ)も適用されます。

## 例

```sql
EXPLAIN SELECT ai_sentiment('The delivery was excellent.');
EXPLAIN SELECT ai_classify('Please reset my password.', ['support', 'sales']);
EXPLAIN SELECT ai_translate('Hello', '', 'zh');
EXPLAIN SELECT ai_translate('support_chat', 'chat-model', 'Bonjour', 'fr', 'en');
EXPLAIN SELECT ai_summarize('chat-model', 'A local test paragraph.');
```

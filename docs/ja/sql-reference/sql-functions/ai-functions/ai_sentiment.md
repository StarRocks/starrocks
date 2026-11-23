---
displayed_sidebar: docs
description: "テキスト全体の感情を分析します。"
products: [oss, enterprise, cloud, anywhere]
---

# ai_sentiment

テキスト全体の感情を分析します。

## 構文

```sql
ai_sentiment(input[, provider[, model]])
```

## 引数

- `input`: 分析するテキストを含む VARCHAR 式。

`provider` と `model` の型およびデフォルトの動作については、[共通の呼び出しルール](ai_functions.mdx#共通の呼び出しルール)を参照してください。

## 戻り値

NULL 許容の VARCHAR を返します。有効な結果は `positive`、`negative`、`neutral`、`mixed`、`unknown` のいずれかに正規化されます。それ以外のレスポンスは行単位のエラーとして処理されます。

## 例

```sql
SELECT ai_sentiment('The delivery was excellent.');

SELECT ai_sentiment(input => 'The delivery was excellent.', provider => 'support_chat');
```

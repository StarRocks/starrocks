---
displayed_sidebar: docs
description: "2 つのテキストの意味的な類似度を評価するよう、チャットモデルに要求します。"
products: [oss, enterprise, cloud, anywhere]
---

# ai_similarity

2 つのテキストの意味的な類似度を評価するよう、チャットモデルに要求します。

## 構文

```sql
ai_similarity(input, other[, provider[, model]])
```

## 引数

- `input`: 1 つ目のテキストを含む VARCHAR 式。
- `other`: 2 つ目のテキストを含む VARCHAR 式。

`provider` と `model` の型およびデフォルトの動作については、[共通の呼び出しルール](ai_functions.mdx#共通の呼び出しルール)を参照してください。

## 戻り値

`[0, 1]` の範囲の NULL 許容の FLOAT を返します。値が大きいほど、モデルが意味的に類似していると判断したことを示します。解析できないレスポンスや範囲外の値は、行単位のエラーとして処理されます。

このスコアはチャットモデルが生成するものであり、埋め込みベクトル間の距離計算ではありません。

## 例

```sql
SELECT ai_similarity('A cat is sleeping.', 'A kitten is taking a nap.');

SELECT ai_similarity(
    input => 'A cat is sleeping.',
    other => 'A kitten is taking a nap.',
    provider => 'support_chat'
);
```

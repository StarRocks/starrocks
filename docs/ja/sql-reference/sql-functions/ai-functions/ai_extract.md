---
displayed_sidebar: docs
description: "指定されたフィールド名に基づいて、テキストから情報を抽出します。"
products: [oss, enterprise, cloud, anywhere]
---

# ai_extract

指定されたフィールド名に基づいて、テキストから情報を抽出します。

## 構文

```sql
ai_extract(input, labels[, provider[, model]])
```

## 引数

- `input`: 情報の抽出元となるテキストを含む VARCHAR 式。
- `labels`: 抽出するフィールド名を含む、空でない定数 `ARRAY<VARCHAR>`。

`provider` と `model` の型およびデフォルトの動作については、[共通の呼び出しルール](ai_functions.mdx#共通の呼び出しルール)を参照してください。

## 戻り値

JSON オブジェクトまたは NULL を返します。StarRocks は、抽出したフィールド名と値を `response` オブジェクトに格納し、値が見つからないフィールドには JSON の `null` を入れるようモデルに指示します。例えば、名前と都市を抽出すると、`{"response":{"name":"Alice","city":"Paris"}}` のような結果が返ります。

StarRocks が確認するのは、結果が JSON オブジェクトかどうかだけです。`response` のフィールド構成や、抽出した値が正しいかどうかは確認しません。

## 例

```sql
SELECT ai_extract('Alice lives in Paris.', ['name', 'city']);

SELECT ai_extract(
    input => 'Alice lives in Paris.',
    labels => ['name', 'city'],
    provider => 'support_chat'
);
```

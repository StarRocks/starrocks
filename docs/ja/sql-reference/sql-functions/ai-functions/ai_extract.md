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

NULL 許容の JSON オブジェクトを返します。要求する構造は `{"response":{"フィールド名":"値"}}` で、見つからないフィールドには JSON null を返すよう要求します。結果の例: `{"response":{"name":"Alice","city":"Paris"}}`。

StarRocks は結果が JSON オブジェクトであることを検証しますが、response のフィールド構造や抽出内容の正確性は検証しません。

## 例

```sql
SELECT ai_extract('Alice lives in Paris.', ['name', 'city']);

SELECT ai_extract(
    input => 'Alice lives in Paris.',
    labels => ['name', 'city'],
    provider => 'support_chat'
);
```

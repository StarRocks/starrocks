---
displayed_sidebar: docs
description: "指定されたカテゴリのいずれかにテキストを分類するよう、モデルに要求します。"
products: [oss, enterprise, cloud, anywhere]
---

# ai_classify

指定されたカテゴリのいずれかにテキストを分類するよう、モデルに要求します。

## 構文

```sql
ai_classify(input, categories[, provider[, model]])
```

## 引数

- `input`: 分類するテキストを含む VARCHAR 式。
- `categories`: 候補となるカテゴリを含む、空でない定数 `ARRAY<VARCHAR>`。

`provider` と `model` の型およびデフォルトの動作については、[共通の呼び出しルール](ai_functions.mdx#共通の呼び出しルール)を参照してください。

## 戻り値

NULL 許容の JSON オブジェクトを返します。要求する構造は `{"labels":["選択されたカテゴリ"]}` で、例えば `{"labels":["support"]}` です。

StarRocks は結果が JSON オブジェクトであることを検証しますが、labels の数、指定カテゴリへの所属、分類の正確性は検証しません。

## 例

```sql
SELECT ai_classify('Please reset my password.', ['support', 'sales']);

SELECT ai_classify(
    input => 'Please reset my password.',
    categories => ['support', 'sales'],
    provider => 'support_chat'
);
```

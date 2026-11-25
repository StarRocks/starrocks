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

JSON オブジェクトまたは NULL を返します。StarRocks は、`categories` からカテゴリを 1 つ選び、`labels` 配列に入れて返すようモデルに指示します。例えば、`{"labels":["support"]}` が返ります。

StarRocks が確認するのは、結果が JSON オブジェクトかどうかだけです。`labels` の要素数、選ばれたカテゴリが `categories` に含まれるかどうか、分類が正しいかどうかは確認しません。

## 例

```sql
SELECT ai_classify('Please reset my password.', ['support', 'sales']);

SELECT ai_classify(
    input => 'Please reset my password.',
    categories => ['support', 'sales'],
    provider => 'support_chat'
);
```

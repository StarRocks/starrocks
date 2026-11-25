---
displayed_sidebar: docs
description: "自然言語で記述された条件をテキストが満たすかどうかを判定します。"
products: [oss, enterprise, cloud, anywhere]
---

# ai_filter

自然言語で記述された条件をテキストが満たすかどうかを判定します。

## 構文

```sql
ai_filter(input, condition[, provider[, model]])
```

## 引数

- `input`: 判定するテキストを含む VARCHAR 式。
- `condition`: 判定条件を自然言語で記述する VARCHAR 式。

`provider` と `model` の型およびデフォルトの動作については、[共通の呼び出しルール](ai_functions.mdx#共通の呼び出しルール)を参照してください。

## 戻り値

NULL 許容の BOOLEAN を返します。モデルのレスポンスは `true` または `false` に正規化できる必要があります。それ以外のレスポンスは行単位のエラーとして処理されます。

## 例

```sql
SELECT ai_filter('The package arrived broken.', 'The customer reports damaged goods.');

SELECT ai_filter(
    input => 'The package arrived broken.',
    condition => 'The customer reports damaged goods.',
    provider => 'support_chat'
);
```

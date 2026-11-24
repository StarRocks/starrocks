---
displayed_sidebar: docs
description: "文法とスペルを修正し、元の意味と語調を維持するようモデルに要求します。"
products: [oss, enterprise, cloud, anywhere]
---

# ai_fix_grammar

文法とスペルを修正し、元の意味と語調を維持するようモデルに要求します。

## 構文

```sql
ai_fix_grammar(input[, provider[, model]])
```

## 引数

- `input`: 修正するテキストを含む VARCHAR 式。

`provider` と `model` の型およびデフォルトの動作については、[共通の呼び出しルール](ai_functions.mdx#共通の呼び出しルール)を参照してください。

## 戻り値

修正されたテキストを含む、NULL 許容の VARCHAR を返します。

## 例

```sql
SELECT ai_fix_grammar('She go to school every day.');

SELECT ai_fix_grammar(input => 'She go to school every day.', provider => 'support_chat');
```

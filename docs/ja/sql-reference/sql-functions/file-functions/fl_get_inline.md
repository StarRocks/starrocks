---
displayed_sidebar: docs
description: "FILE 値の inline フィールドを取得します。"
sidebar_position: 80
products: [oss, enterprise, cloud, anywhere]
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# fl_get_inline

<Experimental />

[FILE](../../data-types/other-data-types/FILE.md) 値の `inline` フィールドを取得します。

## 構文

```sql
VARBINARY fl_get_inline(FILE file_expr)
```

## パラメータ

`file_expr`：FILE 式。[to_file](to_file.md) または [try_to_file](try_to_file.md) の結果も使用できます。

## 戻り値

VARBINARY 値を返します。FILE 値または `inline` フィールドが NULL の場合は NULL を返します。フィールド値はそのまま返され、デフォルト値は補完しません。空のバイト列や埋め込まれたゼロバイトも保持し、バイナリ内容のエンコードやデコードは行いません。

## 例

### 例 1：フィールドを取得する

```sql
SELECT hex(fl_get_inline(to_file(named_struct('inline', to_binary('610062', 'hex'))))) AS inline_hex;
```

```plaintext
+------------+
| inline_hex |
+------------+
| 610062     |
+------------+
```

### 例 2：フィールドが NULL の場合

```sql
SELECT hex(fl_get_inline(to_file(named_struct('uri', 'a.jpg')))) AS inline_hex;
```

```plaintext
+------------+
| inline_hex |
+------------+
| NULL       |
+------------+
```

## キーワード

FL_GET_INLINE, FILE

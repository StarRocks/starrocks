---
displayed_sidebar: docs
description: "FILE 値の uri フィールドを取得します。"
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# fl_get_uri

<Experimental />

[FILE](../../data-types/other-data-types/FILE.md) 値の `uri` フィールドを取得します。

## 構文

```sql
VARCHAR fl_get_uri(FILE file_expr)
```

## パラメータ

`file_expr`：FILE 式。[to_file](to_file.md) または [try_to_file](try_to_file.md) の結果も使用できます。

## 戻り値

VARCHAR 値を返します。FILE 値または `uri` フィールドが NULL の場合は NULL を返します。フィールド値はそのまま返され、デフォルト値は補完しません。空文字列も保持します。

## 例

### 例 1：フィールドを取得する

```sql
SELECT fl_get_uri(to_file(named_struct('uri', 'https://example.com/a.jpg'))) AS uri;
```

```plaintext
+---------------------------+
| uri                       |
+---------------------------+
| https://example.com/a.jpg |
+---------------------------+
```

### 例 2：フィールドが NULL の場合

```sql
SELECT fl_get_uri(to_file(named_struct('inline', to_binary('61', 'hex')))) AS uri;
```

```plaintext
+------+
| uri  |
+------+
| NULL |
+------+
```

## キーワード

FL_GET_URI, FILE

---
displayed_sidebar: docs
description: "FILE 値の offset フィールドを取得します。"
sidebar_position: 40
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# fl_get_offset

<Experimental />

[FILE](../../data-types/other-data-types/FILE.md) 値の `offset` フィールドを取得します。

## 構文

```sql
BIGINT fl_get_offset(FILE file_expr)
```

## パラメータ

`file_expr`：FILE 式。[to_file](to_file.md) または [try_to_file](try_to_file.md) の結果も使用できます。

## 戻り値

BIGINT 値を返します。FILE 値または `offset` フィールドが NULL の場合は NULL を返します。フィールド値はそのまま返され、デフォルト値は補完しません。

## 例

### 例 1：フィールドを取得する

```sql
SELECT fl_get_offset(to_file(named_struct('uri', 'a.jpg', 'offset', 0))) AS file_offset;
```

```plaintext
+-------------+
| file_offset |
+-------------+
| 0           |
+-------------+
```

### 例 2：フィールドが NULL の場合

```sql
SELECT fl_get_offset(to_file(named_struct('uri', 'a.jpg'))) AS file_offset;
```

```plaintext
+-------------+
| file_offset |
+-------------+
| NULL        |
+-------------+
```

## キーワード

FL_GET_OFFSET, FILE

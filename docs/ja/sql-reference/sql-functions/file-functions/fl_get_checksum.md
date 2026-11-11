---
displayed_sidebar: docs
description: "FILE 値の checksum フィールドを取得します。"
sidebar_position: 70
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# fl_get_checksum

<Experimental />

[FILE](../../data-types/other-data-types/FILE.md) 値の `checksum` フィールドを取得します。

## 構文

```sql
VARCHAR fl_get_checksum(FILE file_expr)
```

## パラメータ

`file_expr`：FILE 式。[to_file](to_file.md) または [try_to_file](try_to_file.md) の結果も使用できます。

## 戻り値

VARCHAR 値を返します。FILE 値または `checksum` フィールドが NULL の場合は NULL を返します。フィールド値はそのまま返され、デフォルト値は補完しません。空文字列も保持します。

## 例

### 例 1：フィールドを取得する

```sql
SELECT fl_get_checksum(to_file(named_struct('uri', 'a.jpg', 'checksum', 'opaque'))) AS checksum;
```

```plaintext
+----------+
| checksum |
+----------+
| opaque   |
+----------+
```

### 例 2：フィールドが NULL の場合

```sql
SELECT fl_get_checksum(to_file(named_struct('uri', 'a.jpg'))) AS checksum;
```

```plaintext
+----------+
| checksum |
+----------+
| NULL     |
+----------+
```

## キーワード

FL_GET_CHECKSUM, FILE

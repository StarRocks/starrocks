---
displayed_sidebar: docs
---

import Tip from '../../../_assets/commonMarkdown/quickstart-shared-nothing-tip.mdx';

# regexp_count

文字列内でパターンが出現する回数をカウントします。ターゲット文字列内の正規表現パターンの出現回数を返します。

## 構文

```Haskell
INT regexp_count(VARCHAR str, VARCHAR pattern)
```

## パラメータ

- `str`: 検索する文字列。サポートされているデータ型は VARCHAR です。入力が NULL の場合、NULL が返されます。

- `pattern`: 検索する正規表現パターン。サポートされているデータ型は VARCHAR です。パターンが NULL の場合、NULL が返されます。

## 戻り値

出現回数を表す INT 型の値を返します。一致が見つからない場合や入力文字列が空の場合は 0 を返します。

## 例

<Tip />

```SQL
-- 数字の出現回数をカウント
SELECT regexp_count('abc123def456', '[0-9]');
```

```plaintext
+---------------------------------------+
| regexp_count('abc123def456', '[0-9]') |
+---------------------------------------+
|                                     6 |
+---------------------------------------+
```

```SQL
-- ドットの出現回数をカウント
SELECT regexp_count('test.com test.net test.org', '\\.');
```

```plaintext
+---------------------------------------------------+
| regexp_count('test.com test.net test.org', '\\.') |
+---------------------------------------------------+
|                                                 3 |
+---------------------------------------------------+
```

```SQL
-- 空白シーケンスの出現回数をカウント
SELECT regexp_count('a b  c   d', '\\s+');
```

```plaintext
+----------------------------------------+
| regexp_count('a b  c   d', '\\s+')     |
+----------------------------------------+
|                                      3 |
+----------------------------------------+
```

```SQL
-- 繰り返しパターンの出現回数をカウント
SELECT regexp_count('ababababab', 'ab');
```

```plaintext
+------------------------------------+
| regexp_count('ababababab', 'ab')   |
+------------------------------------+
|                                  5 |
+------------------------------------+
```

```SQL
-- NULL値と空の値の使用
SELECT 
  regexp_count('', '.') AS empty_str,
  regexp_count(NULL, '.') AS null_str,
  regexp_count('abc', NULL) AS null_pattern;
```

```plaintext
+------------+----------+--------------+
| empty_str  | null_str | null_pattern |
+------------+----------+--------------+
|          0 |     NULL |         NULL |
+------------+----------+--------------+
```

```SQL
-- Unicode/中国語文字の出現回数をカウント
SELECT regexp_count('abc中文def', '[\\p{Han}]+');
```

```plaintext
+-----------------------------------------------+
| regexp_count('abc中文def', '[\\p{Han}]+')     |
+-----------------------------------------------+
|                                             1 |
+-----------------------------------------------+
```

### テーブルデータでの使用

```SQL
CREATE TABLE sample_text (
  str VARCHAR(65533) NULL,
  regex VARCHAR(65533) NULL
);

INSERT INTO sample_text VALUES 
  ('abc123def456', '[0-9]'), 
  ('test.com test.net test.org', '\\.'), 
  ('a b  c   d', '\\s+'), 
  ('ababababab', 'ab');

SELECT str, regex, regexp_count(str, regex) AS count 
FROM sample_text 
ORDER BY str;
```

```plaintext
+-----------------------------+------+-------+
| str                         | regex| count |
+-----------------------------+------+-------+
| a b  c   d                  | \s+  |     3 |
| abc123def456                | [0-9]|     6 |
| ababababab                  | ab   |     5 |
| test.com test.net test.org  | \.   |     3 |
+-----------------------------+------+-------+
```

## キーワード

REGEXP_COUNT,REGEXP,COUNT 
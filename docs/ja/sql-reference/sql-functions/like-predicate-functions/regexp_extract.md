---
displayed_sidebar: docs
---

import Tip from '../../../_assets/commonMarkdown/quickstart-shared-nothing-tip.mdx';

# regexp_extract

この関数は、正規表現パターンに一致するターゲット値の最初の一致する部分文字列を返します。パターンに一致する pos の項目を抽出します。パターンは str の一部に完全に一致する必要があり、関数はパターン内で一致する必要がある部分を返します。一致が見つからない場合、空の文字列を返します。

## 構文

```Haskell
VARCHAR regexp_extract(VARCHAR str, VARCHAR pattern, int pos)
```

## 例

<Tip />

次のデータを考えます:

```SQL
SELECT HourlySkyConditions FROM quickstart.weatherdata
WHERE HourlySkyConditions LIKE '%OVC%'
LIMIT 10;
```

```plaintext
+---------------------+
| HourlySkyConditions |
+---------------------+
| OVC:08 110          |
| OVC:08 120          |
| OVC:08 120          |
| OVC:08 30           |
| OVC:08 29           |
| OVC:08 27           |
| OVC:08 26           |
| OVC:08 22           |
| OVC:08 23           |
| OVC:08 22           |
+---------------------+
10 rows in set (0.03 sec)
```

### 文字列 `OVC: ` に続く2つの数字のセットを返す

```SQL
SELECT regexp_extract(HourlySkyConditions, 'OVC:(\\d+ \\d+)', 1) FROM quickstart.weatherdata
WHERE HourlySkyConditions LIKE '%OVC%'
LIMIT 10;
```

```plaintext
+-----------------------------------------------------------+
| regexp_extract(HourlySkyConditions, 'OVC:(\\d+ \\d+)', 1) |
+-----------------------------------------------------------+
| 08 110                                                    |
| 08 120                                                    |
| 08 120                                                    |
| 08 30                                                     |
| 08 29                                                     |
| 08 27                                                     |
| 08 26                                                     |
| 08 22                                                     |
| 08 23                                                     |
| 08 22                                                     |
+-----------------------------------------------------------+
10 rows in set (0.01 sec)
```

### 文字列 `OVC: ` に続く2番目の数字のセットのみを返す

```SQL
SELECT regexp_extract(HourlySkyConditions, 'OVC:(\\d+) (\\d+)', 2) FROM quickstart.weatherdata WHERE HourlySkyConditions LIKE '%OVC%' LIMIT 10;
```

```plaintext
+-------------------------------------------------------------+
| regexp_extract(HourlySkyConditions, 'OVC:(\\d+) (\\d+)', 2) |
+-------------------------------------------------------------+
| 110                                                         |
| 120                                                         |
| 120                                                         |
| 30                                                          |
| 29                                                          |
| 27                                                          |
| 26                                                          |
| 22                                                          |
| 23                                                          |
| 22                                                          |
+-------------------------------------------------------------+
10 rows in set (0.01 sec)
```

:::tip
同じ結果は、最初の数字のセットを一致グループ `()` で囲まず、最初のグループを返すことで得られます:

```SQL
regexp_extract(HourlySkyConditions, 'OVC:\\d+ (\\d+)', 1)
```

:::

## キーワード

REGEXP_EXTRACT,REGEXP,EXTRACT
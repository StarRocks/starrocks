---
displayed_sidebar: docs
---

# str_to_jodatime

Joda 形式の文字列を指定された Joda DateTime フォーマット（例: `yyyy-MM-dd HH:mm:ss`）で DATETIME 値に変換します。

## 構文

```Haskell
DATETIME str_to_jodatime(VARCHAR str, VARCHAR format)
```

## パラメータ

- `str`: 変換したい時間表現。VARCHAR 型である必要があります。
- `format`: 返される DATETIME 値の Joda DateTime フォーマット。利用可能なフォーマットについては、[Joda DateTime](https://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html) を参照してください。

## 戻り値

- 入力文字列の解析が成功した場合、DATETIME 値が返されます。
- 入力文字列の解析が失敗した場合、`NULL` が返されます。

## 例

例 1: 文字列 `2014-12-21 12:34:56` を `yyyy-MM-dd HH:mm:ss` フォーマットの DATETIME 値に変換します。

```SQL
MySQL > select str_to_jodatime('2014-12-21 12:34:56', 'yyyy-MM-dd HH:mm:ss');
+--------------------------------------------------------------+
| str_to_jodatime('2014-12-21 12:34:56', 'yyyy-MM-dd HH:mm:ss') |
+--------------------------------------------------------------+
| 2014-12-21 12:34:56                                          |
+--------------------------------------------------------------+
```

例 2: テキスト形式の月を含む文字列 `21/December/23 12:34:56` を `dd/MMMM/yy HH:mm:ss` フォーマットの DATETIME 値に変換します。

```SQL
MySQL > select str_to_jodatime('21/December/23 12:34:56', 'dd/MMMM/yy HH:mm:ss');
+------------------------------------------------------------------+
| str_to_jodatime('21/December/23 12:34:56', 'dd/MMMM/yy HH:mm:ss') |
+------------------------------------------------------------------+
| 2023-12-21 12:34:56                                              |
+------------------------------------------------------------------+
```

例 3: ミリ秒まで正確な文字列 `21/December/23 12:34:56.123` を `dd/MMMM/yy HH:mm:ss.SSS` フォーマットの DATETIME 値に変換します。

```SQL
MySQL > select str_to_jodatime('21/December/23 12:34:56.123', 'dd/MMMM/yy HH:mm:ss.SSS');
+--------------------------------------------------------------------------+
| str_to_jodatime('21/December/23 12:34:56.123', 'dd/MMMM/yy HH:mm:ss.SSS') |
+--------------------------------------------------------------------------+
| 2023-12-21 12:34:56.123000                                               |
+--------------------------------------------------------------------------+
```

## キーワード

STR_TO_JODATIME, DATETIME
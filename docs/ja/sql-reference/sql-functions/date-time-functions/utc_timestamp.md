---
displayed_sidebar: docs
---

# utc_timestamp

現在の UTC の日付と時刻を 'YYYY-MM-DD HH:MM:SS' または 'YYYYMMDDHHMMSS' 形式で返します。この形式は、関数の使用状況、例えば文字列または数値コンテキストによって異なります。

## Syntax

```Haskell
DATETIME UTC_TIMESTAMP()
```

## Examples

```Plain Text
MySQL > select utc_timestamp(),utc_timestamp() + 1;
+---------------------+---------------------+
| utc_timestamp()     | utc_timestamp() + 1 |
+---------------------+---------------------+
| 2019-07-10 12:31:18 |      20190710123119 |
+---------------------+---------------------+
```

`utc_timestamp() + N` は、現在の時刻に `N` 秒を追加することを意味します。

## keyword

UTC_TIMESTAMP,UTC,TIMESTAMP
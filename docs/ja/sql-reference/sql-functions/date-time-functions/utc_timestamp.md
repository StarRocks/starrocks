---
displayed_sidebar: docs
---

# utc_timestamp

## 説明

現在のUTCの日付と時刻を 'YYYY-MM-DD HH:MM:SS' または 'YYYYMMDDHHMMSS' 形式で返します。この形式は、関数の使用方法、例えば文字列または数値コンテキストによって異なります。

## 構文

```Haskell
DATETIME UTC_TIMESTAMP()
```

## 例

```Plain Text
MySQL > select utc_timestamp(),utc_timestamp() + 1;
+---------------------+---------------------+
| utc_timestamp()     | utc_timestamp() + 1 |
+---------------------+---------------------+
| 2019-07-10 12:31:18 |      20190710123119 |
+---------------------+---------------------+
```

`utc_timestamp() + N` は、現在の時刻に `N` 秒を追加することを意味します。

## キーワード

UTC_TIMESTAMP, UTC, TIMESTAMP
---
displayed_sidebar: docs
---

# utc_timestamp

## 功能

返回当前 UTC 日期时间。

如果该函数用在字符串上下文时，返回值的格式为 `YYYY-MM-DD hh:mm:ss`。如果用在数字上下文中，返回值的格式为 `YYYYMMDDhhmmss`。

## 语法

```Haskell
DATETIME UTC_TIMESTAMP()
```

## 示例

```Plain Text
-- UTC_TIMESTAMP() + N 表示在当前时间加上 N 秒。
mysql > select utc_timestamp(),utc_timestamp() + 1;
+---------------------+---------------------+
| utc_timestamp()     | utc_timestamp() + 1 |
+---------------------+---------------------+
| 2019-07-10 12:31:18 |      20190710123119 |
+---------------------+---------------------+
```

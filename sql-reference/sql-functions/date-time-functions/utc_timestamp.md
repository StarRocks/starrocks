# utc_timestamp

## 功能

返回当前UTC日期和时间的 "YYYY-MM-DD HH:MM:SS" 或"YYYYMMDDHHMMSS"格式的值。

## 语法

```Haskell
DATETIME UTC_TIMESTAMP()
```

## 示例

```Plain Text
select utc_timestamp(),utc_timestamp() + 1;
+---------------------+---------------------+
| utc_timestamp()     | utc_timestamp() + 1 |
+---------------------+---------------------+
| 2019-07-10 12:31:18 |      20190710123119 |
+---------------------+---------------------+
```

---
displayed_sidebar: docs
---

# from_unixtime_ms

## 描述

将毫秒级的 Unix 时间戳转换为日期时间字符串。目前只支持一种格式 'yyyy-MM-dd HH:mm:ss'。

该函数与 [from_unixtime](./from_unixtime.md) 类似，但接受的是毫秒级而非秒级的 Unix 时间戳。

注意，没有对应的 `unix_timestamp_ms()` 函数。

此函数受时区设置影响。更多信息，请参见[配置时区](../../../administration/management/timezone.md)。

## 语法

```Haskell
VARCHAR from_unixtime_ms(BIGINT unix_timestamp_ms)
```

## 参数

- `unix_timestamp_ms`: 要转换的毫秒级 Unix 时间戳，BIGINT 类型。如果时间戳小于 0 或大于 2147483647000，则返回 NULL。有效的时间戳范围从 1970-01-01 00:00:00.000 到 2038-01-19 11:14:07.000。

## 返回值

返回 VARCHAR 类型的日期时间字符串。

如果输入的时间戳超出范围，则返回 NULL。

## 示例

```Plain Text
mysql> SELECT from_unixtime_ms(1196440219000);
+------------------------------+
| from_unixtime_ms(1196440219000) |
+------------------------------+
| 2007-12-01 00:30:19          |
+------------------------------+

mysql> SELECT from_unixtime_ms(1196440219123);
+------------------------------+
| from_unixtime_ms(1196440219123) |
+------------------------------+
| 2007-12-01 00:30:19          |
+------------------------------+

```

## 关键词

FROM_UNIXTIME_MS, FROM, UNIXTIME, MS, MILLISECONDS

## 相关函数

- [from_unixtime](./from_unixtime.md): 将秒级的 Unix 时间戳转换为日期时间字符串
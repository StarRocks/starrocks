---
displayed_sidebar: "Chinese"
---

# from_unixtime_milliseconds

将以毫秒为单位的 UNIX 时间戳转化为对应的时间格式。返回的时间格式由 `string_format` 指定，默认为 `%Y-%m-%d %H:%i:%s.%f`，也支持 [date_format](./date_format.md) 中的格式。

该函数受时区影响，具体参见 [设置时区](../../../administration/management/timezone.md)。

## 语法

```Haskell
VARCHAR from_unixtime_milliseconds(BIGINT unix_timestamp_ms[, VARCHAR string_format])
```

## 参数说明

`unix_timestamp_ms` 要转化的以毫秒为单位的 UNIX 时间戳，BIGINT 类型。如果给定的时间戳小于 0 或大于 2147483647000，则返回 NULL。即时间戳范围是：

1970-01-01 00:00:00 ~ 2038-01-19 11:14:07。

`string_format`: 可选，指定的时间格式。

## 返回值说明

返回 VARCHAR 类型的 DATETIME 或 DATE 值。如果 `string_format` 指定的是 DATE 格式，则返回 VARCHAR 类型的 DATE 值。

如果输入的时间戳超过范围，返回 NULL。如果 `string_format` 指定的格式非法，则返回 NULL。

## 示例

```Plain Text
select from_unixtime_milliseconds(100);
+---------------------------------+
| from_unixtime_milliseconds(100) |
+---------------------------------+
| 1970-01-01 00:00:00.100000      |
+---------------------------------+

select from_unixtime_milliseconds(100, '%y-%m-%d');
+---------------------------------------------+
| from_unixtime_milliseconds(100, '%y-%m-%d') |
+---------------------------------------------+
| 70-01-01                                    |
+---------------------------------------------+

select from_unixtime_milliseconds(9999123, '%Y-%m-%d %H:%i:%s.%f');
+-------------------------------------------------------------+
| from_unixtime_milliseconds(9999123, '%Y-%m-%d %H:%i:%s.%f') |
+-------------------------------------------------------------+
| 1970-01-01 02:46:39.123000                                  |
+-------------------------------------------------------------+
```

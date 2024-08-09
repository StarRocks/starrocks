---
displayed_sidebar: "Chinese"
---

# from_unixtime_ms

将以毫秒为单位的 UNIX 时间戳转换为 DATETIME 类型的值。


该函数受时区影响，具体参见 [设置时区](../../../administration/management/timezone.md)。

## 语法

```Haskell
VARCHAR from_unixtime_ms(BIGINT unix_timestamp_ms)
```

## 参数说明

`unix_timestamp_ms` 要转化的 UNIX 时间戳，BIGINT 类型。如果给定的时间戳小于 0 或大于 2147483647000，则返回 NULL。即时间戳范围是：

1970-01-01 00:00:00 ~ 2038-01-19 11:14:07。

## 返回值说明

返回 DATETIME。

如果输入的时间戳超过范围，返回 NULL。

## 示例

```Plain Text
select from_unixtime_ms(1000);
+------------------------+
| from_unixtime_ms(1000) |
+------------------------+
| 1970-01-01 00:00:01    |
+------------------------+

select from_unixtime_ms(9001);
+------------------------+
| from_unixtime_ms(9001) |
+------------------------+
| 1970-01-01 00:00:09    |
+------------------------+
```

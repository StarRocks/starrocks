---
displayed_sidebar: "Chinese"
---

# from_unixtime

## 功能

将 UNIX 时间戳转化为对应的时间格式。返回的时间格式由 `string_format` 指定，默认为 `yyyy-MM-dd HH:mm:ss`，也支持 [date_format](./date_format.md) 中的格式。

目前 `string_format` 支持如下格式，其余 `string_format` 格式非法，返回 NULL。

```plain text
%Y：年。例：2014，1900
%m：月。例：12，09
%d：日。例：11，01
%H：时。例：23，01，12
%i：分。例：05，11
%s：秒。例：59，01
```

该函数受时区影响，具体参见 [设置时区](../../../administration/timezone.md)。

## 语法

```Haskell
VARCHAR FROM_UNIXTIME(INT unix_timestamp[, VARCHAR string_format])
```

## 参数说明

`unix_timestamp`: 要转化的 UNIX 时间戳，INT 类型。如果给定的时间戳小于 0 或大于 2147483647，则返回 NULL。即时间戳范围是：

1970-01-01 00:00:00 ~ 2038-01-19 11:14:07。

`string_format`: 可选，指定的时间格式。

## 返回值说明

返回 VARCHAR 类型的 DATETIME 或 DATE 值。如果 `string_format` 指定的是 DATE 格式，则返回 VARCHAR 类型的 DATE 值。

如果输入的时间戳超过范围，返回 NULL。如果 `string_format` 指定的格式非法，则返回 NULL。

## 示例

```plain text
MySQL > select from_unixtime(1196440219);
+---------------------------+
| from_unixtime(1196440219) |
+---------------------------+
| 2007-12-01 00:30:19       |
+---------------------------+

MySQL > select from_unixtime(1196440219, 'yyyy-MM-dd HH:mm:ss');
+--------------------------------------------------+
| from_unixtime(1196440219, 'yyyy-MM-dd HH:mm:ss') |
+--------------------------------------------------+
| 2007-12-01 00:30:19                              |
+--------------------------------------------------+

MySQL > select from_unixtime(1196440219, '%Y-%m-%d');
+-----------------------------------------+
| from_unixtime(1196440219, '%Y-%m-%d')   |
+-----------------------------------------+
| 2007-12-01                              |
+-----------------------------------------+

MySQL > select from_unixtime(1196440219, '%Y-%m-%d %H:%i:%s');
+--------------------------------------------------+
| from_unixtime(1196440219, '%Y-%m-%d %H:%i:%s')   |
+--------------------------------------------------+
| 2007-12-01 00:30:19                              |
+--------------------------------------------------+
```

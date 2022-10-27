# from_unixtime

## description

### Syntax

`DATETIME FROM_UNIXTIME(INT unix_timestamp[, VARCHAR string_format])`

将 unix 时间戳转化为对应的 time 格式，返回的格式由 `string_format` 指定

默认为 yyyy-MM-dd HH:mm:ss ,也支持date_format中的format格式

传入的是整形，返回的是字符串类型

目前 `string_format` 支持格式：

```plain text
%Y：年。例：2014，1900
%m：月。例：12，09
%d：日。例：11，01
%H：时。例：23，01，12
%i：分。例：05，11
%s：秒。例：59，01
```

该函数受时区影响，具体参见 [设置时区](../../../using_starrocks/timezone.md)。

## 语法

```Haskell
DATETIME|DATE FROM_UNIXTIME(INT unix_timestamp[, VARCHAR string_format])
```

## 参数说明

`unix_timestamp`: 要转化的 UNIX 时间戳，INT 类型。如果给定的时间戳小于 0 或大于 2147483647，则返回 NULL。即时间戳范围是：

1970-01-01 00:00:00 ~ 2038-01-19 11:14:07。

`string_format`: 可选，指定的时间格式。

如果给定的时间戳小于 0 或大于 253402271999，则返回 NULL。即时间戳范围是：

返回 DATETIME 或 DATE 类型的值。如果 `string_format` 指定的是 DATE 格式，则返回 DATE 类型的值。

## example

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

## keyword

FROM_UNIXTIME,FROM,UNIXTIME

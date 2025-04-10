---
displayed_sidebar: docs
---

# str_to_jodatime

## 功能

将某一个 Joda 格式的字符串转换为指定的 Joda DateTime 格式（如 `yyyy-MM-dd HH:mm:ss`）的 DATETIME 值。

## 语法

```Haskell
DATETIME str_to_jodatime(VARCHAR str, VARCHAR format)
```

## 参数说明

- `str`：待计算转换的时间表达式，取值必须是 VARCHAR 数据类型。
- `format`：计算转换后生成的 DATETIME 值的 Joda DateTime 格式。参见 [Joda DateTime](https://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html)。

## 返回值说明

- 如果输入的字符串解析成功，系统返回 DATETIME 类型的值。
- 如果输入的字符串解析失败，系统返回 `NULL`。

## 示例

示例一：将字符串 `2014-12-21 12:34:56` 转换成 `yyyy-MM-dd HH:mm:ss` 格式的 DATETIME 值。

```SQL
MySQL > select str_to_jodatime('2014-12-21 12:34:56', 'yyyy-MM-dd HH:mm:ss');
+--------------------------------------------------------------+
| str_to_jodatime('2014-12-21 12:34:56', 'yyyy-MM-dd HH:mm:ss') |
+--------------------------------------------------------------+
| 2014-12-21 12:34:56                                          |
+--------------------------------------------------------------+
```

示例二：将字符串 `21/December/23 12:34:56`（字符串所表示的时间中月份通过单词文本表示）转换成 `dd/MMMM/yy HH:mm:ss` 格式的 DATETIME 值。

```SQL
MySQL > select str_to_jodatime('21/December/23 12:34:56', 'dd/MMMM/yy HH:mm:ss');
+------------------------------------------------------------------+
| str_to_jodatime('21/December/23 12:34:56', 'dd/MMMM/yy HH:mm:ss') |
+------------------------------------------------------------------+
| 2023-12-21 12:34:56                                              |
+------------------------------------------------------------------+
```

示例三：将字符串 `21/December/23 12:34:56.123` （字符串所表示的时间精确到毫秒）转换成 `dd/MMMM/yy HH:mm:ss.SSS` 格式的 DATETIME 值。

```SQL
MySQL > select str_to_jodatime('21/December/23 12:34:56.123', 'dd/MMMM/yy HH:mm:ss.SSS');
+--------------------------------------------------------------------------+
| str_to_jodatime('21/December/23 12:34:56.123', 'dd/MMMM/yy HH:mm:ss.SSS') |
+--------------------------------------------------------------------------+
| 2023-12-21 12:34:56.123000                                               |
+--------------------------------------------------------------------------+
```

## 关键字

STR_TO_JODATIME, DATETIME

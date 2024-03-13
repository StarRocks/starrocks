---
displayed_sidebar: "Chinese"
---

# month

## 功能

返回指定日期中的月份。

## 语法

```Haskell
INT MONTH(DATE|DATETIME date)
```

## 参数说明

`date`：必填。支持DATE和DATETIME类型。

## 返回值说明

返回 INT 类型的值，范围 1~12。此函数会对整数和字符串类型的输入进行隐式转换，如果未能从输入中解析出合法月份，如 `month('string')`，则返回 NULL。如果输入数据类型非法，如 `month(3.1415)`，则返回报错。

## 示例

示例一：返回 '1987-01-01' 中的月份 `1`。

```Plain Text
select month('1987-01-01');
+---------------------+
| month('1987-01-01') |
+---------------------+
|                   1 |
+---------------------+
1 row in set (0.01 sec)
```

示例二：返回当前月份。

```Plain Text
select month(now());
+--------------+
| month(now()) |
+--------------+
|            7 |
+--------------+
1 row in set (0.01 sec)
```

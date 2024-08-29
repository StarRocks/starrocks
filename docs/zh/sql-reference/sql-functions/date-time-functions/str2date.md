---
displayed_sidebar: docs
---

# str2date

## 功能

按照 `format` 指定的格式将 `str` 转换为 DATE 类型的值。如果转换结果不对，返回 NULL。

该函数与 [str_to_date](../date-time-functions/str_to_date.md) 函数功能相同，只是返回值数据类型不同。

## 语法

```Haskell
DATE str2date(VARCHAR str, VARCHAR format);
```

## 参数说明

`str`: 要转换的时间字符串，支持的数据类型为 VARCHAR。

`format`: 指定的时间格式，支持的数据类型为 VARCHAR。支持的时间格式与 [date_format](./date_format.md) 函数一致。

## 返回值说明

返回值的数据类型为 DATE，精确到年-月-日。

## 示例

```Plain
select str2date('2010-11-30 23:59:59', '%Y-%m-%d %H:%i:%s');
+------------------------------------------------------+
| str2date('2010-11-30 23:59:59', '%Y-%m-%d %H:%i:%s') |
+------------------------------------------------------+
| 2010-11-30                                           |
+------------------------------------------------------+
1 row in set (0.01 sec)
```

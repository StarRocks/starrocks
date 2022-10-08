# str2date

## 功能

通过 `format` 指定的方式将 `str` 转化为 `DATE` 类型，如果转化结果不对返回 NULL。

>注：功能与 [str_to_date](../date-time-functions/str_to_date.md) 函数相同，只是返回值数据类型不同。

## 语法

```Haskell
DATE str2date(VARCHAR str, VARCHAR format);
```

## 参数说明

`str`: 支持的数据类型为 VARCHAR。

`format`: 支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 DATE，精确到年-月-日。

## 示例

```Plain Text
select str2date('2010-11-30 23:59:59', '%Y-%m-%d %H:%i:%s');
+------------------------------------------------------+
| str2date('2010-11-30 23:59:59', '%Y-%m-%d %H:%i:%s') |
+------------------------------------------------------+
| 2010-11-30                                           |
+------------------------------------------------------+
1 row in set (0.01 sec)
```

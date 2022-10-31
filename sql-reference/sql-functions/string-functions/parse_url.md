# parse_url

## 功能

将参数 `expr2` 从 `expr1` 中解析出来作为结果返回。

## 语法

```Haskell
parse_url(expr1,expr2);
```

## 参数说明

`expr1`: 支持的数据类型为 VARCHAR。

`expr2`: 支持的数据类型为 VARCHAR，可为 PROTOCOL、HOST、 PATH、 REF、 AUTHORITY、 FILE、 USERINFO、 QUERY（不支持返回 QUERY 里面的特定参数）。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
mysql> select  parse_url('https://www.starrocks.com/zh-CN/index', 'HOST');
+------------------------------------------------------------+
| parse_url('https://www.starrocks.com/zh-CN/index', 'HOST') |
+------------------------------------------------------------+
| www.starrocks.com                                          |
+------------------------------------------------------------+
1 row in set (0.00 sec)
```

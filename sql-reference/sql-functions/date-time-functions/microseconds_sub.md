# microseconds_sub

## 功能

从参数 `expr1` 中减去指定的时间间隔，单位为微秒。

## 语法

```Haskell
DATETIME microseconds_sub(DATETIME expr1,INT expr2);
```

## 参数说明

`expr1`: 支持的数据类型为 DATETIME。

`expr2`: 支持的数据类型为 INT。

## 返回值说明

返回值的数据类型为 DATETIME。

## 示例

```Plain Text
select microseconds_sub('2010-11-30 23:50:50', 2);
+--------------------------------------------+
| microseconds_sub('2010-11-30 23:50:50', 2) |
+--------------------------------------------+
| 2010-11-30 23:50:49.999998                 |
+--------------------------------------------+
1 row in set (0.00 sec)
```

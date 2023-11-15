# timestamp

## 功能

将时间表达式 `expr` 转换为 DATETIME 值。

## 语法

```Haskell
DATETIME timestamp(DATETIME|DATE expr);
```

## 参数说明

`expr`: 要转换的日期或日期时间值，支持的数据类型为 DATETIME 或 DATE。

## 返回值说明

返回值的数据类型为 DATETIME。如果输入的日期为空或者不存在，比如 2021-02-29，则返回 NULL。

## 示例

```Plain Text
select timestamp("2019-05-27");
+-------------------------+
| timestamp('2019-05-27') |
+-------------------------+
| 2019-05-27 00:00:00     |
+-------------------------+
1 row in set (0.00 sec)
```

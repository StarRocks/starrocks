# weeks_sub

## 功能

返回原始的日期减去若干周后的日期。

## 语法

```Haskell
DATETIME weeks_sub(DATETIME|DATE expr1, INT expr2)
```

## 参数说明

`expr1`: 原始的日期，支持的数据类型为 DATETIME 或 DATE。如果输入值为 DATE，会隐式转换为 DATETIME。

`expr2`: 要减去的周数，数据类型为 `INT`。

## 返回值说明

返回值的数据类型为 `DATETIME`。如果日期不存在，则返回 NULL。

## 示例

```Plain Text
select weeks_sub('2022-12-22',2);
+----------------------------+
| weeks_sub('2022-12-22', 2) |
+----------------------------+
|        2022-12-08 00:00:00 |
+----------------------------+
```

---
displayed_sidebar: docs
---

# weeks_add

## 功能

返回原始的日期加上指定周数后的日期。

## 语法

```Haskell
DATETIME weeks_add(DATETIME|DATE expr1, INT expr2)
```

## 参数说明

`expr1`: 原始的日期，支持的数据类型为 DATETIME 或 DATE。

`expr2`: 想要增加的周数，支持的数据类型为 `INT`。

## 返回值说明

返回值的数据类型为 DATETIME。如果日期不存在，则返回 NULL。

## 示例

```Plain Text
select weeks_add('2022-12-20',2);
+----------------------------+
| weeks_add('2022-12-20', 2) |
+----------------------------+
|        2023-01-03 00:00:00 |
+----------------------------+
```

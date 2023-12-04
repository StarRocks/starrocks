---
displayed_sidebar: "Chinese"
---

# asin

## 功能

求 `x` 的反正弦值(单位为弧度)， x 为一个数值

## 语法

```Haskell
ASIN(x);
```

## 参数说明

`x`: 支持的数据类型为 DOUBLE

## 返回值说明

返回值的数据类型为 DOUBLE

## 示例

```Plain Text
mysql> select asin(0.25);
+---------------------+
| asin(0.25)          |
+---------------------+
| 0.25268025514207865 |
+---------------------+
1 row in set (0.01 sec)
```

## 关键词

ASIN

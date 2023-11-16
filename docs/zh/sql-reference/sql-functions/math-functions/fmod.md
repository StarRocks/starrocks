---
displayed_sidebar: "Chinese"
---

# fmod

## 功能

返回模系下参数 `x` 对参数 `y` 取余的余数

## 语法

```Haskell
FMOD(x,y);
```

## 参数说明

`x`: 支持的数据类型为 DOUBLE、FLOAT

`y`: 支持的数据类型为 DOUBLE、FLOAT

## 返回值说明

返回值的数据类型为与参数 `x` 类型相同

## 示例

```Plain Text
mysql> select fmod(3.14,3.14);
+------------------+
| fmod(3.14, 3.14) |
+------------------+
|                0 |
+------------------+
1 row in set (0.04 sec)

mysql> select fmod(3,6);
+------------+
| fmod(3, 6) |
+------------+
|          3 |
+------------+
1 row in set (0.01 sec)
```

## 关键词

FMOD

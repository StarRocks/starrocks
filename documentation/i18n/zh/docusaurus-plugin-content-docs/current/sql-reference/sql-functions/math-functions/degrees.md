---
displayed_sidebar: "Chinese"
---

# degrees

## 功能

将参数 `x` 转成角度，`x` 是弧度。

## 语法

```Haskell
DEGREES(x);
```

## 参数说明

`x`: 支持的数据类型为 DOUBLE。

## 返回值说明

返回值的数据类型为 DOUBLE。

## 示例

```Plain Text
mysql> select degrees(3.1415926535898);
+--------------------------+
| degrees(3.1415926535898) |
+--------------------------+
|        180.0000000000004 |
+--------------------------+
1 row in set (0.07 sec)
```

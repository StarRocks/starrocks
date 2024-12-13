---
displayed_sidebar: docs
---

# length

<<<<<<< HEAD
## 功能
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

返回字符串的 **字节** 长度。

## 语法

```Haskell
length(str)
```

## 参数说明

`str`: 支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 INT。

## 示例

```Plain Text
MySQL > select length("abc");
+---------------+
| length('abc') |
+---------------+
|             3 |
+---------------+

MySQL > select length("中国");
+------------------+
| length('中国')   |
+------------------+
|                6 |
+------------------+
```

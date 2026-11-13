---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "返回字符串的字节长度。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# length



返回字符串的 **字节** 长度。

## 语法

```plaintext
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

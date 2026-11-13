---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "根据输入的 ASCII 值返回对应的字符。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# char



根据输入的 ASCII 值返回对应的字符。

## 语法

```plaintext
char(x);
```

## 参数说明

`x`: 支持的数据类型为 INT。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
mysql> SELECT CHAR(77);
+----------+
| char(77) |
+----------+
| M        |
+----------+
1 row in set (0.00 sec)
```

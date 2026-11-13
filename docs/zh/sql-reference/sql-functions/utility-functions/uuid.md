---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "以 VARCHAR 形式返回一个随机的 UUID 值。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# uuid



以 VARCHAR 形式返回一个随机的 UUID 值。长度为36个字符，包含32个十六进制字符，由4个连字符进行连接，形式为8-4-4-4-12。

## 语法

```plaintext
uuid();
```

## 参数说明

无

## 返回值说明

返回 VARCHAR 类型的值。

## 示例

```Plain Text
mysql> select uuid();
+--------------------------------------+
| uuid()                               |
+--------------------------------------+
| 74a2ed19-9d21-4a99-a67b-aa5545f26454 |
+--------------------------------------+
1 row in set (0.01 sec)
```

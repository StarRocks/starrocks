---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "返回以弧度表示的角度x的正弦值。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# sin



计算参数 `x` 的正弦，`x` 为弧度值。

## 语法

```plaintext
SIN(x);
```

## 参数说明

`x`: 支持的数据类型为 DOUBLE。

## 返回值说明

返回值的数据类型为 DOUBLE。

## 示例

```Plain Text
mysql> select sin(3.14);
+-----------------------+
| sin(3.14)             |
+-----------------------+
| 0.0015926529164868282 |
+-----------------------+
1 row in set (0.21 sec)
```

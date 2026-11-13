---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "返回当前 MySQL 数据库的版本。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# version



返回当前 MySQL 数据库的版本。可以使用 `current_version` 函数查询 StarRocks 当前的版本。

## 语法

```plaintext
version();
```

## 参数说明

无。该函数不接受任何参数。

## 返回值说明

返回 VARCHAR 类型的值。

## 示例

```Plain Text
mysql> select version();
+-----------+
| version() |
+-----------+
| 5.1.0     |
+-----------+
1 row in set (0.00 sec)
```

## 相关文档

[current_version](./current_version.md)

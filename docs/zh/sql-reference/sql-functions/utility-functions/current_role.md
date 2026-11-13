---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "获取当前用户激活的角色。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# current_role



获取当前用户激活的角色。该函数从 3.0 版本开始支持。

## 语法

```plaintext
current_role();
current_role;
```

## 参数说明

无

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain
mysql> select current_role();
+----------------+
| current_role() |
+----------------+
| db_admin       |
+----------------+
```

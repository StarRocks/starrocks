---
displayed_sidebar: docs
---

# current_role

## 功能

获取当前用户激活的角色。该函数从 3.0 版本开始支持。

## 语法

```Haskell
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

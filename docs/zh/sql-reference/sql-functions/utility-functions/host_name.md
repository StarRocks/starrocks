---
displayed_sidebar: docs
---

# host_name

<<<<<<< HEAD
## 功能
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

获取计算所在节点的主机名。该函数从 2.5 版本开始支持。

## 语法

```Haskell
host_name();
```

## 参数说明

无

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
mysql> select host_name();
+-------------+
| host_name() |
+-------------+
| sandbox-sql |
+-------------+
1 row in set (0.01 sec)
```

---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "返回 ConnectorTblMetaInfoMgr 中从外部基表到物化视图的映射信息。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# inspect_mv_relationships

`inspect_mv_relationships()`

此函数返回 `ConnectorTblMetaInfoMgr` 中的内容，其中包含从基础外部表到物化视图的映射信息。

## 参数

无。

## 返回值

返回包含映射信息的 JSON 格式的 VARCHAR 字符串。

## 示例

示例1: 检查 ConnectorTblMetaInfoMgr 中当前的 mv 关系:
```
mysql> select inspect_mv_relationships();
+----------------------------+
| inspect_mv_relationships() |
+----------------------------+
| {}                         |
+----------------------------+
1 row in set (0.01 sec)

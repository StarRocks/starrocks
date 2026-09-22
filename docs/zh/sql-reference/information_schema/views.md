---
displayed_sidebar: docs
description: "views 提供所有用户定义视图的信息。"
---

# views

`views` 提供有关所有用户定义视图的信息。

`views` 提供以下字段：

| 字段                 | 描述                                                         |
| -------------------- | ------------------------------------------------------------ |
| TABLE_CATALOG        | 视图所属的目录名称。此值始终为 def。                         |
| TABLE_SCHEMA         | 视图所属的数据库名称。                                       |
| TABLE_NAME           | 视图的名称。                                                 |
| VIEW_DEFINITION      | 提供视图定义的SELECT语句。                                   |
| CHECK_OPTION         | CHECK_OPTION 属性的值。StarRocks 的视图不支持 WITH CHECK OPTION，因此该值始终为 NONE。 |
| IS_UPDATABLE         | 视图是否可更新。该列不填充数据，因此该值始终为 NO。 |
| DEFINER              | 创建视图的用户。该列不填充数据，因此该值始终为空字符串。 |
| SECURITY_TYPE        | 视图的 SQL SECURITY 特性。该列不填充数据，因此该值始终为空字符串，不反映 [CREATE VIEW](../sql-statements/View/CREATE_VIEW.md) 的 SECURITY 子句。 |
| CHARACTER_SET_CLIENT | 创建该视图的客户端连接所使用的字符集。该值始终为 utf8。 |
| COLLATION_CONNECTION | 创建该视图的客户端连接所使用的排序规则。该值始终为 utf8_general_ci。 |

---
displayed_sidebar: docs
---

# inspect_related_mv

`inspect_related_mv(table_name)`

此函数以 JSON 数组格式返回表的关联物化视图。

## 参数

`table_name`: 表的名称 (VARCHAR)。

## 返回值

返回包含关联物化视图的 JSON 数组的 VARCHAR 字符串。

## 示例

示例1: 检查 `ss` 表的关联物化视图:
```
mysql> select inspect_related_mv('ss');
+---------------------------------------------------------------------+
| inspect_related_mv('ss')                                            |
+---------------------------------------------------------------------+
| [{"id":28806,"name":"mv_on_view_1"},{"id":28844,"name":"test_mv1"}] |
+---------------------------------------------------------------------+
1 row in set (0.01 sec)

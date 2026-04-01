---
displayed_sidebar: docs
sidebar_label: "Alias"
---

# 别名

在查询中编写表名、列名或包含列的表达式时，您可以为它们分配别名。别名通常比原始名称更短，更容易记住。

当需要别名时，您只需在 SELECT 列表或 FROM 列表中的表名、列名和表达式名称后添加 AS 子句。AS 关键字是可选的。您也可以直接在原始名称后指定别名，而无需使用 AS。

如果别名或其他标识符与内部 [StarRocks 关键字](../../keywords.md) 同名，则需要将名称用一对反引号括起来，例如 `rank`。

别名区分大小写，但列别名和表达式别名不区分大小写。

示例：

```sql
select tiny_column as name, int_column as sex from big_table;

select sum(tiny_column) as total_count from big_table;

select one.tiny_column, two.int_column from small_table one, <br/> big_table two where one.tiny_column = two.tiny_column;
```

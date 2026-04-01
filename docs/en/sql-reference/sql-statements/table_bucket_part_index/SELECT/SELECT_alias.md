---
displayed_sidebar: docs
sidebar_label: "Alias"
---

# Alias

When you write the names of tables, columns, or expressions that contain columns in a query, you can assign them an alias. Aliases are usually shorter and better to remember than original names.

When an alias is needed, you can simply add an AS clause after the table, column, and expression names in the select list or from list. The AS keyword is optional. You can also specify aliases directly after the original name without using AS.

If an alias or other identifier has the same name as an internal [StarRocks keyword](../../keywords.md), you need to enclose the name in a pair of backticks, for example, `rank`.

Aliases are case-sensitive, but column aliases and expression aliases are not case-sensitive.

Examples:

```sql
select tiny_column as name, int_column as sex from big_table;

select sum(tiny_column) as total_count from big_table;

select one.tiny_column, two.int_column from small_table one, <br/> big_table two where one.tiny_column = two.tiny_column;
```

---
displayed_sidebar: "Chinese"
---

# ADD SQLBLACKLIST

## 功能

将一个 SQL 正则表达式添加至 SQL 黑名单。启用 SQL 黑名单功能后，StarRocks 会将所有需要执行的 SQL 语句与黑名单中的 SQL 正则表达式进行比较。StarRocks 不会执行与黑名单中任何正则表达式相匹配的 SQL，并返回错误。

有关 SQL 黑名单的更多信息，请参阅 [管理 SQL 黑名单](../../../administration/Blacklist.md)。

## 语法

```SQL
ADD SQLBLACKLIST "<sql_reg_expr>"
```

## 参数说明

`sql_reg_expr`：用于指定 SQL 语句的正则表达式。为了区分 SQL 语句中的特殊字符和正则表达式中的特殊字符，您需要使用转义符（\）作为 SQL 语句中特殊字符的前缀，如 `(`、`)` 以及 `+`。由于 `(` 和 `)` 在 SQL 语句中经常被使用，StarRocks 可以直接识别 SQL 语句中的 `(` 和 `)`。所以您无需为 `(` 和 `)` 添加转义符。

## 示例

示例一：将 `count(\*)` 添加到 SQL 黑名单。

```Plain
mysql> ADD SQLBLACKLIST "select count(\\*) from .+";
```

示例二：将 `count(distinct )` 添加到 SQL 黑名单。

```Plain
mysql> ADD SQLBLACKLIST "select count(distinct .+) from .+";
```

示例三：将 `order by limit x, y，1 <= x <=7, 5 <=y <=7` 加入SQL黑名单。

```Plain
mysql> ADD SQLBLACKLIST "select id_int from test_all_type_select1 
    order by id_int 
    limit [1-7], [5-7]";
```

示例四：将复杂的 SQL 正则表达式添加到 SQL 黑名单中。该示例是为了演示如何使用转义符来表示 SQL 语句中的 `*` 和 `-`。

```Plain
mysql> ADD SQLBLACKLIST 
    "select id_int \\* 4, id_tinyint, id_varchar 
        from test_all_type_nullable 
    except select id_int, id_tinyint, id_varchar 
        from test_basic 
    except select (id_int \\* 9 \\- 8) \\/ 2, id_tinyint, id_varchar 
        from test_all_type_nullable2 
    except select id_int, id_tinyint, id_varchar 
        from test_basic_nullable";
```

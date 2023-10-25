# 黑名单管理

StarRocks的客户可以维护一个SQL黑名单，在某些场景下禁止指定的一类SQL，避免这类SQL导致集群crash或者其他预期之外的行为。
客户可以自由添加/浏览/删除 SQL黑名单。

## 语法

这个功能通过FE配置enable_sql_blacklist控制(默认关闭)，开启命令如下：

~~~sql
admin set frontend config ("enable_sql_blacklist" = "true")
~~~

Admin用户(拥有ADMIN_PRIV权限的用户)可以执行以下命令设置黑名单：

~~~sql
ADD SQLBLACKLIST #sql#
DELETE SQLBLACKLIST #sql#
SHOW SQLBLACKLISTS
~~~

* 当 enable\_sql\_blacklist 为true时，每条查询SQL都会和sqlblacklist进行匹配，对于匹配成功的SQL会直接返回错误信息：通报此条SQL处于黑名单之中；匹配失败的SQL会正常执行并输出结果。错误信息示例如下：
`ERROR 1064 (HY000): Access denied; sql 'select count (*) from test_all_type_select_2556' is in blacklist`

## 增加黑名单

~~~sql
ADD SQLBLACKLIST #sql#
~~~

**"sql"**：某类 SQL 的正则表达式。由于 SQL 常用字符里面就包含 `(`、`)`、`*`、`.` 等字符，这些字符会和正则表达式中的语义混淆，因此在设置黑名单的时候需要通过转义符作出区分，鉴于 `(` 和 `)` 在SQL中使用频率过高，我们内部进行了处理，设置的时候不需要转义，其他特殊字符需要使用转义字符"\"作为前缀。

* 禁止count(\*):

    ~~~sql
    ADD SQLBLACKLIST "select count(\\*) from .+"
    ~~~

* 禁止count(distinct ):

    ~~~sql
    ADD SQLBLACKLIST "select count(distinct .+) from .+"
    ~~~

* 禁止order by limit `x, y，1 <= x <=7, 5 <=y <=7`:

    ~~~sql
    ADD SQLBLACKLIST "select id_int from test_all_type_select1 order by id_int limit [1-7], [5-7]"
    ~~~

* 禁止复杂sql，这里主要是展示要转义的写法"*","-":

    ~~~sql
    ADD SQLBLACKLIST "select id_int \\* 4, id_tinyint, id_varchar from test_all_type_nullable except select id_int, id_tinyint, id_varchar from test_basic except select (id_int \\* 9 \\- 8) \\/ 2, id_tinyint, id_varchar from test_all_type_nullable2 except select id_int, id_tinyint, id_varchar from test_basic_nullable"
    ~~~

## 展示黑名单列表

~~~sql
SHOW SQLBLACKLIST
~~~

结果格式：`Index | Forbidden SQL`

比如：

~~~sql
mysql> show sqlblacklist;
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Index | Forbidden SQL                                                                                                                                                                                                                                                                                          |
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 1     | select count\(\*\) from .+                                                                                                                                                                                                                                                                             |
| 2     | select id_int \* 4, id_tinyint, id_varchar from test_all_type_nullable except select id_int, id_tinyint, id_varchar from test_basic except select \(id_int \* 9 \- 8\) \/ 2, id_tinyint, id_varchar from test_all_type_nullable2 except select id_int, id_tinyint, id_varchar from test_basic_nullable |
| 3     | select id_int from test_all_type_select1 order by id_int limit [1-7], [5-7]                                                                                                                                                                                                                            |
| 4     | select count\(distinct .+\) from .+                                                                                                                                                                                                                                                                    |
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

~~~

Forbidden SQL中展示的sql对于所有sql语义的字符做了转义处理。

## 删除黑名单

~~~sql
DELETE SQLBLACKLIST #indexlist#
~~~

比如对`SHOW SQLBLACKLIST`中的sqlblacklist做delete:

~~~sql
delete sqlblacklist  3, 4;   --（#indexlist#是以","分隔的id）
~~~

之后剩下的sqlblacklist为：

~~~sql
mysql> show sqlblacklist;
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Index | Forbidden SQL                                                                                                                                                                                                                                                                                          |
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 1     | select count\(\*\) from .+                                                                                                                                                                                                                                                                             |
| 2     | select id_int \* 4, id_tinyint, id_varchar from test_all_type_nullable except select id_int, id_tinyint, id_varchar from test_basic except select \(id_int \* 9 \- 8\) \/ 2, id_tinyint, id_varchar from test_all_type_nullable2 except select id_int, id_tinyint, id_varchar from test_basic_nullable |
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

~~~

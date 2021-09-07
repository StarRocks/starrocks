# 黑名单管理

管理员需要在某些场景中禁止某些pattern的SQL，避免比如可能触发集群crash的SQL或者客户预期之外的高并发查询。

「查询黑名单」给客户一个SQL黑名单的机制，让他能够自己添加/浏览/删除 sql黑名单。

## 语法

通过 enable_sql_blacklist 开启sql黑名单(默认关闭)：

~~~sql
admin set frontend config ("enable_sql_blacklist" = "true")
~~~

Admin用户(拥有ADMIN_PRIV权限的用户)可以执行以下命令设置黑名单：

~~~sql
ADD SQLBLACKLIST #sql# 
DELETE SQLBLACKLIST #sql# 
SHOW SQLBLACKLISTS  
~~~

* 在 enable\_sql\_blacklist 为true时，每条查询的sql都需用sqlblacklist去过滤。匹配，则通报SQL处于黑名单之中的信息；否则，正常执行并输出结果。处于黑名单之中时，提示信息可能如下：
`ERROR 1064 (HY000): Access denied; sql 'select count (*) from test_all_type_select_2556' is in blacklist`

## 增加黑名单

~~~sql
ADD SQLBLACKLIST #sql#
~~~

**#sql#** 是某类sql的正则表达式，但是由于sql本身包含的常用字符 "(", ")", "*", "."等与正则表达式下的语义会混淆，所以这里需要通过转义符作出区分，鉴于"("和")"在sql中使用频率过高，所以不需要转义字符，其他特殊字符需要使用转义字符"\"作为前缀。例如:

* 禁止count(\*):

    ~~~sql
    ADD SQLBLACKLIST "select count(\\*) from .+"
    ~~~

* 禁止count(distinct ):

    ~~~sql
    ADD SQLBLACKLIST "select count(distinct .+) from .+"
    ~~~

* 禁止order by limit x, y，1 <= x <=7, 5 <=y <=7:

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

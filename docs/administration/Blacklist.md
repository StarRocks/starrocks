# Blacklist Management

In some cases, administrators need to disable certain patterns of SQL to avoid SQL from triggering cluster crashes or unexpected high concurrent queries.

StarRocks allows users to add, view, and delete SQL blacklists.

## Syntax

EnableSQL blacklisting via `enable_sql_blacklist`. The default is False (off).

~~~sql
admin set frontend config ("enable_sql_blacklist" = "true")
~~~

The admin user who has ADMIN_PRIV privileges can manage blacklists by executing the following commands:

~~~sql
ADD SQLBLACKLIST #sql# 
DELETE SQLBLACKLIST #sql# 
SHOW SQLBLACKLISTS  
~~~

* When `enable_sql_blacklist` is true, every SQL query needs to be filtered by sqlblacklist. If it matches, the user will be informed that theSQL is in the blacklist. Otherwise, the SQL will be executed normally. The message may be as follows when the SQL is blacklisted:

`ERROR 1064 (HY000): Access denied; sql 'select count (*) from test_all_type_select_2556' is in blacklist`

## Add blacklist

~~~sql
ADD SQLBLACKLIST #sql#
~~~

**#sql#** is a regular expression for a certain type of SQL. Since SQL itself contains the common characters `(`, `)`, `*`, `.` that may be mixed up with the semantics of regular expressions, so we need to distinguish those by  using escape characters. Given that `(` and `)` are used too often in SQL, there is no need to use escape characters. Other special characters need to use the escape character `\` as a prefix. For example:

* Prohibit `count(\*)`:

~~~sql
ADD SQLBLACKLIST "select count(\\*) from .+"
~~~

* Prohibit `count(distinct)`:

~~~sql
ADD SQLBLACKLIST "select count(distinct .+) from .+"
~~~

<<<<<<< HEAD
* Prohibit order by limit x, y，1 <= x <=7, 5 <=y <=7:
=======
* Prohibit order by limit `x, y, 1 <= x <=7, 5 <=y <=7`:
>>>>>>> 1bdc91c00 ([Doc] Markdown 23 (#33341))

~~~sql
ADD SQLBLACKLIST "select id_int from test_all_type_select1 order by id_int limit [1-7], [5-7]"
~~~

* Prohibit complex SQL:

~~~sql
ADD SQLBLACKLIST "select id_int \\* 4, id_tinyint, id_varchar from test_all_type_nullable except select id_int, id_tinyint, id_varchar from test_basic except select (id_int \\* 9 \\- 8) \\/ 2, id_tinyint, id_varchar from test_all_type_nullable2 except select id_int, id_tinyint, id_varchar from test_basic_nullable"
~~~

## View blacklist

~~~sql
SHOW SQLBLACKLIST
~~~

Result format: `Index | Forbidden SQL`

For example:

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

The SQL shown in `Forbidden SQL` is escaped for all SQL semantic characters.

## Delete blacklist

~~~sql
DELETE SQLBLACKLIST #indexlist#
~~~

For example, delete the sqlblacklist 3 and 4 in the above blacklist:

~~~sql
delete sqlblacklist  3, 4;   --（#indexlist#是以","分隔的id）
~~~

Then, the remaining sqlblacklist is as follows:

~~~sql
mysql> show sqlblacklist;
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Index | Forbidden SQL                                                                                                                                                                                                                                                                                          |
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 1     | select count\(\*\) from .+                                                                                                                                                                                                                                                                             |
| 2     | select id_int \* 4, id_tinyint, id_varchar from test_all_type_nullable except select id_int, id_tinyint, id_varchar from test_basic except select \(id_int \* 9 \- 8\) \/ 2, id_tinyint, id_varchar from test_all_type_nullable2 except select id_int, id_tinyint, id_varchar from test_basic_nullable |
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

~~~

# group_concat

## Description

group_concat concatenates non-null values into one string from a group, with a separator argument, which is ',' by default if not specified.

## Syntax

```SQL
VARCHAR GROUP_CONCAT([DISTINCT] expr [,expr ...]
             [ORDER BY {unsigned_integer | col_name | expr}
                 [ASC | DESC] [,col_name ...]]
             [SEPARATOR str_val])
```

## Parameters

- `expr`: the values to concatenate, ignoring null. It should be cast to VARCHAR. They can be optionally specified `DISTINCT` to eliminate duplicate values. More `expr` are concatenated directly, use `concat()` or `concat_ws` to specify formats.
- order-by items can be unsigned integers (identify `expr`, starting from 1), column names or normal expressions. To sort in reverse order, add the DESC (descending) keyword to the name of the column you are sorting by in the ORDER BY clause. The default is ascending order; this may be specified explicitly using the ASC keyword
- `str_val`: the optional separator is used to concat non-null values from different rows. If it is not specified, `,` (a comma) is used by default.

## Return value

Returns a string value for each group, but returns NULL if there are no non-NULL values.

set `group_concat_max_len` to limit the length of output string from a group, its default value is 1024, minimal value is 4.

## Examples

```sql
CREATE TABLE `ss` (
  `id` int(11) NULL COMMENT "",
  `name` varchar(255) NULL COMMENT "",
  `subject` varchar(255) NULL COMMENT "",
  `score` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 4
PROPERTIES (
"replication_num" = "1"
);

insert into ss values (1,"Tom","English",90);
insert into ss values (1,"Tom","Math",80);
insert into ss values (2,"Tom","English",NULL);
insert into ss values (2,"Tom",NULL,NULL);
insert into ss values (3,"May",NULL,NULL);
insert into ss values (3,"Ti","English",98);
insert into ss values (4,NULL,NULL,NULL);
insert into ss values (NULL,"Ti","Phy",98);
```

```sql
select id, group_concat(distinct name,subject order by score) as res from ss group by id order by id;
+------+--------------------+
| id   | res                |
+------+--------------------+
| NULL | TiPhy              |
|    1 | TomMath,TomEnglish |
|    2 | NULL               |
|    3 | TiEnglish          |
|    4 | NULL               |
+------+--------------------+

mysql> select id, group_concat(distinct concat(name,'-',subject) order by score) as res from ss group by id order by id;
+------+----------------------+
| id   | res                  |
+------+----------------------+
| NULL | Ti-Phy               |
|    1 | Tom-Math,Tom-English |
|    2 | NULL                 |
|    3 | Ti-English           |
|    4 | NULL                 |
+------+----------------------+
    
select group_concat(name) as res from ss;
+---------------------------+
| res                       |
+---------------------------+
| Tom,Tom,Ti,Tom,Tom,May,Ti |
+---------------------------+

select group_concat(distinct name) as res from ss where id < 0;
+------+
| res  |
+------+
| NULL |
+------+
 
set group_concat_max_len = 6;

select id, group_concat(distinct name,subject order by score) as res from ss group by id order by id;
+------+--------+
| id   | res    |
+------+--------+
| NULL | TiPhy  |
|    1 | TomMat |
|    2 | NULL   |
|    3 | TiEngl |
|    4 | NULL   |
+------+--------+
```

## keyword

GROUP_CONCAT,CONCAT,ARRAY_AGG

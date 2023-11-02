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

> **NOTE**
>
> From v3.0.6 and v3.1.3 onwards, there is a behavior change when you specify the separator. You must use `SEPARATOR` to declare the separator, for example, `select group_concat(name SEPARATOR '-') as res from ss;`.

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

<<<<<<< HEAD
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
=======
  ```sql
   select group_concat(name) as res from ss;
   +---------------------------+
   | res                       |
   +---------------------------+
   | Tom,Tom,Ti,Tom,Tom,May,Ti |
   +---------------------------+
  ```

  Example 2: Concatenate names into a string, connected by the separator `-` and with null values ignored. Duplicate names are retained.

  ```sql
   select group_concat(name SEPARATOR '-') as res from ss;
   +---------------------------+
   | res                       |
   +---------------------------+
   | Ti-May-Ti-Tom-Tom-Tom-Tom |
   +---------------------------+
  ```

  Example 3: Concatenate distinct names into a string with the default separator and with null values ignored. Duplicate names are removed.

  ```sql
   select group_concat(distinct name) as res from ss;
   +---------------------------+
   | res                       |
   +---------------------------+
   | Ti,May,Tom                |
   +---------------------------+
  ```

  Example 4: Concatenate the name-subject strings of the same ID in ascending order of `score`. For example, `TomMath` and `TomEnglish` share ID 1 and they are concatenated with a comma in ascending order of `score`.

  ```sql
   select id, group_concat(distinct name,subject order by score) as res from ss group by id order by id;
   +------+--------------------+
   | id   | res                |
   +------+--------------------+
   | NULL | TiPhy              |
   |    1 | TomMath,TomEnglish |
   |    2 | TomEnglish         |
   |    3 | TiEnglish          |
   |    4 | NULL               |
   +------+--------------------+
   ```

  Example 5: group_concat is nested with concat(), which is used to combine `name`, `-`, and `subject` as a string. The strings in the same row are sorted in ascending order of `score`.
  
  ```sql
   select id, group_concat(distinct concat(name, '-',subject) order by score) as res from ss group by id order by id;
   +------+----------------------+
   | id   | res                  |
   +------+----------------------+
   | NULL | Ti-Phy               |
   |    1 | Tom-Math,Tom-English |
   |    2 | Tom-English          |
   |    3 | Ti-English           |
   |    4 | NULL                 |
   +------+----------------------+
   ```
  
  Example 6: No matching result is found and NULL is returned.

  ```sql
  select group_concat(distinct name) as res from ss where id < 0;
   +------+
   | res  |
   +------+
   | NULL |
   +------+
   ```

  Example 7: Limit the length of the returned string to six characters.

  ```sql
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
>>>>>>> d8f3ff6c43 ([Doc] update separator in group_concat and fix mv in grant syntax (#34207))

## keyword

GROUP_CONCAT,CONCAT,ARRAY_AGG

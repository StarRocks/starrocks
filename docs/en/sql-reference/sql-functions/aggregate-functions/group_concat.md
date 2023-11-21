---
displayed_sidebar: "English"
---

# group_concat

## Description

Concatenates non-null values from a group into a single string, with a `sep` argument, which is `,` by default if not specified. This function can be used to concatenate values from multiple rows of a column into one string.

group_concat supports DISTINCT and ORDER BY in 3.0 versions later than 3.0.6 and 3.1 versions later than 3.1.3.

## Syntax

```SQL
VARCHAR GROUP_CONCAT([DISTINCT] expr [,expr ...]
             [ORDER BY {unsigned_integer | col_name | expr}
                 [ASC | DESC] [,col_name ...]]
             [SEPARATOR sep])
```

## Parameters

- `expr`: the values to concatenate, with null values ignored. It must evaluate to VARCHAR. You can optionally specify `DISTINCT` to eliminate duplicate values from the output string. If you want to concatenate multiple `expr` directly, use [concat](../string-functions/concat.md) or [concat_ws](../string-functions/concat_ws.md) to specify formats.
- Items in ORDER BY can be unsigned integers (starting from 1), column names, or normal expressions. The results are sorted in ascending order by default. You can also explicitly specify the ASC keyword. If you want to sort results in descending order, add the DESC keyword to the name of the column you are sorting.
- `sep`: the optional separator used to concatenate non-null values from different rows. If it is not specified, `,` (a comma) is used by default. To eliminate the separator, specify an empty string `''`.

> **NOTE**
>
> From v3.0.6 and v3.1.3 onwards, there is a behavior change when you specify the separator. You must use `SEPARATOR` to declare the separator, for example, `select group_concat(name SEPARATOR '-') as res from ss;`.

## Return value

Returns a string value for each group and returns NULL if there are no non-NULL values.

You can limit the length of the string returned by group_concat by setting the [session variable](../../../reference/System_variable.md) `group_concat_max_len`, which defaults to 1024. Minimum value: 4. Unit: characters.

Example:

```sql
SET [GLOBAL | SESSION] group_concat_max_len = <value>;
```

## Examples

1. Create a table `ss` which contains subject scores.

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

   select * from ss order by id;
   +------+------+---------+-------+
   | id   | name | subject | score |
   +------+------+---------+-------+
   | NULL | Ti   | Phy     |    98 |
   |    1 | Tom  | English |    90 |
   |    1 | Tom  | Math    |    80 |
   |    2 | Tom  | English |  NULL |
   |    2 | Tom  | NULL    |  NULL |
   |    3 | May  | NULL    |  NULL |
   |    3 | Ti   | English |    98 |
   |    4 | NULL | NULL    |  NULL |
   +------+------+---------+-------+
   ```

2. Use group_concat.
  
  Example 1: Concatenate names into a string with the default separator and with null values ignored. Duplicate names are retained.

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

## keyword

GROUP_CONCAT,CONCAT,ARRAY_AGG

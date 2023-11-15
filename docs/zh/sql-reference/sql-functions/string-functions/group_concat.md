# group_concat

## 功能

将分组中的多个非 NULL 值连接成一个字符串，参数 `sep` 为字符串之间的连接符，该参数可选，默认为 `,`。该函数在连接时会忽略 NULL 值。

从 3.0.6，3.1.3 版本开始，group_concat 支持使用 DISTINCT 和 ORDER BY。

## 语法

```Haskell
VARCHAR GROUP_CONCAT([DISTINCT] expr [,expr ...]
             [ORDER BY {unsigned_integer | col_name | expr}
                 [ASC | DESC] [,col_name ...]]
             [SEPARATOR sep])
```

## 参数说明

- `expr`: 待拼接的值，支持的数据类型为 VARCHAR。可以指定 DISTINCT 关键字在连接之前移除分组中的重复值。如果想直接连接多个值，可以使用 [concat](./concat.md) 和 [concat_ws](./concat_ws.md) 来指定连接的方式。
- ORDER BY 后可以跟 unsigned_integer (从 1 开始)、列名、或普通表达式。ORDER BY 用于指定按升序或降序对要连接的值进行排序。默认按升序排序。如果要按降序排序，需要指定 DESC。
- `sep`：字符串之间的连接符，可选。如果不指定，则默认使用逗号 `,` 作为连接符。如果要使用空字符来连接，可以使用 `''`。

> **NOTE**
>
> 从 v3.0.6 和 v3.1.3 版本起，分隔符必须使用 `SEPARATOR` 关键字来声明。 举例，`select group_concat(name SEPARATOR '-') as res from ss;`。

## 返回值说明

返回值的数据类型为 VARCHAR。如果没有非 NULL 值，则返回 NULL。

您可以使用系统变量 [group_concat_max_len](../../../reference/System_variable.md#group_concat_max_len) 来控制可以返回的最大字符长度。默认值：1024。最小值：4。单位：字符。

变量的设置方法：

```sql
SET [GLOBAL | SESSION] group_concat_max_len = <value>
```

## 示例

1. 建表并插入数据。

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

2. 使用 group_concat 对值进行拼接。

   示例一：对 `name` 列的值进行拼接，使用默认连接符，忽略 NULL 值。不对数据进行去重。

   ```plain
   select group_concat(name) as res from ss;
   +---------------------------+
   | res                       |
   +---------------------------+
   | Tom,Tom,Ti,Tom,Tom,May,Ti |
   +---------------------------+
   ```

   示例二：对 `name` 列的值进行拼接，使用 `SEPARATOR` 来声明分隔符 `-`，忽略 NULL 值。不对数据进行去重。

   ```sql
   select group_concat(name SEPARATOR '-') as res from ss;
   +---------------------------+
   | res                       |
   +---------------------------+
   | Ti-May-Ti-Tom-Tom-Tom-Tom |
   +---------------------------+
   ```

   示例三：对 `name` 列的值进行拼接，使用默认连接符，忽略 NULL 值。使用 DISTINCT 对数据进行去重。

    ```plain
   select group_concat(distinct name) as res from ss;
   +---------------------------+
   | res                       |
   +---------------------------+
   | Ti,May,Tom                |
   +---------------------------+
   ```

   示例四：将相同 ID 的 `name` 和 `subject` 组合按照 `score` 的升序进行连接。比如返回示例中的第二行数据 `TomMath,TomEnglish` 就是将 ID 为 1 的 `TomMath` and `TomEnglish` 组合按照 `score` 进行升序排序。

   ```plain
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

   示例五：在 group_concat 中嵌套 concat 函数。concat 指定 `name` 和 `subject` 的连接格式为 "name-subject"。查询逻辑和示例三相同。

   ```plain
   select id, group_concat(distinct concat(name,'-',subject) order by score) as res from ss group by id order by id;
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

   示例六：没有符合条件的结果，返回 NULL。

   ```plain
   select group_concat(distinct name) as res from ss where id < 0;
   +------+
   | res  |
   +------+
   | NULL |
   +------+
   ```

   示例七：将返回字符串的最大长度限制在 6 个字符。查询逻辑和示例三相同。

   ```plain
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

## keywords

GROUP_CONCAT,CONCAT,ARRAY_AGG

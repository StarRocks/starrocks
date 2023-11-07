# group_concat

## Description

This is an aggregate function similar to sum(). group_concat concatenates non-null values into one string, with the second argument `sep` being the separator. The second argument can also be omitted. This function usually needs to be used along with `group by`.

> Please note that strings may not be concatenated in sequence because it uses distributed computing.

## Syntax

```Haskell
VARCHAR group_concat(VARCHAR str[, VARCHAR sep])
```

## Parameters

- `str`: the values to concatenate. It must evaluate to VARCHAR.
- `sep`: the separator, optional. If it is not specified, a comma and a space (`, `) is used by default.

## Return value

Returns a VARCHAR value.

## Examples

```sql
CREATE TABLE IF NOT EXISTS group_concat (
    id        tinyint(4)      NULL,
    value   varchar(65533)  NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(id);

INSERT INTO group_concat VALUES
(1,'fruit'),
(2,'drinks'),
(3,null),
(4,'fruit'),
(5,'meat'),
(6,'seafood');

select * from group_concat order by id;
+------+---------+
| id   | value   |
+------+---------+
|    1 | fruit   |
|    2 | drinks  |
|    3 | NULL    |
|    4 | fruit   |
|    5 | meat    |
|    6 | seafood |
```

```sql
select group_concat(value) from group_concat;
+-------------------------------------+
| group_concat(value)                 |
+-------------------------------------+
| meat, fruit, seafood, fruit, drinks |
+-------------------------------------+

MySQL > select group_concat(value, " ") from group_concat;
+---------------------------------+
| group_concat(value, ' ')        |
+---------------------------------+
| fruit meat fruit drinks seafood |
+---------------------------------+
```

## keyword

GROUP_CONCAT,GROUP,CONCAT

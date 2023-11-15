# group_concat

## description

### Syntax

`VARCHAR group_concat(VARCHAR str[, VARCHAR sep])`

This is an aggregate function similar to sum(). Group_concat concatenates the data from multiple rows into one string, with the second argument sep being the separator. The second argument can also be omitted. This function usually needs to be used along with group by statement.

> Please note that the multiple data may not be concatenated in sequence because it uses distributed computing.

## example

```Plain Text
MySQL > select value from test;
+-------+
| value |
+-------+
| a     |
| b     |
| c     |
+-------+

MySQL > select group_concat(value) from test;
+-----------------------+
| group_concat(`value`) |
+-----------------------+
| a, b, c               |
+-----------------------+

MySQL > select group_concat(value, " ") from test;
+----------------------------+
| group_concat(`value`, ' ') |
+----------------------------+
| a b c                      |
+----------------------------+
```

## keyword

GROUP_CONCAT,GROUP,CONCAT

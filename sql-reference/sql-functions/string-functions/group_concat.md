# group_concat

## 功能

该函数是类似于 sum() 的聚合函数，group_concat 将结果集中的多行结果连接成一个字符串，第二个参数 sep 为字符串之间的连接符号，该参数可以省略。

> 注: 该函数通常需要和 group by 语句一起使用，由于是分布式计算，不能保证 "多行数据是「有序」拼接的"。

## 语法

`group_concat(str[, VARCHAR sep])`

## 参数说明

`str`: 支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

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

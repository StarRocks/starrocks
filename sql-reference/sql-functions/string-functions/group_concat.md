# group_concat

## 功能

该函数是类似于 sum() 的聚合函数, group_concat 将结果集中的多行结果连接成一个字符串, 第二个参数 sep 为字符串之间的连接符号, 该参数可以省略

> 注: 由于是分布式计算，该函数**不能保证多行数据是「有序」拼接的**。该函数通常需要和 GROUP BY 语句一起使用。

## 语法

```Haskell
group_concat(str[, VARCHAR sep])
```

## 参数说明

- `str`: 待拼接的一列值。支持的数据类型为 VARCHAR。
- `sep`: 字符串之间的连接符，可选。如果未指定，默认使用逗号加一个空格 (`, `) 作为连接符。

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

## 关键词

GROUP_CONCAT, GROUP, CONCAT

---
displayed_sidebar: "Chinese"
---

# group_concat

## 功能

group_concat 将结果集中的多行结果连接成一个字符串，第二个参数 `sep` 为字符串之间的连接符号，该参数可选。该函数会忽略 null 值。

> 注: 由于是分布式计算，该函数**不能保证多行数据是「有序」拼接的**。该函数通常需要和 GROUP BY 语句一起使用。

## 语法

```Haskell
group_concat(VARCHAR str[, VARCHAR sep])
```

## 参数说明

- `str`: 待拼接的一列值。支持的数据类型为 VARCHAR。
- `sep`: 字符串之间的连接符，可选。如果未指定，默认使用逗号（`,`）加一个空格作为连接符。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

建表并插入数据。

```Plain Text
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

使用 group_concat 对 value 列中的值进行拼接。

```Plain Text
-- 不指定连接符，未有序拼接。
select group_concat(value) from group_concat;
+-------------------------------------+
| group_concat(value)                 |
+-------------------------------------+
| meat, fruit, seafood, fruit, drinks |
+-------------------------------------+

-- 指定空格作为连接符。
MySQL > select group_concat(value, " ") from group_concat;
+---------------------------------+
| group_concat(value, ' ')        |
+---------------------------------+
| fruit meat fruit drinks seafood |
+---------------------------------+
```

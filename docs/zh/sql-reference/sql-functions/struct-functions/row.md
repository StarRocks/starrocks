---
displayed_sidebar: "Chinese"
---

# row

## 功能

根据给定的一个或多个值来构建 STRUCT。该函数支持 unnamed struct，使用该函数时无需指定字段名，StarRocks 会自动生成字段名，例如 `col1, col2,...`。

该函数从 3.1 版本开始支持。struct() 是 row() 的别名。

## 语法

```Haskell
STRUCT row(ANY val, ...)
```

## 参数说明

`val`: 支持任意类型。必须至少传入一个参数。取值可以为 NULL。多个取值以逗号分隔。

## 返回值说明

返回一个包含所有输入值的 STRUCT。

## 示例

```Plaintext
select row(1,"Apple","Pear");
+-----------------------------------------+
| row(1, 'Apple', 'Pear')                 |
+-----------------------------------------+
| {"col1":1,"col2":"Apple","col3":"Pear"} |
+-----------------------------------------+

select row("Apple", NULL);
+------------------------------+
| row('Apple', NULL)           |
+------------------------------+
| {"col1":"Apple","col2":null} |
+------------------------------+

select struct(1,2,3);
+------------------------------+
| row(1, 2, 3)                 |
+------------------------------+
| {"col1":1,"col2":2,"col3":3} |
+------------------------------+
```

## 相关文档

- [STRUCT 数据类型](../../sql-statements/data-types/STRUCT.md)
- [named_struct](named_struct.md)

---
displayed_sidebar: "Chinese"
---

# named_struct

## 功能

根据给定的字段名和字段值来构建 STRUCT。该参数支持 named struct，使用该函数时需要指定字段名称。

该函数从 3.1 版本开始支持。

## 语法

```Haskell
STRUCT named_struct({STRING name1, ANY val1} [, ...] )
```

## 参数说明

- `nameN`: STRING 类型字段。

- `valN`: 任意类型的值，可以为 NULL。

name 和 value 表达式必须成对出现，否则创建失败。必须输入至少一对 name 和 value，以逗号分隔。

## 返回值说明

返回一个 STRUCT。

## 示例

```plain
SELECT named_struct('a', 1, 'b', 2, 'c', 3);
+--------------------------------------+
| named_struct('a', 1, 'b', 2, 'c', 3) |
+--------------------------------------+
| {"a":1,"b":2,"c":3}                  |
+--------------------------------------+

SELECT named_struct('a', null, 'b', 2, 'c', 3);
+-----------------------------------------+
| named_struct('a', null, 'b', 2, 'c', 3) |
+-----------------------------------------+
| {"a":null,"b":2,"c":3}                  |
+-----------------------------------------+
```

## 相关文档

- [STRUCT data type](../../sql-statements/data-types/STRUCT.md)
- [row/struct](row.md)

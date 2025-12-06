---
displayed_sidebar: docs
---

# is_json_scalar

返回 JSON 值是否为标量（不是对象或数组）。

## 别名
无

## 语法

```Haskell
BOOLEAN is_json_scalar(JSON)
```

### 参数

`json` : JSON 类型的值。该函数检查解析后的 JSON 值并确定其是否为标量。有效的 JSON 标量类型包括 number、string、boolean 和 JSON null。如果输入为 SQL NULL（或 JSON 列值为 NULL），函数返回 SQL NULL。

## 返回值

返回一个 BOOLEAN：
- TRUE (1) 如果 JSON 值是标量（number、string、boolean 或 JSON null）。
- FALSE (0) 如果 JSON 值是非标量（对象或数组）。
- NULL 如果输入为 SQL NULL。

## 使用说明

- 该函数对 JSON 类型的值进行操作。传入 JSON 表达式或将字符串字面量 CAST 为 JSON，例如：CAST('{"a": 1}' AS JSON)。
- JSON null（字面量 JSON 值 null）被此函数视为标量，因为它不是对象或数组。
- SQL NULL（值的缺失）不同于 JSON null；SQL NULL 会导致函数返回 NULL。
- 该函数仅检查顶层 JSON 值：无论其内容如何，对象和数组均被视为非标量。
- 此函数的实现为 JsonFunctions::is_json_scalar，它检查底层的 VPack slice 不是对象且不是数组。

## 示例

```Plain
mysql> SELECT is_json_scalar(CAST('{"a": 1}' AS JSON));
+----------------------------------------------+
| is_json_scalar(CAST('{"a": 1}' AS JSON))     |
+----------------------------------------------+
| 0                                            |
+----------------------------------------------+
```

```Plain
mysql> SELECT is_json_scalar(CAST('[1, 2, 3]' AS JSON));
+-----------------------------------------+
| is_json_scalar(CAST('[1, 2, 3]' AS JSON)) |
+-----------------------------------------+
| 0                                       |
+-----------------------------------------+
```

```Plain
mysql> SELECT is_json_scalar(CAST('"hello"' AS JSON));
+-------------------------------------------+
| is_json_scalar(CAST('"hello"' AS JSON))   |
+-------------------------------------------+
| 1                                         |
+-------------------------------------------+
```

```Plain
mysql> SELECT is_json_scalar(CAST('123' AS JSON));
+----------------------------------------+
| is_json_scalar(CAST('123' AS JSON))    |
+----------------------------------------+
| 1                                      |
+----------------------------------------+
```

```Plain
mysql> SELECT is_json_scalar(CAST('true' AS JSON));
+-----------------------------------------+
| is_json_scalar(CAST('true' AS JSON))    |
+-----------------------------------------+
| 1                                       |
+-----------------------------------------+
```

```Plain
mysql> SELECT is_json_scalar(CAST('null' AS JSON));
+----------------------------------------+
| is_json_scalar(CAST('null' AS JSON))   |
+----------------------------------------+
| 1                                      |
+----------------------------------------+
```

```Plain
mysql> SELECT is_json_scalar(CAST(NULL AS JSON));
+------------------------------+
| is_json_scalar(CAST(NULL AS JSON)) |
+------------------------------+
| NULL                         |
+------------------------------+
```

## 关键字
IS_JSON_SCALAR, 无
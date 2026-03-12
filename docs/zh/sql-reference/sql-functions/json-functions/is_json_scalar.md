---
displayed_sidebar: docs
---

# `is_json_scalar`

返回一个 JSON 值是否为标量（不是对象或数组）。

## 别名
无

## 语法

```Haskell
BOOLEAN is_json_scalar(JSON)
```

### 参数

`json` : JSON 类型的值。该函数检查已解析的 JSON 值并确定它是否为标量。有效的 JSON 标量类型包括数字、字符串、布尔值和 JSON null。如果输入是 SQL NULL（或 JSON 列值为 NULL），则函数返回 SQL NULL。

## 返回值

返回一个 BOOLEAN 值：
- 如果 JSON 值为标量（数字、字符串、布尔值或 JSON null），则返回 TRUE (1)。
- 如果 JSON 值为非标量（对象或数组），则返回 FALSE (0)。
- 如果输入是 SQL NULL，则返回 NULL。

## 使用说明

- 该函数对 JSON 类型的值进行操作。传入 JSON 表达式或使用 `CAST` 将字符串字面量转换为 JSON：例如，`CAST('{"a": 1}' AS JSON)`。
- `JSON null`（JSON 字面量 null）被此函数视为标量，因为它既不是对象也不是数组。
- `SQL NULL`（值的缺失）与 `JSON null` 不同；`SQL NULL` 会产生 NULL 结果。
- 该函数仅检查顶层 JSON 值：对象和数组是非标量，无论其内容如何。
- 此函数实现为 `JsonFunctions::is_json_scalar`，它检查底层的 VPack slice 既不是对象也不是数组。

## 示例

```SQL
SELECT is_json_scalar(CAST('{"a": 1}' AS JSON));
+----------------------------------------------+
| is_json_scalar(CAST('{"a": 1}' AS JSON))     |
+----------------------------------------------+
| 0                                            |
+----------------------------------------------+
```

```SQL
SELECT is_json_scalar(CAST('[1, 2, 3]' AS JSON));
+-----------------------------------------+
| is_json_scalar(CAST('[1, 2, 3]' AS JSON)) |
+-----------------------------------------+
| 0                                       |
+-----------------------------------------+
```

```SQL
SELECT is_json_scalar(CAST('"hello"' AS JSON));
+-------------------------------------------+
| is_json_scalar(CAST('"hello"' AS JSON))   |
+-------------------------------------------+
| 1                                         |
+-------------------------------------------+
```

```SQL
SELECT is_json_scalar(CAST('123' AS JSON));
+----------------------------------------+
| is_json_scalar(CAST('123' AS JSON))    |
+----------------------------------------+
| 1                                      |
+----------------------------------------+
```

```SQL
SELECT is_json_scalar(CAST('true' AS JSON));
+-----------------------------------------+
| is_json_scalar(CAST('true' AS JSON))    |
+-----------------------------------------+
| 1                                       |
+-----------------------------------------+
```

```SQL
SELECT is_json_scalar(CAST('null' AS JSON));
+----------------------------------------+
| is_json_scalar(CAST('null' AS JSON))   |
+----------------------------------------+
| 1                                      |
+----------------------------------------+
```

```SQL
SELECT is_json_scalar(CAST(NULL AS JSON));
+------------------------------+
| is_json_scalar(CAST(NULL AS JSON)) |
+------------------------------+
| NULL                         |
+------------------------------+
```

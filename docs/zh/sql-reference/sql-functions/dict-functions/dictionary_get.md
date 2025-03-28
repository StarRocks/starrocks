---
displayed_sidebar: docs
---

# dictionary_get



查询字典对象中 key 映射的 value。

## 语法

```SQL
dictionary_get('dictionary_object_name', key_expression_list, [NULL_IF_NOT_EXIST])

key_expression_list ::=
    key_expression [, ...]

key_expression ::=
    column_name | const_value
```

## 参数说明

- `dictionary_name`：字典对象名称。
- `key_expression_list`：所有 key 列的表达式列表，可以为列名列表或者值列表。
- `NULL_IF_NOT_EXIST`（选填）：当字段缓存中不存在该 key 时，是否返回 NULL。
  - `true`：Key 不存在时 返回 NULL。
  - `false` (默认)：Key 不存在时返回错误。

## 返回值说明

以 STRUCT 类型返回 value 列值。因此可以使用 `[N]` 或者 `.<column_name>` 来指定返回特定列的值。`N` 表示列的位置，起始位置从 1 开始。

## 示例

以下示例使用 [dict_mapping](dict_mapping.md) 示例中的数据集。

**示例一**：查询字典对象 `dict_obj` 中 key 列 `order_uuid` 所映射的 value 列值。

```Plain
MySQL > SELECT dictionary_get('dict_obj', order_uuid) FROM dict;
+--------------------+
| DICTIONARY_GET     |
+--------------------+
| {"order_id_int":1} |
| {"order_id_int":3} |
| {"order_id_int":2} |
+--------------------+
3 rows in set (0.02 sec)
```

**示例二**：查询字典对象 `dict_obj` 中 key `a1` 所映射的 value 列值。

```Plain
MySQL > SELECT dictionary_get("dict_obj", "a1");
+--------------------+
| DICTIONARY_GET     |
+--------------------+
| {"order_id_int":1} |
+--------------------+
1 row in set (0.01 sec)
```

**示例三**：查询字典对象 `dimension_obj` 中 key `1` 所映射的 value 列值。

```Plain
MySQL > SELECT dictionary_get("dimension_obj", 1);
+-----------------------------------------------------------------------------------------------------------------+
| DICTIONARY_GET                                                                                                  |
+-----------------------------------------------------------------------------------------------------------------+
| {"ProductName":"T-Shirt","Category":"Apparel","SubCategory":"Shirts","Brand":"BrandA","Color":"Red","Size":"M"} |
+-----------------------------------------------------------------------------------------------------------------+
1 row in set (0.01 sec)
```

**示例四**：查询字典对象 `dimension_obj` 中 key `1` 所映射的第一个 value 列值。

```Plain
MySQL > SELECT dictionary_get("dimension_obj", 1)[1];
+-------------------+
| DICTIONARY_GET[1] |
+-------------------+
| T-Shirt           |
+-------------------+
1 row in set (0.01 sec)
```

**示例五**：查询字典对象 `dimension_obj` 中 key `1` 所映射的第二个 value 列值。

```Plain
MySQL > SELECT dictionary_get("dimension_obj", 1)[2];
+-------------------+
| DICTIONARY_GET[2] |
+-------------------+
| Apparel           |
+-------------------+
1 row in set (0.01 sec)
```

**示例六**：查询字典对象 `dimension_obj` 中 key `1` 所映射的 value 列 `ProductName` 的值。

```Plain
MySQL > SELECT dictionary_get("dimension_obj", 1).ProductName;
+----------------------------+
| DICTIONARY_GET.ProductName |
+----------------------------+
| T-Shirt                    |
+----------------------------+
1 row in set (0.01 sec)
```
---
displayed_sidebar: "Chinese"
---

# map_keys

## 功能

返回 Map 中所有 key 组成的数组。MAP 里保存的是键值对 (key-value pair)，比如 `{"a":1, "b":2}`。

该函数从 2.5 版本开始支持。

## 语法

```Haskell
ARRAY map_keys(any_map)
```

## 参数说明

`any_map`:  要获取 Keys 的 MAP 值。

## 返回值说明

返回 ARRAY 类型的数组，格式为 `array<keyType>`。`keyType` 的数据类型和 MAP 值里的 key 类型相同。

如果输入参数是 NULL，结果也是 NULL。如果 MAP 中的某个 Key 或 Value 是 NULL，该 NULL 值正常返回。

## 示例

### 查询 StarRocks 本地表中的 MAP 数据

3.1 版本支持在建表时定义 MAP 类型的列，以创建表 `test_map` 为例。

```SQL
CREATE TABLE test_map(
    col_int INT,
    col_map MAP<VARCHAR(50),INT>
  )
DUPLICATE KEY(col_int);

INSERT INTO test_map VALUES
(1,map{"a":1,"b":2}),
(2,map{"c":3}),
(3,map{"d":4,"e":5});

SELECT * FROM test_map ORDER BY col_int;
+---------+---------------+
| col_int | col_map       |
+---------+---------------+
|       1 | {"a":1,"b":2} |
|       2 | {"c":3}       |
|       3 | {"d":4,"e":5} |
+---------+---------------+
```

获取 `col_map` 列每行的所有 Keys。

```Plain
SELECT map_keys(col_map) FROM test_map ORDER BY col_int;
+-------------------+
| map_keys(col_map) |
+-------------------+
| ["a","b"]         |
| ["c"]             |
| ["d","e"]         |
+-------------------+
```

### 查询外部数据湖中的 MAP 数据

假设 Hive 中有表 `hive_map`，数据如下：

```Plain
select * from hive_map order by col_int;
+---------+---------------+
| col_int | col_map       |
+---------+---------------+
|       1 | {"a":1,"b":2} |
|       2 | {"c":3}       |
|       3 | {"d":4,"e":5} |
+---------+---------------+
3 rows in set (0.05 sec)
```

通过在 StarRocks 集群中[创建 Hive catalog](../../../data_source/catalog/hive_catalog.md#创建-hive-catalog)来访问该表，获取 `col_map` 列每行的所有 Keys。

```Plain
select map_keys(col_map) from hive_map order by col_int;
+-------------------+
| map_keys(col_map) |
+-------------------+
| ["a","b"]         |
| ["c"]             |
| ["d","e"]         |
+-------------------+
3 rows in set (0.05 sec)
```

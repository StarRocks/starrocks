---
displayed_sidebar: docs
---

# isnotnull



判断输入值是否为非 `NULL`。如果不是 `NULL`，返回 1。如果是 `NULL`，返回 0。

## 语法

```Haskell
ISNOTNULL(v)
```

## 参数说明

- `v`: 要判断的值。支持所有数据类型。

## 返回值说明

如果 `v` 不是 `NULL`，返回 1。如果 `v` 是 `NULL`，返回 0。

## 示例

```plain text
MYSQL > SELECT c1, isnotnull(c1) FROM t1;
+------+--------------+
| c1   | `c1` IS NULL |
+------+--------------+
| NULL |            0 |
|    1 |            1 |
+------+--------------+
```

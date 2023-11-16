---
displayed_sidebar: "Chinese"
---

# multiply

## 功能

计算参数 `arg1` 与 `arg2` 的乘积。

## 语法

```Haskell
MULTIPLY(arg1, arg2);
```

## 参数说明

- `arg1`: 支持的数据类型为数值列类型或字面值。
- `arg2`: 支持的数据类型为数值列类型或字面值。

## 返回值说明

返回值的数据类型为取决于 `arg1` 和 `arg2`。

## 示例

```Plain Text
MySQL > select multiply(10,2);
+-----------------+
| multiply(10, 2) |
+-----------------+
|              20 |
+-----------------+
1 row in set (0.01 sec)

MySQL > select multiply(1,2.1);
+------------------+
| multiply(1, 2.1) |
+------------------+
|              2.1 |
+------------------+
1 row in set (0.01 sec)

-- 对表内的数据进行 multiply 计算。
MySQL > select * from t;
+------+------+------+------+
| id   | name | job1 | job2 |
+------+------+------+------+
|    2 |    2 |    2 |    2 |
+------+------+------+------+
1 row in set (0.08 sec)

MySQL > select multiply(1.0,id) from t;
+-------------------+
| multiply(1.0, id) |
+-------------------+
|                 2 |
+-------------------+
1 row in set (0.01 sec)
```

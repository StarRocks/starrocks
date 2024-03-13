---
displayed_sidebar: "Chinese"
---

# if

## 功能

若参数 `expr1` 成立，返回结果 `expr2`，否则返回结果 `expr3`。

## 语法

```Haskell
if(expr1,expr2,expr3);
```

## 参数说明

`expr1`: 支持的数据类型为 BOOLEAN。

`expr2` 和 `expr3` 必须在数据类型上能够兼容，否则返回报错。

## 返回值说明

返回值的数据类型与 `expr2` 类型一致。

## 示例

```Plain Text
mysql> select if(false,1,2);
+-----------------+
| if(FALSE, 1, 2) |
+-----------------+
|               2 |
+-----------------+

mysql> select if(false,2.14,2);
+--------------------+
| if(FALSE, 2.14, 2) |
+--------------------+
|               2.00 |
+--------------------+
```

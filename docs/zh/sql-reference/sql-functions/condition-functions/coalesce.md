---
displayed_sidebar: "Chinese"
---

# coalesce

## 功能

从左向右返回参数中的第一个非 NULL 表达式。

## 语法

```Haskell
coalesce(expr1,...);
```

## 参数说明

`expr1`: 表达式必须在数据类型上能够兼容，否则返回报错。

## 返回值说明

返回值的数据类型与 `expr1` 类型一致。

## 示例

```Plain Text
mysql> select coalesce(3,NULL,1,1);
+-------------------------+
| coalesce(3, NULL, 1, 1) |
+-------------------------+
|                       3 |
+-------------------------+

mysql> select coalesce(NULL,2,1,1);
+-------------------------+
| coalesce(NULL, 2, 1, 1) |
+-------------------------+
|                       2 |
+-------------------------+
```

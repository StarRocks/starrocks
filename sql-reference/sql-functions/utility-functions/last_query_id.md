# last_query_id

## 功能

获取最后一条 sql 的 queryid

## 语法

```Haskell
last_query_id();
```

## 参数说明

无

## 返回值说明

返回值的数据类型为 VARCHAR

## 示例

```Plain Text
mysql> select last_query_id();
+--------------------------------------+
| last_query_id()                      |
+--------------------------------------+
| 7c1d8d68-bbec-11ec-af65-00163e1e238f |
+--------------------------------------+
1 row in set (0.00 sec)
```

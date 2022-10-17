# host_name

## 功能

获取计算所在节点的主机名。

## 语法

```Haskell
host_name();

```

## 参数说明

无

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
mysql> select host_name();
+-------------+
| host_name() |
+-------------+
| sandbox-sql |
+-------------+
1 row in set (0.01 sec)
```

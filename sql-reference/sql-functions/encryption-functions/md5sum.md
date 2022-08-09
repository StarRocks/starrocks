# md5sum

## 功能

计算多个参数 MD5 128-bit。

## 语法

```Haskell
md5sum(expr,...);
```

## 参数说明

`expr`: 支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
mysql> select md5sum("starrocks");
+----------------------------------+
| md5sum('starrocks')              |
+----------------------------------+
| f75523a916caf65f1ad487a9f8017f75 |
+----------------------------------+
1 row in set (0.01 sec)

mysql> select md5sum("starrocks","star");
+----------------------------------+
| md5sum('starrocks', 'star')      |
+----------------------------------+
| 7af4bfe35b8df2786ad133c57cb2aed8 |
+----------------------------------+
1 row in set (0.01 sec)
```

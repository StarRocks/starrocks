# version

## 功能

主要是为了兼容 mysql 的协议，该函数返回 mysql 数据库的当前版本

## 语法

```Haskell
version();
```

## 参数说明

无

## 返回值说明

返回值的数据类型为 VARCHAR

## 示例

```Plain Text
mysql> select version();
+-----------+
| version() |
+-----------+
| 5.1.0     |
+-----------+
1 row in set (0.00 sec)
```

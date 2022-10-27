# sleep

## 功能

使数据库休眠 `x` 秒

## 语法

```Haskell
sleep(x);
```

## 参数说明

`x`: 支持的数据类型为 INT

## 返回值说明

返回值的数据类型为 BOOLEAN

## 示例

```Plain Text
mysql> select sleep(3);
+----------+
| sleep(3) |
+----------+
|        1 |
+----------+
1 row in set (3.00 sec)
```

## 关键词

SLEEP

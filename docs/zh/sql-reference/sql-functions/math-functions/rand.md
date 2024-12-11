---
displayed_sidebar: docs
---

# rand, random

<<<<<<< HEAD
## 功能

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
返回一个 0 (包含) 到 1（不包含）之间的随机浮点数。

## 语法

```Haskell
RAND(x);
```

## 参数说明

<<<<<<< HEAD
`x`: 可选。支持的数据类型为 BIGINT。如果指定了 `x`，则返回一个可重复的随机数。如果没有指定 `x`，则返回一个完全随机数。
=======
`x`: 可选。支持的数据类型为 BIGINT。无论是否指定 `x`，则都会返回一个完全随机数。
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

## 返回值说明

返回值的数据类型为 DOUBLE。

## 示例

```Plain Text
mysql> select rand();
+--------------------+
| rand()             |
+--------------------+
| 0.9393535880089522 |
+--------------------+
1 row in set (0.01 sec)

mysql> select rand(3);
+--------------------+
| rand(3)            |
+--------------------+
| 0.6659865964511347 |
+--------------------+
1 row in set (0.00 sec)
```

## Keyword

RAND, RANDOM

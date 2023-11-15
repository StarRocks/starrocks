# sleep

## 功能

将当前正在执行的线程休眠 `x` 秒。返回 BOOLEAN 类型的值，`1` 表示正常休眠，`0` 表示休眠失败。

## 语法

```Haskell
BOOLEAN sleep(INT x);
```

## 参数说明

`x`: 支持的数据类型为 INT。

## 返回值说明

返回 BOOLEAN 类型的值。

## 示例

```Plain Text
select sleep(3);
+----------+
| sleep(3) |
+----------+
|        1 |
+----------+
1 row in set (3.00 sec)

select sleep(NULL);
+-------------+
| sleep(NULL) |
+-------------+
|        NULL |
+-------------+
1 row in set (0.00 sec)
```

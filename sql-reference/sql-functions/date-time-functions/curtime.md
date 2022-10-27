# curtime,current_time

## 功能

获取当前的时间，以 TIME 类型返回。

该函数受时区影响，具体参见 [设置时区](../../../using_starrocks/timezone.md)。

## 语法

```Haskell
TIME CURTIME()
```

## Examples

```Plain Text
select current_time();
+----------------+
| current_time() |
+----------------+
| 15:25:47       |
+----------------+
```

## keyword

CURTIME,CURRENT_TIME

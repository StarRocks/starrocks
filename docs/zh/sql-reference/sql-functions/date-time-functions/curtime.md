# curtime,current_time

## 功能

获取当前的时间，以 TIME 类型返回。

该函数受时区影响，具体参见 [设置时区](../../../administration/timezone.md)。

## 语法

```Haskell
TIME CURTIME()
```

## 示例

```Plain Text
select current_time();
+----------------+
| current_time() |
+----------------+
| 15:25:47       |
+----------------+
```

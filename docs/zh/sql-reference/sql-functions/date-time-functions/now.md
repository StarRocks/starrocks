---
displayed_sidebar: "Chinese"
---

# now, current_timestamp, localtime, localtimestamp

## 功能

获取当前的时间，以 DATETIME 类型返回。从 3.1 版本开始，该函数的返回结果会到微秒级别。

该函数受时区影响，具体参见 [设置时区](../../../administration/timezone.md)。

## 语法

```Haskell
DATETIME NOW()
```

## 示例

```Plain Text
select now();
+---------------------+
| now()               |
+---------------------+
| 2022-10-09 21:19:35 |
+---------------------+

-- 3.1 版本后的返回结果到微秒级别。

MySQL > select now();
+----------------------------+
| now()                      |
+----------------------------+
| 2023-11-18 12:54:34.878000 |
+----------------------------+
```

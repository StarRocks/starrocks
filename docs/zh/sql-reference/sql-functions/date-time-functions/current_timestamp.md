---
displayed_sidebar: "Chinese"
---

# current_timestamp

## 功能

获取当前时间，以 DATETIME 类型返回。从 3.1 版本开始，该函数的返回结果会到微秒级别。

## 语法

```Haskell
DATETIME CURRENT_TIMESTAMP()
```

## 示例

```Plain Text
select current_timestamp();
+---------------------+
| current_timestamp() |
+---------------------+
| 2019-05-27 15:59:33 |
+---------------------+

-- 3.1 版本后的返回结果到微秒级别。
select current_timestamp();
+----------------------------+
| current_timestamp()        |
+----------------------------+
| 2023-11-18 12:58:05.375000 |
+----------------------------+
```

---
displayed_sidebar: docs
---

# curtime,current_time

<<<<<<< HEAD
## 功能
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

获取当前的时间，以 TIME 类型返回。

该函数受时区影响，具体参见 [设置时区](../../../administration/management/timezone.md)。

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

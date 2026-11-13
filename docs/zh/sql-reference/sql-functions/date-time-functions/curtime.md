---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "获取当前的时间，以 TIME 类型返回。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# curtime,current_time



获取当前的时间，以 TIME 类型返回。

该函数受时区影响，具体参见 [设置时区](../../../administration/management/timezone.md)。

## 语法

```plaintext
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

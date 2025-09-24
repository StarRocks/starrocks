---
displayed_sidebar: docs
---

# current_timezone



获取当前时区，以 VARCHAR 类型返回。

## 语法

```Haskell
VARCHAR CURRENT_TIMEZONE()
```

## 示例

```Plain Text
MySQL > select current_timezone();
+---------------------+
| current_timezone()  |
+---------------------+
| America/Los_Angeles |
+---------------------+
```
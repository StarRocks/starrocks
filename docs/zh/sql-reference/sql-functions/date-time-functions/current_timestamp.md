---
displayed_sidebar: docs
---

# current_timestamp

## 功能

获取当前时间，以 DATETIME 类型返回。

该函数和 [now](./now.md) 功能相同。

## 语法

```Haskell
DATETIME CURRENT_TIMESTAMP()
```

## 示例

示例一：返回当前时间。

```Plain Text
select current_timestamp();
+---------------------+
| current_timestamp() |
+---------------------+
| 2019-05-27 15:59:33 |
+---------------------+
```

示例二：建表时，可以给某列使用该函数，将当前时间作为该列的默认值。

```SQL
CREATE TABLE IF NOT EXISTS sr_member (
    sr_id            INT,
    name             STRING,
    city_code        INT,
    reg_date         DATETIME DEFAULT current_timestamp,
    verified         BOOLEAN
);
```

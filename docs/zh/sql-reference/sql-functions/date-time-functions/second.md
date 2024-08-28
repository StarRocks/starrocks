---
displayed_sidebar: docs
---

# second

## 功能

获得日期中的秒的信息，返回值范围 0~59。

参数为 DATE 或 DATETIME 类型。

### Syntax

```Haskell
INT SECOND(DATETIME date)
```

## 示例

```Plain Text
select second('2018-12-31 23:59:59');
+-----------------------------+
|second('2018-12-31 23:59:59')|
+-----------------------------+
|                          59 |
+-----------------------------+
```

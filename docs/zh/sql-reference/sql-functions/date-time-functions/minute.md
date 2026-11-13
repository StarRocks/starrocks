---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "返回指定日期中的分钟部分，取值范围 0 到 59。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# minute



获得日期中的分钟的信息，返回值范围为 0~59。

参数为 DATE 或者 DATETIME 类型。

## 语法

```plaintext
INT MINUTE(DSATETIME date)
```

## 示例

```Plain Text
select minute('2018-12-31 23:59:59');
+-----------------------------+
|minute('2018-12-31 23:59:59')|
+-----------------------------+
|                          59 |
+-----------------------------+
```

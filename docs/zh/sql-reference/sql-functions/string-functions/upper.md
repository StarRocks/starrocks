---
displayed_sidebar: "Chinese"
---

# UPPER

## 功能

将字符串转换为大写形式。

## 语法

```haskell
upper(str)
```

## 参数说明

- `str`：需要转换的字符串。如果 `str` 不是字符串类型，会先尝试进行隐式的类型转换，将其转换成字符串后再执行该函数。

## 返回值说明

返回大写的字符串。

## 示例

```plaintext
MySQL [test]> select C_String, upper(C_String) from ex_iceberg_tbl;
+---------------+-----------------+
| C_String      | upper(C_String) |
+---------------+-----------------+
| Hello, China! | HELLO, CHINA!   |
| Hello, World! | HELLO, WORLD!   |
+---------------+-----------------+
```

---
displayed_sidebar: "Chinese"
---

# url_encode

## 功能

将字符串根据 [application/x-www-form-urlencoded](https://www.w3.org/TR/html4/interact/forms.html#h-17.13.4.1) 格式进行编码。

该函数从 v3.2 版本开始支持。

## 语法

```haskell
VARCHAR url_encode(VARCHAR str)
```

## 参数说明

- `str`: 要编码的字符串。如果 `str` 不是字符串格式，会尝试隐式转换。

## 返回值说明

返回一个符合 [application/x-www-form-urlencoded](https://www.w3.org/TR/html4/interact/forms.html#h-17.13.4.1) 格式的编码字符串。

## Examples

```plaintext
mysql> select url_encode('https://docs.starrocks.io/docs/introduction/StarRocks_intro/');
+----------------------------------------------------------------------------+
| url_encode('https://docs.starrocks.io/docs/introduction/StarRocks_intro/') |
+----------------------------------------------------------------------------+
| https%3A%2F%2Fdocs.starrocks.io%2Fdocs%2Fintroduction%2FStarRocks_intro%2F |
+----------------------------------------------------------------------------+
```

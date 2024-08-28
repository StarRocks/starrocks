---
displayed_sidebar: docs
---

# url_decode

## 功能

将字符串从 [application/x-www-form-urlencoded](https://www.w3.org/TR/html4/interact/forms.html#h-17.13.4.1) 格式转换回来。相反函数是 [url_encode](./url_encode.md)。

该函数从 v3.2 版本开始支持。

## 语法

```haskell
VARCHAR url_decode(VARCHAR str)
```

## 参数说明

- `str`：待解码的字符串。如果 `str` 不是字符串格式，会尝试隐式转换。

## 返回值说明

返回解码后的字符串。如果输入字符串不符合 application/x-www-form-urlencoded 格式，该函数将返回错误。

## 示例

```plaintext
mysql> select url_decode('https%3A%2F%2Fdocs.starrocks.io%2Fdocs%2Fintroduction%2FStarRocks_intro%2F');
+------------------------------------------------------------------------------------------+
| url_decode('https%3A%2F%2Fdocs.starrocks.io%2Fdocs%2Fintroduction%2FStarRocks_intro%2F') |
+------------------------------------------------------------------------------------------+
| https://docs.starrocks.io/docs/introduction/StarRocks_intro/                             |
+------------------------------------------------------------------------------------------+
```

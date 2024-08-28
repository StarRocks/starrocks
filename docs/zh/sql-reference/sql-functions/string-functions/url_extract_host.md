---
displayed_sidebar: docs
---

# url_extract_host

## 功能

从一个 URL 中截取 host 部分。

该函数从 3.3 版本开始支持。

## 语法

```haskell
VARCHAR url_extract_host(VARCHAR str)
```

## 参数说明

- `str`: 待截取的字符串。如果 `str` 不是字符串格式，该函数会先尝试进行隐式转换。

## 返回值说明

返回 host 字符串。

## 示例

```plaintext
mysql> select url_extract_host('httpa://starrocks.com/test/api/v1');
+-------------------------------------------------------+
| url_extract_host('httpa://starrocks.com/test/api/v1') |
+-------------------------------------------------------+
| starrocks.com                                         |
+-------------------------------------------------------+
```

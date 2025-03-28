---
displayed_sidebar: docs
---

# url_extract_host

URL からホスト部分を抽出します。

この関数は v3.3 以降でサポートされています。

## 構文

```haskell
VARCHAR url_extract_host(VARCHAR str)
```

## パラメータ

- `str`: ホスト文字列を抽出する対象の文字列。`str` が文字列でない場合、この関数はまず暗黙的なキャストを試みます。

## 戻り値

ホスト文字列を返します。

## 例

```plaintext
mysql> select url_extract_host('httpa://starrocks.com/test/api/v1');
+-------------------------------------------------------+
| url_extract_host('httpa://starrocks.com/test/api/v1') |
+-------------------------------------------------------+
| starrocks.com                                         |
+-------------------------------------------------------+
```
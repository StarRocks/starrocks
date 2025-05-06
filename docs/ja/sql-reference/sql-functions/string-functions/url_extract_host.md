---
displayed_sidebar: docs
---

# url_extract_host

## 説明

URLからホスト部分を抽出します。

この関数はバージョン3.3以降でサポートされています。

## 構文

```haskell
VARCHAR url_extract_host(VARCHAR str)
```

## パラメータ

- `str`: ホスト文字列を抽出するための文字列です。`str` が文字列でない場合、この関数は最初に暗黙的なキャストを試みます。

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
---
displayed_sidebar: docs
---

# url_decode

[application/x-www-form-urlencoded](https://www.w3.org/TR/html4/interact/forms.html#h-17.13.4.1) 形式から文字列を元に戻します。この関数は [url_encode](./url_encode.md) の逆の機能を持ちます。

この関数は v3.2 からサポートされています。

## 構文

```haskell
VARCHAR url_decode(VARCHAR str)
```

## パラメータ

- `str`: デコードする文字列。`str` が文字列でない場合、システムはまず暗黙的なキャストを試みます。

## 戻り値

デコードされた文字列を返します。

## 例

```plaintext
mysql> select url_decode('https%3A%2F%2Fdocs.starrocks.io%2Fdocs%2Fintroduction%2FStarRocks_intro%2F');
+------------------------------------------------------------------------------------------+
| url_decode('https%3A%2F%2Fdocs.starrocks.io%2Fdocs%2Fintroduction%2FStarRocks_intro%2F') |
+------------------------------------------------------------------------------------------+
| https://docs.starrocks.io/docs/introduction/StarRocks_intro/                             |
+------------------------------------------------------------------------------------------+
```
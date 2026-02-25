---
displayed_sidebar: docs
---

# url_encode

文字列を [application/x-www-form-urlencoded](https://www.w3.org/TR/html4/interact/forms.html#h-17.13.4.1) 形式に変換します。

この関数は v3.2 からサポートされています。

## 構文

```haskell
VARCHAR url_encode(VARCHAR str)
```

## パラメータ

- `str`: エンコードする文字列。`str` が文字列でない場合、この関数は最初に暗黙的なキャストを試みます。

## 戻り値

[application/x-www-form-urlencoded](https://www.w3.org/TR/html4/interact/forms.html#h-17.13.4.1) 形式に準拠したエンコードされた文字列を返します。

## 例

```plaintext
mysql> select url_encode('https://docs.starrocks.io/docs/introduction/StarRocks_intro/');
+----------------------------------------------------------------------------+
| url_encode('https://docs.starrocks.io/docs/introduction/StarRocks_intro/') |
+----------------------------------------------------------------------------+
| https%3A%2F%2Fdocs.starrocks.io%2Fdocs%2Fintroduction%2FStarRocks_intro%2F |
+----------------------------------------------------------------------------+
```
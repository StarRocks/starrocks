---
displayed_sidebar: docs
---

# url_encode

## 説明

URL 文字列をエンコードします。

## 構文

```haskell
url_encode(str)
```

## パラメータ

- `str`: エンコードする文字列。`str` が文字列型でない場合、最初に暗黙のキャストを試みます。

## 戻り値

エンコードされた文字列を返します。

## 例

```plaintext
mysql> select url_encode('https://docs.starrocks.io/en-us/latest/quick_start/Deploy');
+-------------------------------------------------------------------------+
| url_encode('https://docs.starrocks.io/en-us/latest/quick_start/Deploy') |
+-------------------------------------------------------------------------+
| https%3A%2F%2Fdocs.starrocks.io%2Fen-us%2Flatest%2Fquick_start%2FDeploy |
+-------------------------------------------------------------------------+
```
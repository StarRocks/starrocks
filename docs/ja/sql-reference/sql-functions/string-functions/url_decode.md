---
displayed_sidebar: docs
---

# url_decode

## 説明

URL をデコードされた文字列に変換します。

## 構文

```haskell
url_decode(str)
```

## パラメータ

- `str`: デコードする文字列。`str` が文字列型でない場合、まず暗黙的なキャストを試みます。

## 戻り値

エンコードされた文字列を返します。

## 例

```plaintext
mysql> select url_decode('https%3A%2F%2Fdocs.starrocks.io%2Fen-us%2Flatest%2Fquick_start%2FDeploy');
+---------------------------------------------------------------------------------------+
| url_decode('https%3A%2F%2Fdocs.starrocks.io%2Fen-us%2Flatest%2Fquick_start%2FDeploy') |
+---------------------------------------------------------------------------------------+
| https://docs.starrocks.io/en-us/latest/quick_start/Deploy                             |
+---------------------------------------------------------------------------------------+
```
---
displayed_sidebar: docs
---

# regexp

## 説明

指定された `pattern` の正規表現に、与えられた式が一致するかどうかを確認します。一致する場合は 1 を返し、そうでない場合は 0 を返します。入力パラメータのいずれかが NULL の場合は NULL を返します。

regexp() は [like()](like.md) よりも複雑なマッチング条件をサポートします。

## 構文

```Haskell
BOOLEAN regexp(VARCHAR expr, VARCHAR pattern);
```

## パラメータ

- `expr`: 文字列式。サポートされているデータ型は VARCHAR です。

- `pattern`: マッチするパターン。サポートされているデータ型は VARCHAR です。

## 戻り値

BOOLEAN 値を返します。

## 例

```Plain Text
mysql> select regexp("abc123","abc*");
+--------------------------+
| regexp('abc123', 'abc*') |
+--------------------------+
|                        1 |
+--------------------------+
1 row in set (0.06 sec)

select regexp("abc123","xyz*");
+--------------------------+
| regexp('abc123', 'xyz*') |
+--------------------------+
|                        0 |
+--------------------------+
```

## キーワード

regexp, regular
---
displayed_sidebar: docs
---

# regexp

指定された `pattern` によって、与えられた式が正規表現に一致するかどうかを確認します。一致する場合は 1 が返されます。それ以外の場合は 0 が返されます。入力パラメータのいずれかが NULL の場合、NULL が返されます。

regexp() は [like()](like.md) よりも複雑なマッチング条件をサポートしています。

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
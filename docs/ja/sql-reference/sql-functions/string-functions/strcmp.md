---
displayed_sidebar: docs
---

# strcmp

## 説明

この関数は2つの文字列を比較します。lhs と rhs が等しい場合は 0 を返します。lhs が辞書順で rhs より前にある場合は -1 を返します。lhs が辞書順で rhs より後にある場合は 1 を返します。引数が NULL の場合、結果は NULL です。

## 構文

```Haskell
INT strcmp(VARCHAR lhs, VARCHAR rhs)
```

## 例

```Plain Text
mysql> select strcmp("test1", "test1");
+--------------------------+
| strcmp('test1', 'test1') |
+--------------------------+
|                        0 |
+--------------------------+

mysql> select strcmp("test1", "test2");
+--------------------------+
| strcmp('test1', 'test2') |
+--------------------------+
|                       -1 |
+--------------------------+

mysql> select strcmp("test2", "test1");
+--------------------------+
| strcmp('test2', 'test1') |
+--------------------------+
|                        1 |
+--------------------------+

mysql> select strcmp("test1", NULL);
+-----------------------+
| strcmp('test1', NULL) |
+-----------------------+
|                  NULL |
+-----------------------+
```

## キーワード

STRCMP
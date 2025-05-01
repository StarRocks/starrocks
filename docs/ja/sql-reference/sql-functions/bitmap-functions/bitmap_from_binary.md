---
displayed_sidebar: docs
---

# bitmap_from_binary

## Description

特定のフォーマットを持つバイナリ文字列をビットマップに変換します。

この関数は、ビットマップデータを StarRocks にロードするために使用できます。

この関数は v3.0 からサポートされています。

## Syntax

```Haskell
BITMAP bitmap_from_binary(VARBINARY str)
```

## Parameters

`str`: サポートされているデータ型は VARBINARY です。

## Return value

BITMAP 型の値を返します。

## Examples

Example 1: 他のビットマップ関数と一緒にこの関数を使用します。

```Plain
mysql> select bitmap_to_string(bitmap_from_binary(bitmap_to_binary(bitmap_from_string("0,1,2,3"))));
+---------------------------------------------------------------------------------------+
| bitmap_to_string(bitmap_from_binary(bitmap_to_binary(bitmap_from_string('0,1,2,3')))) |
+---------------------------------------------------------------------------------------+
| 0,1,2,3                                                                               |
+---------------------------------------------------------------------------------------+
1 row in set (0.01 sec)
```
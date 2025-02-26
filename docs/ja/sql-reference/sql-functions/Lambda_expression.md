---
displayed_sidebar: docs
sidebar_position: 0.9
---

# Lambda expression

ラムダ式は、匿名関数であり、高階 SQL 関数のパラメータとして渡すことができます。ラムダ式を使用すると、より簡潔でエレガント、かつ拡張性のあるコードを開発できます。

ラムダ式は `->` 演算子を使用して記述され、「goes to」と読みます。`->` の左側は入力パラメータ（ある場合）で、右側は式です。

バージョン 2.5 以降、StarRocks は次の高階 SQL 関数でラムダ式の使用をサポートしています: [array_map()](./array-functions/array_map.md), [array_filter()](./array-functions/array_filter.md), [array_sum()](./array-functions/array_sum.md), および [array_sortby()](./array-functions/array_sortby.md)。

## Syntax

```Haskell
parameter -> expression
```

## Parameters

- `parameter`: ラムダ式の入力パラメータで、0 個、1 個、または複数のパラメータを受け入れることができます。2 つ以上の入力パラメータは括弧で囲みます。

- `expression`: `parameter` を参照する単純な式です。この式は入力パラメータに対して有効でなければなりません。

## Return value

戻り値の型は、`expression` の結果の型によって決まります。

## Usage notes

ほとんどのスカラー関数はラムダ本体で使用できますが、いくつかの例外があります:

- サブクエリはサポートされていません。例えば、`x -> 5 + (SELECT 3)`。
- 集計関数はサポートされていません。例えば、`x -> min(y)`。
- ウィンドウ関数はサポートされていません。
- テーブル関数はサポートされていません。
- 相関列はラムダ関数内に現れることはできません。

## Examples

ラムダ式の簡単な例:

```SQL
-- パラメータを受け取らず、5 を返します。
() -> 5    
-- x を受け取り、(x + 2) の値を返します。
x -> x + 2 
-- x と y を受け取り、それらの合計を返します。
(x, y) -> x + y 
-- x を受け取り、x に関数を適用します。
x -> COALESCE(x, 0)
x -> day(x)
x -> split(x,",")
x -> if(x>0,"positive","negative")
```

高階関数でラムダ式を使用する例:

```Haskell
select array_map((x,y,z) -> x + y, [1], [2], [4]);
+----------------------------------------------+
| array_map((x, y, z) -> x + y, [1], [2], [4]) |
+----------------------------------------------+
| [3]                                          |
+----------------------------------------------+
1 row in set (0.01 sec)
```
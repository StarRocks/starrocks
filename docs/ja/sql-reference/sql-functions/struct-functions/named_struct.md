---
displayed_sidebar: docs
---

# named_struct

指定されたフィールド名と値を持つ struct を作成します。

この関数は v3.1 以降でサポートされています。

## 構文

```Haskell
STRUCT named_struct({STRING name1, ANY val1} [, ...] )
```

## パラメーター

- `nameN`: STRING フィールド。

- `valN`: フィールド N の値を指定する任意の型の式。値は nullable です。

名前と値の式はペアでなければなりません。そうでない場合、struct を作成することはできません。少なくとも 1 組のフィールド名と値をカンマ（`,`）で区切って渡す必要があります。

## 戻り値

STRUCT 値を返します。

## 例

```plain
SELECT named_struct('a', 1, 'b', 2, 'c', 3);
+--------------------------------------+
| named_struct('a', 1, 'b', 2, 'c', 3) |
+--------------------------------------+
| {"a":1,"b":2,"c":3}                  |
+--------------------------------------+

SELECT named_struct('a', null, 'b', 2, 'c', 3);
+-----------------------------------------+
| named_struct('a', null, 'b', 2, 'c', 3) |
+-----------------------------------------+
| {"a":null,"b":2,"c":3}                  |
+-----------------------------------------+
```

## 参考文献

- [STRUCT data type](../../data-types/semi_structured/STRUCT.md)
- [row/struct](row.md)
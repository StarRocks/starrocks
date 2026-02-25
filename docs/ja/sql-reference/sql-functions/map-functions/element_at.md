---
displayed_sidebar: docs
---

# element_at

指定されたキーに対応するマップのキーと値のペアから値を返します。入力パラメータが NULL の場合や、キーがマップに存在しない場合、結果は NULL になります。

配列から要素を取得したい場合は、[element_at](../array-functions/element_at.md) を参照してください。

この関数は v3.0 以降でサポートされています。

## 構文

```Haskell
element_at(any_map, any_key)
```

## パラメータ

- `any_map`: 値を取得するための MAP 式。
- `any_key`: マップ内のキー。

## 戻り値

`any_key` が `any_map` に存在する場合、そのキーに対応する値が返されます。それ以外の場合は NULL が返されます。

## 例

```plain text
mysql> select element_at(map{1:3,2:4},1);
+-----------------+
| map{1:3,2:4}[1] |
+-----------------+
|               3 |
+-----------------+

mysql> select element_at(map{1:3,2:4},3);
+-----------------+
| map{1:3,2:4}[3] |
+-----------------+
|            NULL |
+-----------------+

mysql> select element_at(map{'a':1,'b':2},'a');
+-----------------------+
| map{'a':1,'b':2}['a'] |
+-----------------------+
|                     1 |
+-----------------------+
```

## キーワード

ELEMENT_AT, MAP
---
displayed_sidebar: docs
---

# array_concat

複数の配列を連結して、すべての要素を含む1つの配列にします。

連結する配列内の要素は、同じ型でも異なる型でもかまいません。ただし、要素は同じ型であることを推奨します。

Null は通常の値として処理されます。

## Syntax

```Haskell
array_concat(input0, input1, ...)
```

## Parameters

`input`: 連結したい1つ以上の配列を指定します。配列は `(input0, input1, ...)` 形式で指定します。この関数は、次のタイプの配列要素をサポートします: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, VARCHAR, DECIMALV2, DATETIME, DATE, および JSON。**JSON はバージョン 2.5 からサポートされています。**

## Return value

`input` パラメータで指定された配列内のすべての要素を含む配列を返します。返される配列の要素は、入力配列の要素と同じデータ型です。さらに、返される配列の要素は、入力配列とその要素の順序に従います。

## Examples

Example 1: 数値要素を含む配列を連結します。

```plaintext
select array_concat([57.73,97.32,128.55,null,324.2], [3], [5]) as res;

+-------------------------------------+

| res                                 |

+-------------------------------------+

| [57.73,97.32,128.55,null,324.2,3,5] |

+-------------------------------------+
```

Example 2: 文字列要素を含む配列を連結します。

```plaintext
select array_concat(["sql","storage","execute"], ["Query"], ["Vectorized", "cbo"]);

+----------------------------------------------------------------------------+

| array_concat(['sql','storage','execute'], ['Query'], ['Vectorized','cbo']) |

+----------------------------------------------------------------------------+

| ["sql","storage","execute","Query","Vectorized","cbo"]                     |

+----------------------------------------------------------------------------+
```

Example 3: 異なる型の2つの配列を連結します。

```plaintext
select array_concat([57,65], ["pear","apple"]);
+-------------------------------------------+
| array_concat([57, 65], ['pear', 'apple']) |
+-------------------------------------------+
| ["57","65","pear","apple"]                |
+-------------------------------------------+
```

Example 4: Null を通常の値として処理します。

```plaintext
select array_concat(["sql",null], [null], ["Vectorized", null]);

+---------------------------------------------------------+

| array_concat(['sql',NULL], [NULL], ['Vectorized',NULL]) |

+---------------------------------------------------------+

| ["sql",null,null,"Vectorized",null]                     |

+---------------------------------------------------------+
```
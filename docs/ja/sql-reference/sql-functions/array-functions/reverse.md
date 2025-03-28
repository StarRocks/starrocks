---
displayed_sidebar: docs
---

# reverse

文字列または配列を逆にします。文字列内の文字や配列要素を逆順にした文字列または配列を返します。

## Syntax

```Haskell
reverse(param)
```

## Parameters

`param`: 逆にする文字列または配列。VARCHAR、CHAR、または ARRAY 型である必要があります。

現在、この関数は一次元配列のみをサポートしており、配列要素は DECIMAL 型であってはなりません。この関数は次のタイプの配列要素をサポートします: BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、VARCHAR、DECIMALV2、DATETIME、DATE、および JSON。**JSON はバージョン 2.5 からサポートされています。**

## Return value

戻り値の型は `param` と同じです。

## Examples

Example 1: 文字列を逆にする。

```Plain Text
MySQL > SELECT REVERSE('hello');
+------------------+
| REVERSE('hello') |
+------------------+
| olleh            |
+------------------+
1 row in set (0.00 sec)
```

Example 2: 配列を逆にする。

```Plain Text
MYSQL> SELECT REVERSE([4,1,5,8]);
+--------------------+
| REVERSE([4,1,5,8]) |
+--------------------+
| [8,5,1,4]          |
+--------------------+
```

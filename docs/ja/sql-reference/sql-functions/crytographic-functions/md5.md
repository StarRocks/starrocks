---
displayed_sidebar: docs
---

# md5

MD5 メッセージダイジェストアルゴリズムを使用して、文字列の128ビットチェックサムを計算します。チェックサムは32文字の16進数文字列で表されます。

## Syntax

```sql
md5(expr)
```

## Parameters

`expr`: 計算する文字列。VARCHAR 型でなければなりません。

## Return value

VARCHAR 型のチェックサムを返します。これは32文字の16進数文字列です。

入力が NULL の場合、NULL が返されます。

## Examples

```sql
select md5('abc');
```

```plaintext
+----------------------------------+
| md5('abc')                       |
+----------------------------------+
| 900150983cd24fb0d6963f7d28e17f72 |
+----------------------------------+
1 row in set (0.01 sec)
```

```sql
select md5(null);
```

```plaintext
+-----------+
| md5(NULL) |
+-----------+
| NULL      |
+-----------+
1 row in set (0.00 sec)
```

## Keywords

MD5, ENCRYPTION
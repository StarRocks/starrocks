---
displayed_sidebar: "English"
---

# md5

Uses the MD5 Message-Digest Algorithm to calculate the 128-bit checksum of a string. The checksum is represented by a 32-character hexadecimal string.

## Syntax

```sql
md5(expr)
```

## Parameters

`expr`: the string to calculate. It must be of the VARCHAR type.

## Return value

Returns a checksum of the VARCHAR type, which is a 32-character hexadecimal string.

If the input is NULL, NULL is returned.

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

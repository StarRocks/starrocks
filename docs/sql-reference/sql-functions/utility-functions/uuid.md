# uuid

## Description

Returns a random UUID of the VARCHAR type. Two calls to this function can generate two different numbers. The UUID is 36 characters in length. It contains 5 hexadecimal numbers which are connected with four hyphens in the aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee format.

## Syntax

```Haskell
uuid();
```

## Parameters

None

## Return value

Returns a value of the VARCHAR type.

## Examples

```Plain Text
mysql> select uuid();
+--------------------------------------+
| uuid()                               |
+--------------------------------------+
| 74a2ed19-9d21-4a99-a67b-aa5545f26454 |
+--------------------------------------+
1 row in set (0.01 sec)
```

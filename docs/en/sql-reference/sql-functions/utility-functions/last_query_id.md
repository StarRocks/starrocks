# last_query_id

## Description

Obtains the ID of the most recently executed query in the current session.

## Syntax

```Haskell
VARCHAR last_query_id();
```

## Parameters

None

## Return value

Returns a value of the VARCHAR type.

## Examples

```Plain Text
mysql> select last_query_id();
+--------------------------------------+
| last_query_id()                      |
+--------------------------------------+
| 7c1d8d68-bbec-11ec-af65-00163e1e238f |
+--------------------------------------+
1 row in set (0.00 sec)
```

## Keywords

LAST_QUERY_ID

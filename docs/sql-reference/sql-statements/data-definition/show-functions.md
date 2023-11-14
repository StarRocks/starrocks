# SHOW FUNCTIONS

## Description

Queries all the custom (or built-in) functions under a database. If no database is specified, the current database is used by default.

## Syntax

```sql
SHOW [FULL] [BUILTIN] FUNCTIONS [IN|FROM db] [LIKE 'function_pattern']
```

## Parameters

```plain text
full: indicates showing all the functions. 

builtin: indicates showing functions provided by the system.

db: the name of the database to query.

function_pattern: the pattern used to filter function name. 
```

## Examples

```Plain Text
mysql> show full functions in testDb\G
*************************** 1. row ***************************
        Signature: my_add(INT,INT)
      Return Type: INT
    Function Type: Scalar
Intermediate Type: NULL
       Properties: {"symbol":"_ZN9starrocks_udf6AddUdfEPNS_15FunctionContextERKNS_6IntValES4_","object_file":"http://host:port/libudfsample.so","md5":"cfe7a362d10f3aaf6c49974ee0f1f878"}
*************************** 2. row ***************************
        Signature: my_count(BIGINT)
      Return Type: BIGINT
    Function Type: Aggregate
Intermediate Type: NULL
       Properties: {"object_file":"http://host:port/libudasample.so","finalize_fn":"_ZN9starrocks_udf13CountFinalizeEPNS_15FunctionContextERKNS_9BigIntValE","init_fn":"_ZN9starrocks_udf9CountInitEPNS_15FunctionContextEPNS_9BigIntValE","merge_fn":"_ZN9starrocks_udf10CountMergeEPNS_15FunctionContextERKNS_9BigIntValEPS2_","md5":"37d185f80f95569e2676da3d5b5b9d2f","update_fn":"_ZN9starrocks_udf11CountUpdateEPNS_15FunctionContextERKNS_6IntValEPNS_9BigIntValE"}

2 rows in set (0.00 sec)
mysql> show builtin functions in testDb like 'year%';
+---------------+
| Function Name |
+---------------+
| year          |
| years_add     |
| years_diff    |
| years_sub     |
+---------------+
2 rows in set (0.00 sec)
```

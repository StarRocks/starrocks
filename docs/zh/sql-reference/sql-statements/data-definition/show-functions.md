---
displayed_sidebar: "Chinese"
---

# SHOW FUNCTIONS

## 功能

查看数据库下所有的自定义(系统提供)函数。如果未指定数据库，则直接查询当前会话所在数据库。

## 语法

```sql
SHOW [FULL] [BUILTIN] [GLOBAL] FUNCTIONS [IN|FROM <db_name>] [LIKE 'function_pattern']
```

## 参数说明

* `FULL`: 表示显示函数的详细信息。
* `BUILTIN`: 表示显示系统提供的函数。
* `GLOBAL`: 表示显示全局函数。StarRocks 从 3.0 版本开始支持创建 [Global UDF](../../sql-functions/JAVA_UDF.md)。
* `db_name`: 要查询的数据库名称。
* `function_pattern`: 用于过滤函数名称。

## 示例

```sql
-- \G表示将查询结果进行按列打印
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

## 相关 SQL

* [DROP FUNCTION](./drop-function.md)
* [Java UDF](../../sql-functions/JAVA_UDF.md)

---
displayed_sidebar: "English"
---

# CREATE FUNCTION

## Description

Creates a user-defined function (UDF). Currently, you can only create Java UDFs, including Scalar functions, user-defined aggregate functions (UDAFs), user-defined window functions (UDWFs), and user-defined table functions (UDTFs).

**For details about how to compile, create, and use a Java UDF, see [Java UDF](../../sql-functions/JAVA_UDF.md).**

> **NOTE**
>
> To create a global UDF, you must have the SYSTEM-level CREATE GLOBAL FUNCTION privilege. To create a database-wide UDF, you must have the DATABASE-level CREATE FUNCTION privilege.

## Syntax

```sql
CREATE [GLOBAL][AGGREGATE | TABLE] FUNCTION function_name
(arg_type [, ...])
RETURNS return_type
PROPERTIES ("key" = "value" [, ...])
```

## Parameters

| **Parameter**      | **Required** | **Description**                                                     |
| ------------- | -------- | ------------------------------------------------------------ |
| GLOBAL        | No       | Whether to create a global UDF, supported from v3.0.  |
| AGGREGATE     | No       | Whether to create a UDAF or UDWF.       |
| TABLE         | No       | Whether to create a UDTF. If both `AGGREGATE` and `TABLE` are not specified, a Scalar function is created.               |
| function_name | Yes       | The name of the function you want to create. You can include the name of the database in this parameter, for example,`db1.my_func`. If `function_name` includes the database name, the UDF is created in that database. Otherwise, the UDF is created in the current database. The name of the new function and its parameters cannot be the same as an existing name in the destination database. Otherwise, the function cannot be created. The creation succeeds if the function name is the same but the parameters are different. |
| arg_type      | Yes       | Argument type of the function. The added argument can be represented by `, ...`. For the supported data types, see [Java UDF](../../sql-functions/JAVA_UDF.md#mapping-between-sql-data-types-and-java-data-types).|
| return_type      | Yes       | The return type of the function. For the supported data types, see [Java UDF](../../sql-functions/JAVA_UDF.md#mapping-between-sql-data-types-and-java-data-types). |
| PROPERTIES    | Yes       | Properties of the function, which vary depending on the type of the UDF to create. For details, see [Java UDF](../../sql-functions/JAVA_UDF.md#step-6-create-the-udf-in-starrocks)。 |

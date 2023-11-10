---
displayed_sidebar: "Chinese"
---

# CREATE FUNCTION

## 功能

创建 UDF（用户自定义函数）。当前仅支持创建 Java UDF，即 Java 语言编写的自定义函数，具体包括：

- Scalar UDF：自定义标量函数。
- UDAF：自定义聚合函数。
- UDWF：自定义窗口函数。
- UDTF：自定义表值函数。

**更多创建和使用 UDF 的信息，推荐参考 [Java UDF](../../sql-functions/JAVA_UDF.md)。**

> **注意**
>
> - 要创建全局 UDF，需要拥有 SYSTEM 级 CREATE GLOBAL FUNCTION 权限。
> - 要创建数据库级别的 UDF，需要拥有 DATABASE 级 CREATE FUNCTION 权限。

## 语法

```SQL
CREATE [GLOBAL][AGGREGATE | TABLE] FUNCTION function_name(arg_type [, ...])
RETURNS return_type
[PROPERTIES ("key" = "value" [, ...]) ]
```

## 参数说明

| **参数**      | **必选** | **说明**                                                     |
| ------------- | -------- | ------------------------------------------------------------ |
| GLOBAL        | 否       | 如需创建全局 UDF，需指定该关键字。从 3.0 版本开始支持。              |
| AGGREGATE     | 否       | 如要创建 UDAF 和 UDWF，需指定该关键字。                         |
| TABLE         | 否       | 如要创建 UDTF，需指定该关键字。                              |
| function_name | 是       | 函数名，可以包含数据库名称，比如，`db1.my_func`。如果 `function_name` 中包含了数据库名称，那么该 UDF 会创建在对应的数据库中，否则该 UDF 会创建在当前数据库。新函数名和参数不能与目标数据库中已有的函数相同，否则会创建失败；如只有函数名相同，参数不同，则可以创建成功。 |
| arg_type      | 是       | 函数的参数类型。具体支持的数据类型，请参见 [Java UDF](/sql-reference/sql-functions/JAVA_UDF.md#类型映射关系)。 |
| return_type      | 是       | 函数的返回值类型。具体支持的数据类型，请参见 [Java UDF](/sql-reference/sql-functions/JAVA_UDF.md#类型映射关系)。 |
| properties    | 是       | 函数相关属性。创建不同类型的 UDF 需配置不同的属性，详情和示例请参考 [Java UDF](/sql-reference/sql-functions/JAVA_UDF.md#步骤六在-starrocks-中创建-udf)。 |

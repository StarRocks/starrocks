# CREATE FUNCTION

## Description

### Syntax

```sql
CREATE [AGGREGATE] FUNCTION function_name
(arg_type [, ...])
RETURNS ret_type
[INTERMEDIATE inter_type]
[PROPERTIES ("key" = "value" [, ...]) ]
```

### Parameters

AGGREGATE: This parameter indicates that the created function is an aggregate function, otherwise it is a scalar function.

function_name: When creating the name of a function, you can include the name of the database. For example,`db1.my_func`。

arg_type: Argument type of function, the same type as that defined when creating table. The added argument can be represented by`, ...` The added type is the same as that of the last argument before it was added.

ret_type:  A function return type.

inter_type: A data type used to represent the intermediate stage of an aggregate function.

properties: It is used to set properties related to aggregate function, which include:

* object_file: Custom function dynamic library URL path, currently only supports HTTP/HTTPS protocol. This path needs to remain valid throughout the life cycle of the function. The option is required.

* symbol: Function signature of scalar functions for finding function entries from dynamic libraries. This option is required for scalar functions.

* init_fn: Initialization function signature of aggregate function. Required for aggregation functions.

* update_fn: Update function signature for aggregate function. Required for aggregation functions.

* merge_fn: Merge function signature of aggregate function. Required for aggregation functions.

* serialize_fn: Serialized function signature of aggregate function. Optional for aggregation functions. If not specified, the default serialization function will be used.

* finalize_fn: A function signature that aggregates functions to obtain the final result. Optional for aggregate functions. If not specified, the default obtained result function will be used.  

* md5: The MD5 value of the function dynamic link library, which is used to verify that the downloaded content is correct. This option is optional.

* prepare_fn: Function signature of the prepare function for finding the entry from the dynamic library. This option is optional for custom functions.

* close_fn: Function signature of the close function for finding the entry from the dynamic library. This option is optional for custom functions.

    This statement creates a custom function. To execute this command requires the `ADMIN` permission of the user.

    If the `function_name` contains the database name, the custom function will be created in the corresponding database. Otherwise the function will be created in the database where the current session is located. The name and parameters of the new function cannot be the same as functions already existing in the current namespace, otherwise the creation will fail. But the creation can be successful with the same name and different parameters.

## Examples

1. Create a custom scalar function.

    ```sql
    CREATE FUNCTION my_add(INT, INT) RETURNS INT PROPERTIES (
    "symbol" = "_ZN9starrocks_udf6AddUdfEPNS_15FunctionContextERKNS_6IntValES4_",
    "object_file" = "http://host:port/libmyadd.so"
    );
    ```

2. Create a custom scalar function with prepare/close functions.

    ```sql
    CREATE FUNCTION my_add(INT, INT) RETURNS INT PROPERTIES (
    "symbol" = "_ZN9starrocks_udf6AddUdfEPNS_15FunctionContextERKNS_6IntValES4_",
    "prepare_fn" = "_ZN9starrocks_udf14AddUdf_prepareEPNS_15FunctionContextENS0_18FunctionStateScopeE",
    "close_fn" = "_ZN9starrocks_udf12AddUdf_closeEPNS_15FunctionContextENS0_18FunctionStateScopeE",
    "object_file" = "http://host:port/libmyadd.so"
    );
    ```

3. Create a custom aggregate function.

    ```sql
    CREATE AGGREGATE FUNCTION my_count (BIGINT) RETURNS BIGINT PROPERTIES (
    "init_fn"="_ZN9starrocks_udf9CountInitEPNS_15FunctionContextEPNS_9BigIntValE",
    "update_fn"="_ZN9starrocks_udf11CountUpdateEPNS_15FunctionContextERKNS_6IntValEPNS_9BigIntValE",
    "merge_fn"="_ZN9starrocks_udf10CountMergeEPNS_15FunctionContextERKNS_9BigIntValEPS2_",
    "finalize_fn"="_ZN9starrocks_udf13CountFinalizeEPNS_15FunctionContextERKNS_9BigIntValE",
    "object_file"="http://host:port/libudasample.so"
    );
    ```

4. Create a scalar function with variable length arguments.

    ```sql
    CREATE FUNCTION strconcat(varchar, ...) RETURNS varchar properties (
    "symbol" = "_ZN9starrocks_udf6StrConcatUdfEPNS_15FunctionContextERKNS_6IntValES4_",
    "object_file" = "http://host:port/libmyStrConcat.so"
    );
    ```

# DROP FUNCTION

## description

### Syntax

```sql
DROP FUNCTION function_name
(arg_type [, ...])
```

### Parameters

`function_name`: the name of function to be dropped.

`arg_type`: the argument type of function to be dropped.

Delete a custom function. It can only be deleted when its name and parameter type are consistent.

## example

1. Drop a function.

    ```sql
    DROP FUNCTION my_add(INT, INT)
    ```

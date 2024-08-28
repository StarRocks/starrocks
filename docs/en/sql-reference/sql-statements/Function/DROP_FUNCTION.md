---
displayed_sidebar: docs
---

# DROP FUNCTION

## Description

Deletes a custom function. The function can only be deleted when its name and parameter type are consistent.

Only the owner of the custom function have the permissions to delete the function.

### Syntax

```sql
DROP FUNCTION function_name(arg_type [, ...])
```

### Parameters

`function_name`: the name of function to be dropped.

`arg_type`: the argument type of function to be dropped.

## Examples

1. Drop a function.

    ```sql
    DROP FUNCTION my_add(INT, INT)
    ```

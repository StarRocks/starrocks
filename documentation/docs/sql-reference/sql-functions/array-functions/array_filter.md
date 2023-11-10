# array_filter

## Description

Returns values from an array that matches the given filter.

This function comes in the following two forms. The lambda adoption allows for more flexible array filtering. For more information about lambda functions, see [Lambda expression](../Lambda_expression.md). This function is supported from v2.5.

## Syntax

```Haskell
array_filter(array, array<bool>)
array_filter(lambda_function, arr1,arr2...)
```

- `array_filter(array, array<bool>)`

  Returns values from an array that matches `array<bool>`.

- `array_filter(lambda_function, arr1,arr2...)`

  Returns values from an array that matches the lambda function.

## Parameters

`array`: the array to filter values from.

`array<bool>`: the expression used to filter values.

`lambda_function`: the lambda function used to filter values.

## Usage notes

- The two input parameters of `array_filter(array, array<bool>)` must be ARRAY and the filter expression can evaluate to `array<bool>`.
- The lambda function in `array_filter(lambda_function, arr1,arr2...)` follows the usage notes in [array_map()](array_map.md).
- If the input array is null, null is returned. If the filter array is null, an empty array is returned.

## Examples

- Examples without using lambda functions

    ```Plain
    -- All the elements in the array match the filter.
    select array_filter([1,2,3],[1,1,1]);
    +------------------------------------+
    | array_filter([1, 2, 3], [1, 1, 1]) |
    +------------------------------------+
    | [1,2,3]                            |
    +------------------------------------+
    1 row in set (0.01 sec)
    
    -- The filter is null and an empty array is returned.
    select array_filter([1,2,3],null);
    +-------------------------------+
    | array_filter([1, 2, 3], NULL) |
    +-------------------------------+
    | []                            |
    +-------------------------------+
    1 row in set (0.01 sec)
    
    -- The input array is null, null is returned.
    select array_filter(null,[1]);
    +-------------------------+
    | array_filter(NULL, [1]) |
    +-------------------------+
    | NULL                    |
    +-------------------------+
    
    -- Both the input array and filter are null. Null is returned.
    select array_filter(null,null);
    +--------------------------+
    | array_filter(NULL, NULL) |
    +--------------------------+
    | NULL                     |
    +--------------------------+
    1 row in set (0.01 sec)
    
    -- The filter contains a null element and an empty array is returned.
    select array_filter([1,2,3],[null]);
    +---------------------------------+
    | array_filter([1, 2, 3], [NULL]) |
    +---------------------------------+
    | []                              |
    +---------------------------------+
    1 row in set (0.01 sec)
    
    -- The filter contains two null elements and an empty array is returned.
    select array_filter([1,2,3],[null,null]);
    +---------------------------------------+
    | array_filter([1, 2, 3], [NULL, NULL]) |
    +---------------------------------------+
    | []                                    |
    +---------------------------------------+
    1 row in set (0.00 sec)
    
    -- Only one element matches the filter.
    select array_filter([1,2,3],[null,1,0]);
    +---------------------------------------+
    | array_filter([1, 2, 3], [NULL, 1, 0]) |
    +---------------------------------------+
    | [2]                                   |
    +---------------------------------------+
    1 row in set (0.00 sec)
    ```

- Examples using lambda functions

  ```Plain
    -- Return the elements in x that are less than the elements in y.
    select array_filter((x,y) -> x < y, [1,2,null], [4,5,6]);
    +--------------------------------------------------------+
    | array_filter((x, y) -> x < y, [1, 2, NULL], [4, 5, 6]) |
    +--------------------------------------------------------+
    | [1,2]                                                  |
    +--------------------------------------------------------+
  ```

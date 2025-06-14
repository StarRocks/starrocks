---
displayed_sidebar: docs
---

# dictionary_get



Query the value mapped to the key in a dictionary object.

## Syntax

```SQL
dictionary_get('dictionary_object_name', key_expression_list, [NULL_IF_NOT_EXIST])

key_expression_list ::=
    key_expression [, ...]

key_expression ::=
    column_name | const_value
```

## Parameters

- `dictionary_name`: The name of the dictionary object.
- `key_expression_list`: A list of expressions for all key columns. It can be a list of column names or a list of values.
- `NULL_IF_NOT_EXIST` (Optional): Whether to return if the key does not exist in the dictionary cache. Valid values:
  - `true`: Null is returned if the key does not exist.
  - `false` (Default): An exception is thrown if the key does not exist.

## Returns

Returns the values of value columns as a STRUCT type. Therefore, you can use `[N]` or `.<column_name>` to specify a particular column's value. `N` represents the column's position, starting from 1.

## Examples

The following examples uses the dataset from the examples of [dict_mapping](dict_mapping.md).

- Example 1: Query the values of the value column mapped to the key column `order_uuid` in the dictionary object `dict_obj`.

    ```Plain
    MySQL > SELECT dictionary_get('dict_obj', order_uuid) FROM dict;
    +--------------------+
    | DICTIONARY_GET     |
    +--------------------+
    | {"order_id_int":1} |
    | {"order_id_int":3} |
    | {"order_id_int":2} |
    +--------------------+
    3 rows in set (0.02 sec)
    ```

- Example 2: Query the value of the value column mapped to key `a1` in the dictionary object `dict_obj`.

    ```Plain
    MySQL > SELECT dictionary_get("dict_obj", "a1");
    +--------------------+
    | DICTIONARY_GET     |
    +--------------------+
    | {"order_id_int":1} |
    +--------------------+
    1 row in set (0.01 sec)
    ```

- Example 3: Query the values of the value columns mapped to key `1` in the dictionary object `dimension_obj`.

    ```Plain
    MySQL > SELECT dictionary_get("dimension_obj", 1);
    +-----------------------------------------------------------------------------------------------------------------+
    | DICTIONARY_GET                                                                                                  |
    +-----------------------------------------------------------------------------------------------------------------+
    | {"ProductName":"T-Shirt","Category":"Apparel","SubCategory":"Shirts","Brand":"BrandA","Color":"Red","Size":"M"} |
    +-----------------------------------------------------------------------------------------------------------------+
    1 row in set (0.01 sec)
    ```

- Example 4: Query the value of the first value column mapped to key `1` in the dictionary object `dimension_obj`.

    ```Plain
    MySQL > SELECT dictionary_get("dimension_obj", 1)[1];
    +-------------------+
    | DICTIONARY_GET[1] |
    +-------------------+
    | T-Shirt           |
    +-------------------+
    1 row in set (0.01 sec)
    ```

- Example 5: Query the value of the second value column mapped to key `1` in the dictionary object `dimension_obj`.

    ```Plain
    MySQL > SELECT dictionary_get("dimension_obj", 1)[2];
    +-------------------+
    | DICTIONARY_GET[2] |
    +-------------------+
    | Apparel           |
    +-------------------+
    1 row in set (0.01 sec)
    ```

- Example 6: Query the value of `ProductName` value column mapped to key `1` in the dictionary object `dimension_obj`.

    ```Plain
    MySQL > SELECT dictionary_get("dimension_obj", 1).ProductName;
    +----------------------------+
    | DICTIONARY_GET.ProductName |
    +----------------------------+
    | T-Shirt                    |
    +----------------------------+
    1 row in set (0.01 sec)
    ```
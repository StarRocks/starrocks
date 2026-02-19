---
displayed_sidebar: docs
sidebar_position: 0.95
---

# SQL UDF

Since version 4.1, StarRocks supports creating SQL user-define-functions(UDFs), allowing users to create functions using SQL expressions.

SQL UDF is a lightweight function definition method that encapsulates a SQL expression into a reusable function, which is dynamically expanded into the actual SQL expression at query time.

## Overview

Main features of SQL UDF:

- **Dynamic Expansion**: Expands function calls into actual SQL expressions during the query optimization phase
- **Type Safety**: Supports explicit declaration of parameter types and return types
- **Parameterized**: Supports named parameters to improve function readability

Applicable scenarios:
- Complex SQL expressions that need to be reused
- Simple data transformation and calculation logic
- Standardized data processing rules

## Syntax

### Creating SQL UDF

```SQL
CREATE [GLOBAL] FUNCTION
    function_name(arg1_name arg1_type, arg2_name arg2_type, ...)
RETURNS expression
```

#### Parameters

| **Parameter**      | **Description**                                                                                      |
| -------------------|------------------------------------------------------------------------------------------------------|
| GLOBAL             | Optional. If specified, creates a global function visible to all databases                          |
| function_name      | Function name. Can include database name, e.g., `db1.my_func`                                       |
| arg_name           | Parameter name used to reference in the expression                                                   |
| arg_type           | Parameter type, supports all StarRocks basic data types                                              |
| expression         | SQL expression that will be expanded into actual expression when function is called                  |

### Dropping SQL UDF

```SQL
DROP FUNCTION [IF EXISTS] function_name(arg_type [, ...])
```

### Showing SQL UDF

```SQL
SHOW [GLOBAL] FUNCTIONS;
```

## Usage Examples

### Example 1: String Processing Function

```SQL
-- Create a function that converts string to uppercase and adds a prefix
CREATE FUNCTION format_username(name STRING)
RETURNS concat('USER_', upper(name));

-- Use the function
SELECT format_username('alice');
-- Result: USER_ALICE

-- Use in queries
SELECT format_username(username) as display_name FROM users;
```

### Example 2: Multi-parameter Calculation Function

```SQL
-- Create a function that calculates discounted price
CREATE FUNCTION calculate_discount_price(original_price DECIMAL(10,2), discount_rate DOUBLE)
RETURNS original_price * (1 - discount_rate);

-- Use the function
SELECT calculate_discount_price(100.00, 0.2);  -- Result: 80.00
SELECT calculate_discount_price(price, 0.15) as final_price FROM products;
```

### Example 3: Complex Expression Encapsulation

```SQL
-- Create a function that extracts JSON and transforms
CREATE FUNCTION extract_user_info(json_str STRING, field_name STRING)
RETURNS get_json_string(get_json_string(json_str, concat('$.', field_name)), '$.value');

-- Simplify complex nested calls
SELECT extract_user_info(user_data, 'email') as user_email FROM events;
```

### Example 4: Conditional Logic Function

```SQL
-- Create a function with conditional logic
CREATE FUNCTION classify_temperature(temp DOUBLE)
RETURNS CASE
    WHEN temp >= 30 THEN 'hot'
    WHEN temp >= 20 THEN 'warm'
    WHEN temp >= 10 THEN 'cool'
    ELSE 'cold'
END;

-- Use the function
SELECT classify_temperature(25);  -- Result: warm
```

### Example 5: Global SQL UDF

```SQL
-- Create a global function visible to all databases
CREATE GLOBAL FUNCTION format_date_display(dt DATETIME)
RETURNS concat(year(dt), '-', lpad(month(dt), 2, '0'), '-', lpad(day(dt), 2, '0'));

-- Can be used directly in any database
SELECT format_date_display(create_time) from my_table;
```

## Advanced Features

### 1. Nested Usage

SQL UDF supports nested calls:

```SQL
CREATE FUNCTION func_a(x INT, y INT) RETURNS x + y;
CREATE FUNCTION func_b(a INT, b INT) RETURNS func_a(a, b) * 2;

SELECT func_b(3, 4);  -- Result: 14
```

### 2. Type Conversion

SQL UDF supports implicit type conversion:

```SQL
CREATE FUNCTION convert_and_add(a STRING, b INT)
RETURNS cast(a AS INT) + b;

SELECT convert_and_add('100', 50);  -- Result: 150
```

### 3. Combination with Built-in Functions

SQL UDF can be freely combined with StarRocks built-in functions:

```SQL
CREATE FUNCTION get_year_month(dt DATETIME)
RETURNS concat(year(dt), '-', lpad(month(dt), 2, '0'));

SELECT get_year_month(create_time) as ym FROM events GROUP BY ym;
```

## Viewing and Managing

### Showing All Functions

```SQL
-- Show functions in current database
SHOW FUNCTIONS;

-- Show all global functions
SHOW GLOBAL FUNCTIONS;
```

### Dropping Functions

```SQL
-- Drop function in current database
DROP FUNCTION format_username(STRING);

-- Drop global function
DROP GLOBAL FUNCTION format_date_display(DATETIME);

-- Use IF EXISTS to avoid errors
DROP FUNCTION IF EXISTS my_function(INT, STRING);
```

### Replacing Functions

Use the `OR REPLACE` keyword to replace an existing function:

```SQL
-- Modify function definition
CREATE OR REPLACE FUNCTION calculate_tax(amount DECIMAL(10,2))
RETURNS amount * 0.1 + 5.0;
```

## Performance Characteristics

SQL UDF is expanded into actual SQL expressions during the query optimization phase, with the following performance characteristics:

1. **Zero Function Call Overhead**: No function call overhead like traditional UDFs
2. **Optimizer Visibility**: The optimizer can see the complete expression for better optimization
3. **Predicate Pushdown**: Supports predicate pushdown and other optimization rules

For example, the following query:

```SQL
CREATE FUNCTION my_func(x INT) RETURNS x * 2 + 1;

SELECT * FROM t WHERE my_func(a) > 10;
```

Will be expanded by the optimizer as:

```SQL
SELECT * FROM t WHERE a * 2 + 1 > 10;
```

## Limitations and Considerations

1. **Parameter Count Limitation**: Parameter count should match variables used in expression
2. **Type Matching**: Parameter types should match usage in expression

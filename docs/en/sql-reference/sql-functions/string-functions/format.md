---
displayed_sidebar: docs
---

# format

Formats a string using printf-style format specifiers, allowing you to create formatted output by combining a format string with variable arguments.

This function provides comprehensive string formatting capabilities similar to printf, supporting various data types, positional arguments, width and precision controls, alignment options, and special formatting features. It is particularly useful for creating structured output, reports, and formatted displays in SQL queries.

## Syntax

```Haskell
VARCHAR format(VARCHAR format, ...)
```

## Parameters

- `format`: the format string containing format specifiers. VARCHAR is supported. The format string can include literal text and format specifiers.
- `...`: variable number of arguments to be formatted according to the format string. Supported data types include VARCHAR, BIGINT/INTEGER/SMALLINT, and DOUBLE/FLOAT.

## Return value

Returns a VARCHAR value representing the formatted string according to the format specification.

- If the format string is NULL, returns NULL
- If any argument is NULL, it is treated as an empty string in the output
- If there are insufficient arguments for the format specifiers, missing arguments are replaced with empty strings
- If format specifiers are malformed, the original format string is used as fallback

## Supported format specifiers

### Basic conversion specifiers
- `%s`: String argument
- `%d` or `%i`: Integer argument (decimal)
- `%f`: Floating-point argument

### Format modifier flags
- **Alignment**: `-` (left-align), default is right-align
- **Zero padding**: `0` (for numeric values)
- **Comma separator**: `,` (for numeric grouping)
- **Width**: `<number>` (minimum field width)
- **Precision**: `.<number>` (for floating-point precision or maximum string length)

### Positional arguments
- `%n$s` where n is the argument position (1-based): Use the n-th argument instead of the next sequential argument

### Special characters
- `%%`: Escaped percent sign (literal %)

## Usage notes

- The function follows printf-style formatting conventions
- String truncation occurs if the formatted result exceeds the column size limit
- Floating-point precision defaults to 6 decimal places if not specified
- Integer values are formatted as decimal numbers
- For positional arguments, argument indices are 1-based
- The function handles type conversions automatically for supported data types
- Malformed format strings fall back to the original format string

## Examples

Example 1: Basic string formatting.

```sql
SELECT format('Hello %s!', 'World');
-- Hello World!

SELECT format('Number: %d, String: %s', 42, 'test');
-- Number: 42, String: test

SELECT format('Pi: %.2f', 3.14159);
-- Pi: 3.14
```

Example 2: Alignment and width control.

```sql
SELECT format('%-10s', 'left');
-- left       (left-aligned, 10 characters)

SELECT format('%10s', 'right');
--       right (right-aligned, 10 characters)

SELECT format('%05d', 42);
-- 00042 (zero-padded integer)

SELECT format('%-15s %10s', 'Name:', 'John');
-- Name:                John
```

Example 3: Floating-point precision.

```sql
SELECT format('%.0f', 3.14159);
-- 3

SELECT format('%.2f', 3.14159);
-- 3.14

SELECT format('%.5f', 3.14159);
-- 3.14159

SELECT format('%.10f', 3.14159);
-- 3.1415900000
```

Example 4: Positional arguments.

```sql
SELECT format('%2$s %3$s %1$s', 'a', 'b', 'c');
-- b c a

SELECT format('%1$s %2$s %1$s', 'repeat', 'once');
-- repeat once repeat

SELECT format('%3$s %2$s %1$s', 'first', 'second', 'third');
-- third second first
```

Example 5: Complex formatting with multiple types.

```sql
SELECT format('Value: %-8.2f, Count: %04d, Name: %s', 2.3456789, 42, 'test');
-- Value: 2.35    , Count: 0042, Name: test

SELECT format('User %s has %d points (%.1f%% complete)', 'Alice', 150, 75.5);
-- User Alice has 150 points (75.5% complete)
```

Example 6: Handling special characters and edge cases.

```sql
SELECT format('Percentage: 50%% complete');
-- Percentage: 50% complete

SELECT format('%s', NULL);
-- (empty string)

SELECT format(NULL, 'test');
-- NULL

SELECT format('%s %s', 'only_one_arg');
-- only_one_arg (second argument missing, outputs empty string)
```

Example 7: Table data formatting with dynamic format strings.

```sql
-- Create a sample table with format specifications
CREATE TABLE format_test (
    format_str VARCHAR(100),
    arg1 VARCHAR(50),
    arg2 VARCHAR(50),
    arg3 VARCHAR(50)
);

INSERT INTO format_test VALUES 
('%s %s', 'hello', 'world', NULL),
('%d %s', '123', 'test', NULL),
('%3$s %1$s', 'first', NULL, 'third'),
('Pi: %.2f', '3.14159', NULL, NULL);

SELECT 
    format_str,
    format(format_str, arg1, arg2, arg3) as formatted_result
FROM format_test;

-- Expected output:
-- +-------------+-------------------+
-- | format_str  | formatted_result  |
-- +-------------+-------------------+
-- | %s %s       | hello world       |
-- | %d %s       | 123 test          |
-- | %3$s %1$s   | third first       |
-- | Pi: %.2f    | Pi: 3.14          |
-- +-------------+-------------------+
```

Example 8: Number formatting with commas.

```sql
SELECT format('%,.2f', 1234567.89);
-- 1,234,567.89

SELECT format('%,d', 1000000);
-- 1,000,000

SELECT format('Sales: $%,.2f', 2500000.75);
-- Sales: $2,500,000.75
```

## keyword

FORMAT
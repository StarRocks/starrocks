-- name: test_decimal256_string_functions
DROP DATABASE IF EXISTS test_decimal256_string;
CREATE DATABASE test_decimal256_string;
USE test_decimal256_string;

-- Create test table with decimal256 types
CREATE TABLE decimal_string_test (
    id INT,
    d50_15 DECIMAL(50,15),
    d76_20 DECIMAL(76,20),
    d76_0 DECIMAL(76,0),
    category VARCHAR(10)
) PROPERTIES("replication_num"="1");

-- Insert test data with various decimal patterns - using values that exceed decimal128 range
INSERT INTO decimal_string_test VALUES
(1, 12345678901234567890123456789012345.123456789012345, 12345678901234567890123456789012345678901234567890123456.12345678901234567890, 1234567890123456789012345678901234567890123456789012345678901234567890123456, 'A'),
(2, -98765432109876543210987654321098765.456789012345678, -98765432109876543210987654321098765432109876543210987654.45678901234567890123, -9876543210987654321098765432109876543210987654321098765432109876543210987654, 'B'),
(3, 0.000000000000001, 0.00000000000000000001, 0, 'C'),
(4, 99999888887777766666555554444433333.999999999999999, 99999888887777766666555554444433333222221111100009999988.99999999999999999999, 9999988888777776666655555444443333322222111110000099999888877776666555544, 'A'),
(5, -88888777776666655555444443333322222.888888888888888, -88888777776666655555444443333322222111110000099999888877.88888888888888888888, -8888877777666665555544444333332222211111000009999988888777766665555444433, 'B'),
(6, 11111222223333344444555556666677777.111111111111111, 11111222223333344444555556666677777888889999900001111122.11111111111111111111, 1111122222333334444455555666667777788888999990000011111222233334444555566, 'C'),
(7, -77777666665555544444333332222211111.777777777777777, -77777666665555544444333332222211111000009999988888777776.77777777777777777777, -7777766666555554444433333222221111100000999998888877777666655554444333322, 'A');

-- Test 1: Basic string conversion and manipulation
SELECT
    'Test1_BASIC_STRING_CONVERSION' as test_name,
    id,
    d50_15,
    CAST(d50_15 AS STRING) as d50_as_string,
    CONCAT('Value: ', CAST(d50_15 AS STRING)) as formatted_d50,
    CONCAT('D50=', CAST(d50_15 AS STRING), ', D76=', CAST(d76_20 AS STRING)) as combined_string
FROM decimal_string_test
ORDER BY id;

-- Test 2: String length and truncation operations
SELECT
    'Test2_STRING_LENGTH_TRUNCATION' as test_name,
    id,
    d76_0,
    CAST(d76_0 AS STRING) as d76_string,
    LENGTH(CAST(d76_0 AS STRING)) as string_length,
    LEFT(CAST(d76_0 AS STRING), 10) as left_10_chars,
    RIGHT(CAST(d76_0 AS STRING), 10) as right_10_chars,
    SUBSTR(CAST(d76_0 AS STRING), 1, 20) as first_20_chars
FROM decimal_string_test
ORDER BY id;

-- Test 3: String pattern matching and search
SELECT
    'Test3_STRING_PATTERN_MATCHING' as test_name,
    id,
    d50_15,
    CAST(d50_15 AS STRING) as decimal_string,
    LOCATE('.', CAST(d50_15 AS STRING)) as decimal_point_position,
    LOCATE('-', CAST(d50_15 AS STRING)) as negative_sign_position,
    CASE WHEN CAST(d50_15 AS STRING) LIKE '%-%.%' THEN 'NEGATIVE_DECIMAL'
         WHEN CAST(d50_15 AS STRING) LIKE '%.%' THEN 'POSITIVE_DECIMAL'
         WHEN CAST(d50_15 AS STRING) LIKE '-%' THEN 'NEGATIVE_INTEGER'
         ELSE 'POSITIVE_INTEGER'
    END as number_type
FROM decimal_string_test
ORDER BY id;

-- Test 4: Regular expression operations
SELECT
    'Test4_REGEX_OPERATIONS' as test_name,
    id,
    d76_20,
    CAST(d76_20 AS STRING) as decimal_string,
    REGEXP_EXTRACT(CAST(d76_20 AS STRING), '^(-?[0-9]+)', 1) as integer_part,
    REGEXP_EXTRACT(CAST(d76_20 AS STRING), '\\.([0-9]+)$', 1) as decimal_part,
    REGEXP_REPLACE(CAST(d76_20 AS STRING), '0+$', '') as trimmed_trailing_zeros,
    REGEXP_EXTRACT(CAST(d76_20 AS STRING), '^(-?[0-9]+\\.[0-9]{0,5})', 1) as first_5_decimal_places
FROM decimal_string_test
WHERE d76_20 != 0
ORDER BY id;

-- Test 5: String splitting and parsing
SELECT
    'Test5_STRING_SPLITTING' as test_name,
    id,
    d50_15,
    CAST(d50_15 AS STRING) as original_string,
    SPLIT_PART(CAST(ABS(d50_15) AS STRING), '.', 1) as integer_part_split,
    SPLIT_PART(CAST(ABS(d50_15) AS STRING), '.', 2) as decimal_part_split,
    CASE WHEN d50_15 < 0 THEN '-' ELSE '+' END as sign_part
FROM decimal_string_test
ORDER BY id;

-- Test 6: String formatting and padding
SELECT
    'Test6_STRING_FORMATTING' as test_name,
    id,
    d76_0,
    CAST(d76_0 AS STRING) as original_string,
    LPAD(CAST(ABS(d76_0) AS STRING), 80, '0') as zero_padded,
    RPAD(CAST(d76_0 AS STRING), 20, ' ') as space_padded
FROM decimal_string_test
ORDER BY id;

-- Test 7: Case conversion and special formatting
SELECT
    'Test7_CASE_SPECIAL_FORMATTING' as test_name,
    id,
    category,
    d50_15,
    UPPER(CONCAT(category, '_', CAST(d50_15 AS STRING))) as upper_formatted,
    LOWER(CONCAT(category, '_', CAST(d50_15 AS STRING))) as lower_formatted
FROM decimal_string_test
ORDER BY id;

-- Test 8: Complex string operations with multiple decimals
SELECT
    'Test8_COMPLEX_STRING_OPERATIONS' as test_name,
    id,
    d50_15,
    d76_20,
    d76_0,
    CONCAT_WS(' | ', 
        CONCAT('D50: ', CAST(d50_15 AS STRING)),
        CONCAT('D76_20: ', CAST(d76_20 AS STRING)),
        CONCAT('D76_0: ', CAST(d76_0 AS STRING))
    ) as pipe_separated,
    REPLACE(
        REPLACE(CAST(d50_15 AS STRING), '.', '_DOT_'),
        '-', '_MINUS_'
    ) as symbol_replaced,
    REVERSE(CAST(ABS(d50_15) AS STRING)) as reversed_string
FROM decimal_string_test
ORDER BY id;

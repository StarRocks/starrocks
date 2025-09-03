-- name: test_decimal256_complex_types

DROP DATABASE IF EXISTS test_decimal256_complex;
CREATE DATABASE test_decimal256_complex;
USE test_decimal256_complex;

-- =============================================================================
-- Part 1: ARRAY with decimal256 tests
-- =============================================================================

-- Create table with ARRAY<decimal256> columns
CREATE TABLE decimal_array_test (
    id INT,
    decimal_array_50 ARRAY<DECIMAL(50,15)>,
    decimal_array_76 ARRAY<DECIMAL(76,20)>,
    simple_decimals ARRAY<DECIMAL(40,10)>
) PROPERTIES("replication_num"="1");

-- Insert test data for arrays - using decimal256 values beyond decimal128 range  
INSERT INTO decimal_array_test VALUES
(1, [12345678901234567890123456789012345.123456789012345, 98765432109876543210987654321098765.456789012345678, 55555555555555555555555555555555555.789012345678901], 
    [12345678901234567890123456789012345678901234567890123456.12345678901234567890, 98765432109876543210987654321098765432109876543210987654.45678901234567890123, 55555555555555555555555555555555555555555555555555555555.78901234567890123456],
    [12345678901234567890123456789.1234567890, 98765432109876543210987654321.2345678901, 55555555555555555555555555555.3456789012]),
(2, [77777777777777777777777777777777777.999999999999999, -44444444444444444444444444444444444.123456789012345, 0.000000000000001],
    [77777777777777777777777777777777777777777777777777777777.99999999999999999999, -44444444444444444444444444444444444444444444444444444444.12345678901234567890, 0.00000000000000000001],
    [77777777777777777777777777777.9999999999, -44444444444444444444444444444.1234567890, 0.0000000001]),
(3, [99999888887777766666555554444433333.999999999999999],
    [99999888887777766666555554444433333222221111100009999988.99999999999999999999],
    [99999888887777766666555554444.9999999999]),
(4, [], [], []),
(5, [0.000000000000001, -0.000000000000001, 11111111111111111111111111111111111.0, -22222222222222222222222222222222222.0],
    [0.00000000000000000001, -0.00000000000000000001, 11111111111111111111111111111111111111111111111111111111.0, -22222222222222222222222222222222222222222222222222222222.0],
    [0.0000000001, -0.0000000001, 11111111111111111111111111111.0, -22222222222222222222222222222.0]);

-- Test 1: Basic array operations with decimal256
SELECT
    'Test1_ARRAY_BASIC_OPERATIONS' as test_name,
    id,
    decimal_array_50,
    CARDINALITY(decimal_array_50) as array_size,
    decimal_array_50[1] as first_element,
    decimal_array_50[CARDINALITY(decimal_array_50)] as last_element
FROM decimal_array_test
ORDER BY id;

-- Test 2: Array element access
SELECT
    'Test2_ARRAY_ELEMENT_ACCESS' as test_name,
    id,
    simple_decimals,
    simple_decimals[1] as first_decimal,
    simple_decimals[2] as second_decimal,
    simple_decimals[3] as third_decimal
FROM decimal_array_test
WHERE CARDINALITY(simple_decimals) >= 2
ORDER BY id;

-- =============================================================================
-- Part 2: MAP with decimal256 tests  
-- =============================================================================

-- Create table with MAP<string, decimal256> columns
CREATE TABLE decimal_map_test (
    id INT,
    decimal_map_50 MAP<STRING, DECIMAL(50,15)>,
    decimal_map_76 MAP<STRING, DECIMAL(76,0)>,
    key_decimal_map MAP<DECIMAL(40,10), STRING>
) PROPERTIES("replication_num"="1");

-- Insert test data for maps - using decimal256 values beyond decimal128 range
INSERT INTO decimal_map_test VALUES
(1, MAP{'price': 12345678901234567890123456789012345.123456789012345, 'cost': 98765432109876543210987654321098765.456789012345678, 'profit': 55555555555555555555555555555555555.666666666666666},
    MAP{'large_num1': 1234567890123456789012345678901234567890123456789012345678901234567890123456, 'large_num2': 9876543210987654321098765432109876543210987654321098765432109876543210987654},
    MAP{12345678901234567890123456789.1234567890: 'huge_ten', 98765432109876543210987654321.2345678901: 'huge_twenty', 55555555555555555555555555555.3456789012: 'huge_thirty'}),
(2, MAP{'balance': -77777777777777777777777777777777777.999999999999999, 'limit': 88888888888888888888888888888888888.000000000000000, 'available': 44444444444444444444444444444444444.000000000000000},
    MAP{'negative': -9999988888777776666655555444443333322222111110000099999888877776666555544, 'zero': 0, 'positive': 8888877777666665555544444333332222211111000009999988888777766665555444433},
    MAP{-77777777777777777777777777777.9876543210: 'huge_negative', 0.0000000000: 'zero', 88888888888888888888888888888.1111111111: 'huge_positive'}),
(3, MAP{},
    MAP{},
    MAP{}),
(4, MAP{'small': 0.000000000000001, 'tiny': 0.000000000000000},
    MAP{'one': 1, 'max': 9999988888777776666655555444443333322222111110000099999888877776666555544},
    MAP{0.0000000001: 'very_small', 99999888887777766666555554444.9999999999: 'very_large'});

-- Test 4: Basic map operations with decimal256
SELECT
    'Test4_MAP_BASIC_OPERATIONS' as test_name,
    id,
    decimal_map_50,
    MAP_SIZE(decimal_map_50) as map_size,
    decimal_map_50['price'] as price_value,
    MAP_KEYS(decimal_map_50) as all_keys,
    MAP_VALUES(decimal_map_50) as all_values
FROM decimal_map_test
ORDER BY id;

-- Test 5: Map operations with large decimal256 values
SELECT
    'Test5_MAP_LARGE_DECIMALS' as test_name,
    id,
    decimal_map_76,
    MAP_SIZE(decimal_map_76) as map_size,
    decimal_map_76['large_num1'] as large_value1,
    decimal_map_76['negative'] as negative_value
FROM decimal_map_test
WHERE MAP_SIZE(decimal_map_76) > 0
ORDER BY id;

-- Test 6: Map with decimal keys
SELECT
    'Test6_MAP_DECIMAL_KEYS' as test_name,
    id,
    key_decimal_map,
    MAP_SIZE(key_decimal_map) as map_size,
    key_decimal_map[12345678901234567890123456789.1234567890] as value_for_key,
    MAP_KEYS(key_decimal_map) as decimal_keys
FROM decimal_map_test
WHERE MAP_SIZE(key_decimal_map) > 0
ORDER BY id;

-- =============================================================================
-- Part 3: STRUCT with decimal256 tests
-- =============================================================================

-- Create table with STRUCT containing decimal256 fields
CREATE TABLE decimal_struct_test (
    id INT,
    financial_data STRUCT<
        balance DECIMAL(50,15),
        credit_limit DECIMAL(50,15),
        interest_rate DECIMAL(40,10)
    >,
    large_numbers STRUCT<
        max_value DECIMAL(76,0),
        min_value DECIMAL(76,0),
        precision_value DECIMAL(76,38)
    >,
    account_info STRUCT<
        account_id BIGINT,
        balance DECIMAL(50,15),
        metadata STRUCT<
            created_date STRING,
            last_transaction DECIMAL(40,10)
        >
    >
) PROPERTIES("replication_num"="1");

-- Insert test data for structs - using decimal256 values beyond decimal128 range
INSERT INTO decimal_struct_test VALUES
(1, ROW(12345678901234567890123456789012345.123456789012345, 98765432109876543210987654321098765.000000000000000, 77777777777777777777777777777.5000000000),
    ROW(9999988888777776666655555444443333322222111110000099999888877776666555544, -8888877777666665555544444333332222211111000009999988888777766665555444433, 12345678901234567890123456.45678901234567890123456789012345678901),
    ROW(12345, 77777777777777777777777777777777777.750000000000000, ROW('2024-01-01', 55555555555555555555555555555.2500000000))),
(2, ROW(-44444444444444444444444444444444444.999999999999999, 66666666666666666666666666666666666.000000000000000, 33333333333333333333333333333.9999999999),
    ROW(1234567890123456789012345678901234567890123456789012345678901234567890123456, 0, -99988877766655544433322211100.99999999999999999999999999999999999999),
    ROW(67890, -22222222222222222222222222222222222.250000000000000, ROW('2024-02-15', -11111111111111111111111111111.7500000000))),
(3, ROW(0.000000000000001, 0.000000000000000, 0.0000000001),
    ROW(1, -1, 0.00000000000000000000000000000000000001),
    ROW(11111, 0.000000000000001, ROW('2024-03-30', 0.0100000000))),
(4, NULL,
    NULL,
    ROW(99999, 88888777776666655555444443333322222.999999999999999, ROW('2024-12-31', 77777666665555544444333332222.9999999999)));

-- Test 7: Basic struct field access
SELECT
    'Test7_STRUCT_FIELD_ACCESS' as test_name,
    id,
    financial_data,
    financial_data.balance as balance,
    financial_data.credit_limit as credit_limit,
    financial_data.interest_rate as interest_rate,
    financial_data.balance + financial_data.credit_limit as total_available
FROM decimal_struct_test
WHERE financial_data IS NOT NULL
ORDER BY id;

-- Test 8: Nested struct operations
SELECT
    'Test8_NESTED_STRUCT_OPERATIONS' as test_name,
    id,
    account_info,
    account_info.account_id as account_id,
    account_info.balance as account_balance,
    account_info.metadata.created_date as created_date,
    account_info.metadata.last_transaction as last_transaction,
    account_info.balance - account_info.metadata.last_transaction as net_balance
FROM decimal_struct_test
WHERE account_info IS NOT NULL
ORDER BY id;

-- Test 9: Large decimal256 values in structs
SELECT
    'Test9_LARGE_DECIMAL_STRUCTS' as test_name,
    id,
    large_numbers,
    large_numbers.max_value as max_val,
    large_numbers.min_value as min_val,
    large_numbers.precision_value as precision_val,
    CASE 
        WHEN large_numbers.max_value > 0 AND large_numbers.min_value < 0 THEN 'MIXED_RANGE'
        WHEN large_numbers.max_value > 0 THEN 'POSITIVE_RANGE'
        ELSE 'OTHER'
    END as range_type
FROM decimal_struct_test
WHERE large_numbers IS NOT NULL
ORDER BY id;

-- =============================================================================
-- Part 4: Complex nested structures
-- =============================================================================

-- Create table with nested complex types containing decimal256
CREATE TABLE complex_nested_test (
    id INT,
    portfolio ARRAY<STRUCT<
        asset_name STRING,
        quantity DECIMAL(50,15),
        price DECIMAL(50,15),
        metadata MAP<STRING, DECIMAL(40,10)>
    >>,
    risk_metrics MAP<STRING, ARRAY<DECIMAL(76,20)>>
) PROPERTIES("replication_num"="1");

-- Insert test data for complex nested structures - using decimal256 values beyond decimal128 range
INSERT INTO complex_nested_test VALUES
(1, [
        ROW('STOCK_A', 12345678901234567890123456789012345.500000000000000, 98765432109876543210987654321098765.750000000000000, MAP{'daily_change': 77777777777777777777777777777.2500000000, 'volume_weight': 33333333333333333333333333333.8500000000}),
        ROW('BOND_B', 55555555555555555555555555555555555.000000000000000, 44444444444444444444444444444444444.250000000000000, MAP{'yield': 11111111111111111111111111111.7500000000, 'duration': 22222222222222222222222222222.2500000000})
    ],
    MAP{
        'volatility': [12345678901234567890123456789012345678901234567890123456.15000000000000000000, 98765432109876543210987654321098765432109876543210987654.18000000000000000000, 55555555555555555555555555555555555555555555555555555555.12000000000000000000],
        'correlation': [77777777777777777777777777777777777777777777777777777777.65000000000000000000, -44444444444444444444444444444444444444444444444444444444.25000000000000000000, 88888888888888888888888888888888888888888888888888888888.85000000000000000000]
    }),
(2, [
        ROW('CRYPTO_C', 11111111111111111111111111111111111.001500000000000, 88888888888888888888888888888888888.999999999999999, MAP{'market_cap': 99999888887777766666555554444.9999999999, 'circulating': 77777666665555544444333332222.0000000000})
    ],
    MAP{
        'beta': [66666666666666666666666666666666666666666666666666666666.25000000000000000000, 33333333333333333333333333333333333333333333333333333333.35000000000000000000],
        'sharpe': [22222222222222222222222222222222222222222222222222222222.75000000000000000000, 11111111111111111111111111111111111111111111111111111111.95000000000000000000, 99999999999999999999999999999999999999999999999999999999.15000000000000000000]
    });

-- Test 10: Complex nested structure operations
SELECT
    'Test10_COMPLEX_NESTED_OPERATIONS' as test_name,
    id,
    CARDINALITY(portfolio) as portfolio_size,
    portfolio[1].asset_name as first_asset,
    portfolio[1].quantity as first_quantity,
    portfolio[1].price as first_price,
    portfolio[1].quantity * portfolio[1].price as first_total_value,
    portfolio[1].metadata['daily_change'] as first_daily_change
FROM complex_nested_test
ORDER BY id;

-- Test 11: Risk metrics analysis
SELECT
    'Test11_RISK_METRICS_ANALYSIS' as test_name,
    id,
    MAP_SIZE(risk_metrics) as metrics_count,
    CARDINALITY(risk_metrics['volatility']) as volatility_points,
    risk_metrics['volatility'][1] as first_volatility
FROM complex_nested_test
WHERE risk_metrics['volatility'] IS NOT NULL
ORDER BY id;

-- Test 12: Array element operations on nested structures
SELECT
    'Test12_ARRAY_ELEMENT_OPERATIONS' as test_name,
    id,
    portfolio[1].metadata as first_asset_metadata,
    MAP_KEYS(portfolio[1].metadata) as metadata_keys,
    MAP_VALUES(portfolio[1].metadata) as metadata_values
FROM complex_nested_test
WHERE CARDINALITY(portfolio) > 0
ORDER BY id;
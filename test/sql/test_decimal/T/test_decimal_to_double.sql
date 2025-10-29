-- name: test_decimal_to_double

DROP DATABASE IF EXISTS test_decimal_to_double;
CREATE DATABASE test_decimal_to_double;
USE test_decimal_to_double;

CREATE TABLE `t_decimal_precision_overflow` (
  `c_id` int(11) NOT NULL,
  `c_d64_max` decimal64(18,9) NOT NULL,
  `c_d128_large` decimal128(30,10) NOT NULL,
  `c_d128_max` decimal128(38,15) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c_id`)
DISTRIBUTED BY HASH(`c_id`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);

-- Insert test data that will definitely cause precision overflow when multiplied
-- decimal64(18,9) * decimal64(18,9) = precision 36, scale 18 (fits in decimal128)
-- decimal128(30,10) * decimal64(18,9) = precision 48, scale 19 (overflow!)
-- decimal128(38,15) * decimal128(38,15) = precision 76, scale 30 (overflow!)
INSERT INTO `t_decimal_precision_overflow` (c_id, c_d64_max, c_d128_large, c_d128_max) values
   (1, 123456789.123456789, 12345678901234567890.1234567890, 12345678901234567890123.123456789012345),
   (2, -123456789.123456789, -12345678901234567890.1234567890, -12345678901234567890123.123456789012345),
   (3, 987654321.987654321, 98765432109876543210.9876543210, 98765432109876543210987.987654321098765),
   (4, 111222333.444555666, 11122233344455566677.8899001122, 11122233344455566677889.900112233445566),
   (5, 555666777.888999111, 55566677788899911122.2333444555, 55566677788899911122233.344455566677888);

-- Test 1: decimal_overflow_to_double = false (default behavior)
set decimal_overflow_to_double = false;

-- These should NOT cause overflow (precision 36, scale 18 <= 38)
select c_d64_max * c_d64_max from t_decimal_precision_overflow where c_id = 1;
select c_d64_max * c_d64_max from t_decimal_precision_overflow where c_id = 2;
select c_d64_max * c_d64_max from t_decimal_precision_overflow where c_id = 3;
select c_d64_max * c_d64_max from t_decimal_precision_overflow where c_id = 4;
select c_d64_max * c_d64_max from t_decimal_precision_overflow where c_id = 5;

-- These SHOULD cause precision overflow (precision 48, scale 19) but use decimal128 truncation
select c_d128_large * c_d64_max from t_decimal_precision_overflow where c_id = 1;
select c_d128_large * c_d64_max from t_decimal_precision_overflow where c_id = 2;
select c_d128_large * c_d64_max from t_decimal_precision_overflow where c_id = 3;
select c_d128_large * c_d64_max from t_decimal_precision_overflow where c_id = 4;
select c_d128_large * c_d64_max from t_decimal_precision_overflow where c_id = 5;

-- These SHOULD cause major precision overflow (precision 76, scale 30) but use decimal128 truncation
select c_d128_max * c_d128_max from t_decimal_precision_overflow where c_id = 1;
select c_d128_max * c_d128_max from t_decimal_precision_overflow where c_id = 2;
select c_d128_max * c_d128_max from t_decimal_precision_overflow where c_id = 3;

-- Test literal multiplication that definitely causes precision overflow
-- precision 60, scale 18 -> overflow!
select 123456789012345678901234567890.123456789 * 987654321098765432109876543210.987654321;
-- precision 52, scale 20 -> overflow!
select 12345678901234567890123456789012.12345678901 * 98765432109876543210.12345678901;
-- precision 50, scale 16 -> overflow!
select 1234567890123456789012345678.12345678 * 9876543210987654321098765432.87654321;
-- precision 44, scale 12 -> overflow!
select 12345678901234567890123456.123456 * 98765432109876543210987654.876543;

-- Test 2: decimal_overflow_to_double = true (new behavior)
set decimal_overflow_to_double = true;

-- These should still return decimal128 (no overflow, precision 36, scale 18 <= 38)
select c_d64_max * c_d64_max from t_decimal_precision_overflow where c_id = 1;
select c_d64_max * c_d64_max from t_decimal_precision_overflow where c_id = 2;
select c_d64_max * c_d64_max from t_decimal_precision_overflow where c_id = 3;
select c_d64_max * c_d64_max from t_decimal_precision_overflow where c_id = 4;
select c_d64_max * c_d64_max from t_decimal_precision_overflow where c_id = 5;

-- These SHOULD now return double instead of decimal128 (precision 48 > 38, scale 19 <= 38)
select c_d128_large * c_d64_max from t_decimal_precision_overflow where c_id = 1;
select c_d128_large * c_d64_max from t_decimal_precision_overflow where c_id = 2;
select c_d128_large * c_d64_max from t_decimal_precision_overflow where c_id = 3;
select c_d128_large * c_d64_max from t_decimal_precision_overflow where c_id = 4;
select c_d128_large * c_d64_max from t_decimal_precision_overflow where c_id = 5;

-- These SHOULD now return double instead of decimal128 (precision 76 > 38, scale 30 <= 38)
select c_d128_max * c_d128_max from t_decimal_precision_overflow where c_id = 1;
select c_d128_max * c_d128_max from t_decimal_precision_overflow where c_id = 2;
select c_d128_max * c_d128_max from t_decimal_precision_overflow where c_id = 3;

-- Test literal multiplication that causes precision overflow - should return double
-- precision 60 > 38, scale 18 <= 38 -> should convert to double
select 123456789012345678901234567890.123456789 * 987654321098765432109876543210.987654321;
-- precision 52 > 38, scale 20 <= 38 -> should convert to double
select 12345678901234567890123456789012.12345678901 * 98765432109876543210.12345678901;
-- precision 50 > 38, scale 16 <= 38 -> should convert to double
select 1234567890123456789012345678.12345678 * 9876543210987654321098765432.87654321;
-- precision 44 > 38, scale 12 <= 38 -> should convert to double
select 12345678901234567890123456.123456 * 98765432109876543210987654.876543;

set decimal_overflow_to_double = false;
select 1234567890123456789.1234567890123456789 * 1.0000000000000000000;

set decimal_overflow_to_double = true;
select 1234567890123456789.1234567890123456789 * 1.0000000000000000000;

set decimal_overflow_to_double = false;
select 12345678901234567890123456789012345678.0 * 1.0;

set decimal_overflow_to_double = true;
select 12345678901234567890123456789012345678.0 * 1.1;

-- Test 4: Mixed operations
set decimal_overflow_to_double = true;
-- Test multiplication in expressions - these should convert to double due to overflow
select c_id, c_d128_large * c_d64_max + 1.0 from t_decimal_precision_overflow where c_id <= 2 order by 1;
select c_id, (c_d128_max * c_d128_max) / 2.0 from t_decimal_precision_overflow where c_id <= 2 order by 1;

-- Test 6: Additional extreme overflow cases
set decimal_overflow_to_double = false;
-- Extreme case: very large precision overflow (should use decimal128 truncation)
select 12345678901234567890123456789012345678.0 * 98765432109876543210987654321098765432.0;

set decimal_overflow_to_double = true;
-- Same extreme case: should convert to double
select 12345678901234567890123456789012345678.0 * 98765432109876543210987654321098765432.0;

-- Test 7: Verify behavior with different scale combinations
set decimal_overflow_to_double = false;
-- High precision, low scale (should use decimal128 truncation)
select 123456789012345678901234567890123456.78 * 987654321098765432109876543210987654.32;

set decimal_overflow_to_double = true;
-- Same case: should convert to double (precision 72 > 38, scale 4 <= 38)
select 123456789012345678901234567890123456.78 * 987654321098765432109876543210987654.32;

-- Test 8: More diverse precision overflow cases
set decimal_overflow_to_double = false;
-- Medium precision overflow
select 1111222233334444555566667777.8888 * 8888777766665555444433332222.1111;

set decimal_overflow_to_double = true;
-- Same case with session variable enabled
select 1111222233334444555566667777.8888 * 8888777766665555444433332222.1111;

-- Different scale patterns
set decimal_overflow_to_double = false;
select 123456789012345678901234567890.123456 * 987654321098765432109876543210.654321;

set decimal_overflow_to_double = true;
select 123456789012345678901234567890.123456 * 987654321098765432109876543210.654321;

SELECT(0.58000000 * 0.970825897017235893 * 3021621.785498);

set decimal_overflow_to_double = false;

SELECT(0.58000000 * 0.970825897017235893 * 3021621.785498);

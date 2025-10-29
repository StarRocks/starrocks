-- name: test_decimal_to_double
DROP DATABASE IF EXISTS test_decimal_to_double;
-- result:
-- !result
CREATE DATABASE test_decimal_to_double;
-- result:
-- !result
USE test_decimal_to_double;
-- result:
-- !result
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
-- result:
-- !result
INSERT INTO `t_decimal_precision_overflow` (c_id, c_d64_max, c_d128_large, c_d128_max) values
   (1, 123456789.123456789, 12345678901234567890.1234567890, 12345678901234567890123.123456789012345),
   (2, -123456789.123456789, -12345678901234567890.1234567890, -12345678901234567890123.123456789012345),
   (3, 987654321.987654321, 98765432109876543210.9876543210, 98765432109876543210987.987654321098765),
   (4, 111222333.444555666, 11122233344455566677.8899001122, 11122233344455566677889.900112233445566),
   (5, 555666777.888999111, 55566677788899911122.2333444555, 55566677788899911122233.344455566677888);
-- result:
-- !result
set decimal_overflow_to_double = false;
-- result:
-- !result
select c_d64_max * c_d64_max from t_decimal_precision_overflow where c_id = 1;
-- result:
15241578780673678.515622620750190521
-- !result
select c_d64_max * c_d64_max from t_decimal_precision_overflow where c_id = 2;
-- result:
15241578780673678.515622620750190521
-- !result
select c_d64_max * c_d64_max from t_decimal_precision_overflow where c_id = 3;
-- result:
975461059740893157.555403139789971041
-- !result
select c_d64_max * c_d64_max from t_decimal_precision_overflow where c_id = 4;
-- result:
12370407456851925.839407296172703556
-- !result
select c_d64_max * c_d64_max from t_decimal_precision_overflow where c_id = 5;
-- result:
308765568049542271.320789913358790321
-- !result
select c_d128_large * c_d64_max from t_decimal_precision_overflow where c_id = 1;
-- result:
None
-- !result
select c_d128_large * c_d64_max from t_decimal_precision_overflow where c_id = 2;
-- result:
None
-- !result
select c_d128_large * c_d64_max from t_decimal_precision_overflow where c_id = 3;
-- result:
None
-- !result
select c_d128_large * c_d64_max from t_decimal_precision_overflow where c_id = 4;
-- result:
None
-- !result
select c_d128_large * c_d64_max from t_decimal_precision_overflow where c_id = 5;
-- result:
None
-- !result
select c_d128_max * c_d128_max from t_decimal_precision_overflow where c_id = 1;
-- result:
None
-- !result
select c_d128_max * c_d128_max from t_decimal_precision_overflow where c_id = 2;
-- result:
None
-- !result
select c_d128_max * c_d128_max from t_decimal_precision_overflow where c_id = 3;
-- result:
None
-- !result
select 123456789012345678901234567890.123456789 * 987654321098765432109876543210.987654321;
-- result:
None
-- !result
select 12345678901234567890123456789012.12345678901 * 98765432109876543210.12345678901;
-- result:
None
-- !result
select 1234567890123456789012345678.12345678 * 9876543210987654321098765432.87654321;
-- result:
None
-- !result
select 12345678901234567890123456.123456 * 98765432109876543210987654.876543;
-- result:
None
-- !result
set decimal_overflow_to_double = true;
-- result:
-- !result
select c_d64_max * c_d64_max from t_decimal_precision_overflow where c_id = 1;
-- result:
15241578780673678.515622620750190521
-- !result
select c_d64_max * c_d64_max from t_decimal_precision_overflow where c_id = 2;
-- result:
15241578780673678.515622620750190521
-- !result
select c_d64_max * c_d64_max from t_decimal_precision_overflow where c_id = 3;
-- result:
975461059740893157.555403139789971041
-- !result
select c_d64_max * c_d64_max from t_decimal_precision_overflow where c_id = 4;
-- result:
12370407456851925.839407296172703556
-- !result
select c_d64_max * c_d64_max from t_decimal_precision_overflow where c_id = 5;
-- result:
308765568049542271.320789913358790321
-- !result
select c_d128_large * c_d64_max from t_decimal_precision_overflow where c_id = 1;
-- result:
1.5241578766956256e+27
-- !result
select c_d128_large * c_d64_max from t_decimal_precision_overflow where c_id = 2;
-- result:
1.5241578766956256e+27
-- !result
select c_d128_large * c_d64_max from t_decimal_precision_overflow where c_id = 3;
-- result:
9.754610588629782e+28
-- !result
select c_d128_large * c_d64_max from t_decimal_precision_overflow where c_id = 4;
-- result:
1.2370407456851927e+27
-- !result
select c_d128_large * c_d64_max from t_decimal_precision_overflow where c_id = 5;
-- result:
3.087655680495423e+28
-- !result
select c_d128_max * c_d128_max from t_decimal_precision_overflow where c_id = 1;
-- result:
1.5241578753238837e+44
-- !result
select c_d128_max * c_d128_max from t_decimal_precision_overflow where c_id = 2;
-- result:
1.5241578753238837e+44
-- !result
select c_d128_max * c_d128_max from t_decimal_precision_overflow where c_id = 3;
-- result:
9.754610579850631e+45
-- !result
select 123456789012345678901234567890.123456789 * 987654321098765432109876543210.987654321;
-- result:
1.2193263113702178e+59
-- !result
select 12345678901234567890123456789012.12345678901 * 98765432109876543210.12345678901;
-- result:
1.2193263113702178e+51
-- !result
select 1234567890123456789012345678.12345678 * 9876543210987654321098765432.87654321;
-- result:
1.219326311370218e+55
-- !result
select 12345678901234567890123456.123456 * 98765432109876543210987654.876543;
-- result:
1.2193263113702181e+51
-- !result
set decimal_overflow_to_double = false;
-- result:
-- !result
select 1234567890123456789.1234567890123456789 * 1.0000000000000000000;
-- result:
None
-- !result
set decimal_overflow_to_double = true;
-- result:
-- !result
select 1234567890123456789.1234567890123456789 * 1.0000000000000000000;
-- result:
1.2345678901234568e+18
-- !result
set decimal_overflow_to_double = false;
-- result:
-- !result
select 12345678901234567890123456789012345678.0 * 1.0;
-- result:
12345678901234567890123456789012345678.0
-- !result
set decimal_overflow_to_double = true;
-- result:
-- !result
select 12345678901234567890123456789012345678.0 * 1.1;
-- result:
1.3580246791358026e+37
-- !result
set decimal_overflow_to_double = true;
-- result:
-- !result
select c_id, c_d128_large * c_d64_max + 1.0 from t_decimal_precision_overflow where c_id <= 2 order by 1;
-- result:
1	1.5241578766956256e+27
2	1.5241578766956256e+27
-- !result
select c_id, (c_d128_max * c_d128_max) / 2.0 from t_decimal_precision_overflow where c_id <= 2 order by 1;
-- result:
1	7.620789376619419e+43
2	7.620789376619419e+43
-- !result
set decimal_overflow_to_double = false;
-- result:
-- !result
select 12345678901234567890123456789012345678.0 * 98765432109876543210987654321098765432.0;
-- result:
None
-- !result
set decimal_overflow_to_double = true;
-- result:
-- !result
select 12345678901234567890123456789012345678.0 * 98765432109876543210987654321098765432.0;
-- result:
1.2193263113702179e+75
-- !result
set decimal_overflow_to_double = false;
-- result:
-- !result
select 123456789012345678901234567890123456.78 * 987654321098765432109876543210987654.32;
-- result:
None
-- !result
set decimal_overflow_to_double = true;
-- result:
-- !result
select 123456789012345678901234567890123456.78 * 987654321098765432109876543210987654.32;
-- result:
1.219326311370218e+71
-- !result
set decimal_overflow_to_double = false;
-- result:
-- !result
select 1111222233334444555566667777.8888 * 8888777766665555444433332222.1111;
-- result:
None
-- !result
set decimal_overflow_to_double = true;
-- result:
-- !result
select 1111222233334444555566667777.8888 * 8888777766665555444433332222.1111;
-- result:
9.877407481487654e+54
-- !result
set decimal_overflow_to_double = false;
-- result:
-- !result
select 123456789012345678901234567890.123456 * 987654321098765432109876543210.654321;
-- result:
None
-- !result
set decimal_overflow_to_double = true;
-- result:
-- !result
select 123456789012345678901234567890.123456 * 987654321098765432109876543210.654321;
-- result:
1.2193263113702178e+59
-- !result
SELECT(0.58000000 * 0.970825897017235893 * 3021621.785498);
-- result:
1701411.8346046924
-- !result
set decimal_overflow_to_double = false;
-- result:
-- !result
SELECT(0.58000000 * 0.970825897017235893 * 3021621.785498);
-- result:
None
-- !result
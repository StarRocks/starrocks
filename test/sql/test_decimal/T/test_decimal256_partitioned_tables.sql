-- name: test_decimal256_partitioned_tables
DROP DATABASE IF EXISTS test_decimal256_partition;
CREATE DATABASE test_decimal256_partition;
USE test_decimal256_partition;

-- =============================================================================
-- Part 1: Range partitioned table with decimal256
-- =============================================================================

-- Create range partitioned table using decimal256 column
CREATE TABLE decimal_range_partition (
    id BIGINT,
    transaction_date DATE,
    amount DECIMAL(50,15),
    balance DECIMAL(76,20),
    account_type VARCHAR(20)
)
DUPLICATE KEY(id, transaction_date)
PARTITION BY RANGE(transaction_date) (
    PARTITION p202401 VALUES [('2024-01-01'), ('2024-02-01')),
    PARTITION p202402 VALUES [('2024-02-01'), ('2024-03-01')),
    PARTITION p202403 VALUES [('2024-03-01'), ('2024-04-01')),
    PARTITION p202404 VALUES [('2024-04-01'), ('2024-05-01'))
)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);

-- Insert test data across different partitions - using decimal256 values beyond decimal128 range
INSERT INTO decimal_range_partition VALUES
-- Partition p202401
(1, '2024-01-15', 12345678901234567890123456789012345.123456789012345, 12345678901234567890123456789012345678901234567890123456.12345678901234567890, 'SAVINGS'),
(2, '2024-01-20', -55555555555555555555555555555555555.987654321098765, 55555555555555555555555555555555555555555555555555555555.13579246801357924680, 'CHECKING'),
(3, '2024-01-25', 98765432109876543210987654321098765.555555555555555, 98765432109876543210987654321098765432109876543210987654.99999999999999999999, 'INVESTMENT'),

-- Partition p202402  
(4, '2024-02-10', 77777777777777777777777777777777777.777777777777777, 77777777777777777777777777777777777777777777777777777777.77777777777777777777, 'SAVINGS'),
(5, '2024-02-15', -44444444444444444444444444444444444.111111111111111, 44444444444444444444444444444444444444444444444444444444.66666666666666666666, 'CHECKING'),
(6, '2024-02-28', 88888888888888888888888888888888888.999999999999999, 88888888888888888888888888888888888888888888888888888888.65432109876543210987, 'INVESTMENT'),

-- Partition p202403
(7, '2024-03-05', 11111111111111111111111111111111111.250000000000000, 11111111111111111111111111111111111111111111111111111111.90400000000000000000, 'SAVINGS'),
(8, '2024-03-12', -22222222222222222222222222222222222.125000000000000, 22222222222222222222222222222222222222222222222222222222.77900000000000000000, 'CHECKING'),
(9, '2024-03-25', 99999888887777766666555554444433333.000000000000001, 99999888887777766666555554444433333222221111100009999988.77900000000000000001, 'INVESTMENT'),

-- Partition p202404
(10, '2024-04-08', 33333333333333333333333333333333333.888888888888888, 33333333333333333333333333333333333333333333333333333333.66788888888888888889, 'SAVINGS'),
(11, '2024-04-15', -66666666666666666666666666666666666.333333333333333, 66666666666666666666666666666666666666666666666666666666.33455555555555555556, 'CHECKING'),
(12, '2024-04-30', 77777666665555544444333332222211111.000000000000000, 77777666665555544444333332222211111000009999988888777776.33455555555555555556, 'INVESTMENT');

-- Test 1: Basic partition-wise queries
SELECT
    'Test1_PARTITION_BASIC_QUERY' as test_name,
    DATE_FORMAT(transaction_date, '%Y-%m') as month,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(balance) as avg_balance,
    MAX(amount) as max_amount,
    MIN(amount) as min_amount
FROM decimal_range_partition
GROUP BY DATE_FORMAT(transaction_date, '%Y-%m')
ORDER BY month;

-- Test 2: Partition pruning test - single partition
SELECT
    'Test2_SINGLE_PARTITION_QUERY' as test_name,
    id,
    transaction_date,
    amount,
    balance,
    account_type
FROM decimal_range_partition
WHERE transaction_date >= '2024-02-01' AND transaction_date < '2024-03-01'
ORDER BY transaction_date;

-- Test 3: Cross-partition aggregation by account type
SELECT
    'Test3_CROSS_PARTITION_AGGREGATION' as test_name,
    account_type,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as total_deposits,
    SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) as total_withdrawals
FROM decimal_range_partition
GROUP BY account_type
ORDER BY account_type;

-- =============================================================================
-- Part 2: List partitioned table with decimal256 ranges
-- =============================================================================

-- Create list partitioned table using decimal256 ranges
CREATE TABLE decimal_amount_partition (
    id BIGINT,
    customer_id INT,
    amount DECIMAL(50,15),
    large_amount DECIMAL(76,0),
    category VARCHAR(20),
    amount_range STRING
)
DUPLICATE KEY(id, customer_id)
PARTITION BY LIST(amount_range) (
    PARTITION p_small VALUES IN ('SMALL'),
    PARTITION p_medium VALUES IN ('MEDIUM'),
    PARTITION p_large VALUES IN ('LARGE'),
    PARTITION p_xlarge VALUES IN ('XLARGE')
)
DISTRIBUTED BY HASH(customer_id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);

-- Insert test data with different amount ranges - using decimal256 values beyond decimal128 range
INSERT INTO decimal_amount_partition VALUES
-- Small amounts (< 40 digits)
(1, 101, 12345678901234567890123456789012345.123456789012345, 12345678901234567, 'RETAIL', 'SMALL'),
(2, 102, 98765432109876543210987654321098765.987654321098765, 98765432109876543, 'ONLINE', 'SMALL'),
(3, 103, 55555555555555555555555555555555555.999999999999999, 55555555555555555, 'RETAIL', 'SMALL'),

-- Medium amounts (40-50 digits) 
(4, 201, 1234567890123456789012345678901234.555555555555, 1234567890123456789012345678901234567890123456789012345678901234567890, 'WHOLESALE', 'MEDIUM'),
(5, 202, 9876543210987654321098765432109876.777777777777, 9876543210987654321098765432109876543210987654321098765432109876543210, 'ENTERPRISE', 'MEDIUM'),
(6, 203, 5555555555555555555555555555555555.888888888888, 5555555555555555555555555555555555555555555555555555555555555555555555, 'WHOLESALE', 'MEDIUM'),

-- Large amounts (50-60 digits)
(7, 301, 12345678901234567890123456789012.123456789012345, 123456789012345, 'ENTERPRISE', 'LARGE'),
(8, 302, 98765432109876543210987654321098.999999999999999, 987654321098765, 'GOVERNMENT', 'LARGE'),
(9, 303, 77777777777777777777777777777777.111111111111111, 777777777777777, 'ENTERPRISE', 'LARGE');


-- Test 4: List partition aggregation
SELECT
    'Test4_LIST_PARTITION_AGGREGATION' as test_name,
    amount_range,
    category,
    COUNT(*) as count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    MIN(amount) as min_amount,
    MAX(amount) as max_amount
FROM decimal_amount_partition
GROUP BY amount_range, category
ORDER BY amount_range, category;

-- Test 5: Partition pruning with specific range
SELECT
    'Test5_PARTITION_PRUNING_SPECIFIC' as test_name,
    id,
    customer_id,
    amount,
    large_amount,
    category
FROM decimal_amount_partition
WHERE amount_range = 'LARGE'
ORDER BY amount DESC;

-- =============================================================================
-- Part 3: Partition maintenance operations
-- =============================================================================

-- Test 6: Add new partition with decimal256 data
ALTER TABLE decimal_range_partition 
ADD PARTITION p202405 VALUES [('2024-05-01'), ('2024-06-01'));

-- Insert data into new partition - using decimal256 values beyond decimal128 range
INSERT INTO decimal_range_partition VALUES
(13, '2024-05-10', 888887777766666555554444433.123456789012345, 8888877777666665555544444333332222211111.45800234567890123456, 'SAVINGS'),
(14, '2024-05-20', -11111222223333344444555556.987654321098765, 1111122222333334444455555666667777788888.47034913469124801235, 'CHECKING'),
(15, '2024-05-31', 999999999999999999999999999.000000000000000, 9999999999999999999999999999999999999999.47034913469124801235, 'INVESTMENT');

-- Test 7: Query across all partitions including new one
SELECT
    'Test7_ALL_PARTITIONS_INCLUDING_NEW' as test_name,
    DATE_FORMAT(transaction_date, '%Y-%m') as month,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(balance) as avg_balance
FROM decimal_range_partition
GROUP BY DATE_FORMAT(transaction_date, '%Y-%m')
ORDER BY month;

-- Test 8: Partition-wise window functions
SELECT
    'Test8_PARTITION_WINDOW_FUNCTIONS' as test_name,
    id,
    transaction_date,
    amount,
    balance,
    account_type,
    ROW_NUMBER() OVER (PARTITION BY DATE_FORMAT(transaction_date, '%Y-%m') ORDER BY amount DESC) as rank_in_month,
    SUM(amount) OVER (PARTITION BY account_type ORDER BY transaction_date) as running_total_by_type,
    LAG(balance, 1) OVER (PARTITION BY account_type ORDER BY transaction_date) as prev_balance
FROM decimal_range_partition
ORDER BY transaction_date, account_type;

-- Test 9: Cross-partition joins
WITH monthly_stats AS (
    SELECT
        DATE_FORMAT(transaction_date, '%Y-%m') as month,
        account_type,
        SUM(amount) as monthly_total,
        COUNT(*) as monthly_count
    FROM decimal_range_partition
    GROUP BY DATE_FORMAT(transaction_date, '%Y-%m'), account_type
)
SELECT
    'Test9_CROSS_PARTITION_JOINS' as test_name,
    dp.transaction_date,
    dp.amount,
    dp.account_type,
    ms.monthly_total,
    dp.amount / ms.monthly_total * 100 as percent_of_monthly_total
FROM decimal_range_partition dp
JOIN monthly_stats ms ON DATE_FORMAT(dp.transaction_date, '%Y-%m') = ms.month 
    AND dp.account_type = ms.account_type
WHERE ABS(dp.amount) > 1000.000000000000000
ORDER BY dp.transaction_date;

-- Test 10: Partition information query
SELECT
    'Test10_PARTITION_INFORMATION' as test_name,
    DATE_FORMAT(transaction_date, '%Y-%m') as partition_month,
    COUNT(*) as row_count,
    SUM(CASE WHEN amount > 0 THEN 1 ELSE 0 END) as positive_transactions,
    SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) as negative_transactions,
    MIN(balance) as min_balance_in_partition,
    MAX(balance) as max_balance_in_partition
FROM decimal_range_partition
GROUP BY DATE_FORMAT(transaction_date, '%Y-%m')
ORDER BY partition_month;

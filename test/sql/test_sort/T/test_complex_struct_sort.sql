-- name: test_complex_struct_sort

drop database if exists test_complex_struct_sort;
create database test_complex_struct_sort;
use test_complex_struct_sort;

CREATE TABLE user_behavior (
    id BIGINT,
    user_profile STRUCT<
        user_id INT,
        user_name STRING,
        user_level INT,
        registration_date DATE,
        location STRUCT<
            country STRING,
            city STRING,
            coordinates STRUCT<
                latitude DOUBLE,
                longitude DOUBLE
            >
        >,
        preferences STRUCT<
            category STRING,
            sub_category STRING,
            score INT
        >
    >,
    event_timestamp DATETIME
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

INSERT INTO user_behavior
SELECT
    generate_series AS id,
    row(
        CAST(generate_series % 1000000 AS INT),
        CONCAT('user_', CAST(generate_series % 1000000 AS STRING)),
        CAST(generate_series % 10 AS INT),
        DATE_ADD('2020-01-01', INTERVAL (generate_series % 1460) DAY),
        row(
            CASE generate_series % 10
                WHEN 0 THEN 'USA'
                WHEN 1 THEN 'China'
                WHEN 2 THEN 'UK'
                WHEN 3 THEN 'Japan'
                WHEN 4 THEN 'Germany'
                WHEN 5 THEN 'France'
                WHEN 6 THEN 'Canada'
                WHEN 7 THEN 'Australia'
                WHEN 8 THEN 'India'
                ELSE 'Brazil'
            END,
            CONCAT('City_', CAST(generate_series % 100 AS STRING)),
            row(
                CAST(20.0 + (generate_series % 60) AS DOUBLE),
                CAST(-180.0 + (generate_series % 360) AS DOUBLE)
            )
        ),
        row(
            CASE generate_series % 5
                WHEN 0 THEN 'Electronics'
                WHEN 1 THEN 'Books'
                WHEN 2 THEN 'Clothing'
                WHEN 3 THEN 'Food'
                ELSE 'Sports'
            END,
            CONCAT('Sub_', CAST(generate_series % 20 AS STRING)),
            CAST(generate_series % 100 AS INT)
        )
    ) AS user_profile,
    date_add(CAST('2024-01-01 00:00:00' AS DATETIME), INTERVAL (generate_series % 86400) SECOND) AS event_timestamp
FROM TABLE(generate_series(1, 1000000));

select user_profile from user_behavior order by user_profile limit 10;

SELECT user_profile
FROM (
    SELECT user_profile
    FROM user_behavior
    ORDER BY user_profile
    LIMIT 1025
) t
WHERE user_profile.user_id = 1;

SELECT COUNT(a.c1), COUNT(a.c2)
FROM (
    SELECT
        id as  c1,
        row_number() OVER (ORDER BY user_profile) AS c2
    FROM user_behavior
) AS a;

-- ==========================================
-- Test: Complex Nested Transaction Structure
-- Card + Transaction with deep nesting
-- ==========================================

CREATE TABLE transaction_data (
    id BIGINT,
    transaction_info STRUCT<
        card STRUCT<
            card_details STRUCT<
                card_bin STRING,
                card_type STRING,
                issuer STRING,
                card_program STRING
            >,
            card_tags ARRAY<STRUCT<value STRING>>,
            version INT
        >,
        transaction STRUCT<
            transaction_id STRING,
            transaction_details STRUCT<
                transaction_type STRING,
                merchant_data STRUCT<
                    merchant_id STRING,
                    merchant_name STRING,
                    merchant_country_code STRING
                >,
                posting_date STRING,
                amount STRUCT<
                    currency_code STRING
                >,
                billing_amount STRUCT<
                    currency_code STRING
                >,
                billing_amount_no_fee DECIMAL(18, 4),
                conversion_rate DECIMAL(18, 4),
                ird STRING
            >
        >
    >
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- Insert test data with various card and transaction combinations
INSERT INTO transaction_data VALUES
(1, row(
    row(
        row('411111', 'VISA', 'Chase', 'Rewards'),
        [row('Premium'), row('Verified')],
        1
    ),
    row(
        'TXN001',
        row(
            'PURCHASE',
            row('MERCH001', 'Amazon', 'US'),
            '2024-01-15',
            row('USD'),
            row('USD'),
            100.5000,
            1.0000,
            'IRD001'
        )
    )
)),
(2, row(
    row(
        row('520000', 'MASTERCARD', 'Citi', 'CashBack'),
        [row('Standard')],
        2
    ),
    row(
        'TXN002',
        row(
            'REFUND',
            row('MERCH002', 'Walmart', 'US'),
            '2024-01-16',
            row('USD'),
            row('USD'),
            50.2500,
            1.0000,
            'IRD002'
        )
    )
)),
(3, row(
    row(
        row('370000', 'AMEX', 'American Express', 'Platinum'),
        [row('Premium'), row('Travel'), row('Insurance')],
        1
    ),
    row(
        'TXN003',
        row(
            'PURCHASE',
            row('MERCH003', 'Apple Store', 'US'),
            '2024-01-17',
            row('USD'),
            row('EUR'),
            1200.0000,
            0.9200,
            'IRD003'
        )
    )
)),
(4, row(
    row(
        row('411111', 'VISA', 'Bank of America', 'Travel'),
        [row('Gold')],
        1
    ),
    row(
        'TXN004',
        row(
            'PURCHASE',
            row('MERCH001', 'Amazon', 'UK'),
            '2024-01-18',
            row('GBP'),
            row('USD'),
            85.7500,
            1.2700,
            'IRD004'
        )
    )
)),
(5, row(
    row(
        row('520000', 'MASTERCARD', 'HSBC', 'Standard'),
        [row('Basic')],
        2
    ),
    row(
        'TXN005',
        row(
            'WITHDRAWAL',
            row('MERCH004', 'ATM Network', 'CN'),
            '2024-01-19',
            row('CNY'),
            row('USD'),
            500.0000,
            0.1400,
            'IRD005'
        )
    )
)),
(6, row(
    row(
        row('370000', 'AMEX', 'American Express', 'Gold'),
        [row('Premium'), row('Concierge')],
        3
    ),
    row(
        'TXN006',
        row(
            'PURCHASE',
            row('MERCH005', 'Luxury Hotel', 'FR'),
            '2024-01-20',
            row('EUR'),
            row('USD'),
            2500.0000,
            1.0800,
            'IRD006'
        )
    )
)),
(7, row(
    row(
        row('411111', 'VISA', 'Chase', 'Rewards'),
        [row('Verified')],
        1
    ),
    row(
        'TXN007',
        row(
            'PURCHASE',
            row('MERCH001', 'Amazon', 'US'),
            '2024-01-21',
            row('USD'),
            row('USD'),
            45.9900,
            1.0000,
            'IRD007'
        )
    )
)),
(8, row(
    row(
        row('520000', 'MASTERCARD', 'Citi', 'CashBack'),
        [row('Standard'), row('Contactless')],
        2
    ),
    row(
        'TXN008',
        row(
            'PURCHASE',
            row('MERCH006', 'Grocery Store', 'CA'),
            '2024-01-22',
            row('CAD'),
            row('USD'),
            75.0000,
            0.7500,
            'IRD008'
        )
    )
));

-- Test 1: Basic ORDER BY on complex nested structure
SELECT transaction_info 
FROM transaction_data 
ORDER BY transaction_info 
LIMIT 5;

-- Test 2: ORDER BY DESC
SELECT transaction_info 
FROM transaction_data 
ORDER BY transaction_info DESC 
LIMIT 5;

-- Test 3: TopN with window function to avoid large output
SELECT id, rn
FROM (
    SELECT 
        id,
        ROW_NUMBER() OVER (ORDER BY transaction_info) AS rn
    FROM transaction_data
) t
WHERE rn <= 5;

-- Test 4: ORDER BY with WHERE clause filtering
SELECT transaction_info 
FROM transaction_data 
WHERE id <= 5
ORDER BY transaction_info;

-- Test 5: Verify specific ordering (card type comparison)
-- AMEX vs MASTERCARD vs VISA (lexicographical order)
SELECT 
    id,
    transaction_info.card.card_details.card_type,
    transaction_info.card.card_details.issuer
FROM transaction_data 
ORDER BY transaction_info 
LIMIT 5;

-- Test 6: ORDER BY with DISTINCT
SELECT DISTINCT transaction_info.card.card_details.card_type
FROM transaction_data
ORDER BY transaction_info.card.card_details.card_type;

-- Test 7: Complex struct in subquery
SELECT id, transaction_info
FROM (
    SELECT * FROM transaction_data WHERE id <= 6
) t
ORDER BY transaction_info
LIMIT 3;

-- Test 8: TopN with specific row (only return row 5)
SELECT id, transaction_info
FROM (
    SELECT 
        id,
        transaction_info,
        ROW_NUMBER() OVER (ORDER BY transaction_info) AS rn
    FROM transaction_data
) t
WHERE rn = 5;

-- Test 9: ORDER BY with COUNT aggregation (performance test)
SELECT COUNT(*)
FROM (
    SELECT transaction_info 
    FROM transaction_data 
    ORDER BY transaction_info 
    LIMIT 8
) t;

-- Test 10: Multiple STRUCT columns ordering
SELECT 
    id,
    transaction_info.card.version,
    transaction_info.transaction.transaction_id
FROM transaction_data
ORDER BY transaction_info, id;

-- Clean up
DROP TABLE transaction_data FORCE;

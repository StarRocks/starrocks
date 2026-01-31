-- name: test_complex_struct_sort
drop database if exists test_complex_struct_sort;
-- result:
-- !result
create database test_complex_struct_sort;
-- result:
-- !result
use test_complex_struct_sort;
-- result:
-- !result
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
-- result:
-- !result
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
-- result:
-- !result
select user_profile from user_behavior order by user_profile limit 10;
-- result:
{"user_id":0,"user_name":"user_0","user_level":0,"registration_date":"2023-09-22","location":{"country":"USA","city":"City_0","coordinates":{"latitude":60,"longitude":100}},"preferences":{"category":"Electronics","sub_category":"Sub_0","score":0}}
{"user_id":1,"user_name":"user_1","user_level":1,"registration_date":"2020-01-02","location":{"country":"China","city":"City_1","coordinates":{"latitude":21,"longitude":-179}},"preferences":{"category":"Books","sub_category":"Sub_1","score":1}}
{"user_id":2,"user_name":"user_2","user_level":2,"registration_date":"2020-01-03","location":{"country":"UK","city":"City_2","coordinates":{"latitude":22,"longitude":-178}},"preferences":{"category":"Clothing","sub_category":"Sub_2","score":2}}
{"user_id":3,"user_name":"user_3","user_level":3,"registration_date":"2020-01-04","location":{"country":"Japan","city":"City_3","coordinates":{"latitude":23,"longitude":-177}},"preferences":{"category":"Food","sub_category":"Sub_3","score":3}}
{"user_id":4,"user_name":"user_4","user_level":4,"registration_date":"2020-01-05","location":{"country":"Germany","city":"City_4","coordinates":{"latitude":24,"longitude":-176}},"preferences":{"category":"Sports","sub_category":"Sub_4","score":4}}
{"user_id":5,"user_name":"user_5","user_level":5,"registration_date":"2020-01-06","location":{"country":"France","city":"City_5","coordinates":{"latitude":25,"longitude":-175}},"preferences":{"category":"Electronics","sub_category":"Sub_5","score":5}}
{"user_id":6,"user_name":"user_6","user_level":6,"registration_date":"2020-01-07","location":{"country":"Canada","city":"City_6","coordinates":{"latitude":26,"longitude":-174}},"preferences":{"category":"Books","sub_category":"Sub_6","score":6}}
{"user_id":7,"user_name":"user_7","user_level":7,"registration_date":"2020-01-08","location":{"country":"Australia","city":"City_7","coordinates":{"latitude":27,"longitude":-173}},"preferences":{"category":"Clothing","sub_category":"Sub_7","score":7}}
{"user_id":8,"user_name":"user_8","user_level":8,"registration_date":"2020-01-09","location":{"country":"India","city":"City_8","coordinates":{"latitude":28,"longitude":-172}},"preferences":{"category":"Food","sub_category":"Sub_8","score":8}}
{"user_id":9,"user_name":"user_9","user_level":9,"registration_date":"2020-01-10","location":{"country":"Brazil","city":"City_9","coordinates":{"latitude":29,"longitude":-171}},"preferences":{"category":"Sports","sub_category":"Sub_9","score":9}}
-- !result
SELECT user_profile
FROM (
    SELECT user_profile
    FROM user_behavior
    ORDER BY user_profile
    LIMIT 1025
) t
WHERE user_profile.user_id = 1;
-- result:
{"user_id":1,"user_name":"user_1","user_level":1,"registration_date":"2020-01-02","location":{"country":"China","city":"City_1","coordinates":{"latitude":21,"longitude":-179}},"preferences":{"category":"Books","sub_category":"Sub_1","score":1}}
-- !result
SELECT COUNT(a.c1), COUNT(a.c2)
FROM (
    SELECT
        id as  c1,
        row_number() OVER (ORDER BY user_profile) AS c2
    FROM user_behavior
) AS a;
-- result:
1000000	1000000
-- !result
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
-- result:
-- !result
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
-- result:
-- !result
SELECT transaction_info 
FROM transaction_data 
ORDER BY transaction_info 
LIMIT 5;
-- result:
{"card":{"card_details":{"card_bin":"370000","card_type":"AMEX","issuer":"American Express","card_program":"Gold"},"card_tags":[{"value":"Premium"},{"value":"Concierge"}],"version":3},"transaction":{"transaction_id":"TXN006","transaction_details":{"transaction_type":"PURCHASE","merchant_data":{"merchant_id":"MERCH005","merchant_name":"Luxury Hotel","merchant_country_code":"FR"},"posting_date":"2024-01-20","amount":{"currency_code":"EUR"},"billing_amount":{"currency_code":"USD"},"billing_amount_no_fee":2500.0000,"conversion_rate":1.0800,"ird":"IRD006"}}}
{"card":{"card_details":{"card_bin":"370000","card_type":"AMEX","issuer":"American Express","card_program":"Platinum"},"card_tags":[{"value":"Premium"},{"value":"Travel"},{"value":"Insurance"}],"version":1},"transaction":{"transaction_id":"TXN003","transaction_details":{"transaction_type":"PURCHASE","merchant_data":{"merchant_id":"MERCH003","merchant_name":"Apple Store","merchant_country_code":"US"},"posting_date":"2024-01-17","amount":{"currency_code":"USD"},"billing_amount":{"currency_code":"EUR"},"billing_amount_no_fee":1200.0000,"conversion_rate":0.9200,"ird":"IRD003"}}}
{"card":{"card_details":{"card_bin":"411111","card_type":"VISA","issuer":"Bank of America","card_program":"Travel"},"card_tags":[{"value":"Gold"}],"version":1},"transaction":{"transaction_id":"TXN004","transaction_details":{"transaction_type":"PURCHASE","merchant_data":{"merchant_id":"MERCH001","merchant_name":"Amazon","merchant_country_code":"UK"},"posting_date":"2024-01-18","amount":{"currency_code":"GBP"},"billing_amount":{"currency_code":"USD"},"billing_amount_no_fee":85.7500,"conversion_rate":1.2700,"ird":"IRD004"}}}
{"card":{"card_details":{"card_bin":"411111","card_type":"VISA","issuer":"Chase","card_program":"Rewards"},"card_tags":[{"value":"Premium"},{"value":"Verified"}],"version":1},"transaction":{"transaction_id":"TXN001","transaction_details":{"transaction_type":"PURCHASE","merchant_data":{"merchant_id":"MERCH001","merchant_name":"Amazon","merchant_country_code":"US"},"posting_date":"2024-01-15","amount":{"currency_code":"USD"},"billing_amount":{"currency_code":"USD"},"billing_amount_no_fee":100.5000,"conversion_rate":1.0000,"ird":"IRD001"}}}
{"card":{"card_details":{"card_bin":"411111","card_type":"VISA","issuer":"Chase","card_program":"Rewards"},"card_tags":[{"value":"Verified"}],"version":1},"transaction":{"transaction_id":"TXN007","transaction_details":{"transaction_type":"PURCHASE","merchant_data":{"merchant_id":"MERCH001","merchant_name":"Amazon","merchant_country_code":"US"},"posting_date":"2024-01-21","amount":{"currency_code":"USD"},"billing_amount":{"currency_code":"USD"},"billing_amount_no_fee":45.9900,"conversion_rate":1.0000,"ird":"IRD007"}}}
-- !result
SELECT transaction_info 
FROM transaction_data 
ORDER BY transaction_info DESC 
LIMIT 5;
-- result:
{"card":{"card_details":{"card_bin":"520000","card_type":"MASTERCARD","issuer":"HSBC","card_program":"Standard"},"card_tags":[{"value":"Basic"}],"version":2},"transaction":{"transaction_id":"TXN005","transaction_details":{"transaction_type":"WITHDRAWAL","merchant_data":{"merchant_id":"MERCH004","merchant_name":"ATM Network","merchant_country_code":"CN"},"posting_date":"2024-01-19","amount":{"currency_code":"CNY"},"billing_amount":{"currency_code":"USD"},"billing_amount_no_fee":500.0000,"conversion_rate":0.1400,"ird":"IRD005"}}}
{"card":{"card_details":{"card_bin":"520000","card_type":"MASTERCARD","issuer":"Citi","card_program":"CashBack"},"card_tags":[{"value":"Standard"},{"value":"Contactless"}],"version":2},"transaction":{"transaction_id":"TXN008","transaction_details":{"transaction_type":"PURCHASE","merchant_data":{"merchant_id":"MERCH006","merchant_name":"Grocery Store","merchant_country_code":"CA"},"posting_date":"2024-01-22","amount":{"currency_code":"CAD"},"billing_amount":{"currency_code":"USD"},"billing_amount_no_fee":75.0000,"conversion_rate":0.7500,"ird":"IRD008"}}}
{"card":{"card_details":{"card_bin":"520000","card_type":"MASTERCARD","issuer":"Citi","card_program":"CashBack"},"card_tags":[{"value":"Standard"}],"version":2},"transaction":{"transaction_id":"TXN002","transaction_details":{"transaction_type":"REFUND","merchant_data":{"merchant_id":"MERCH002","merchant_name":"Walmart","merchant_country_code":"US"},"posting_date":"2024-01-16","amount":{"currency_code":"USD"},"billing_amount":{"currency_code":"USD"},"billing_amount_no_fee":50.2500,"conversion_rate":1.0000,"ird":"IRD002"}}}
{"card":{"card_details":{"card_bin":"411111","card_type":"VISA","issuer":"Chase","card_program":"Rewards"},"card_tags":[{"value":"Verified"}],"version":1},"transaction":{"transaction_id":"TXN007","transaction_details":{"transaction_type":"PURCHASE","merchant_data":{"merchant_id":"MERCH001","merchant_name":"Amazon","merchant_country_code":"US"},"posting_date":"2024-01-21","amount":{"currency_code":"USD"},"billing_amount":{"currency_code":"USD"},"billing_amount_no_fee":45.9900,"conversion_rate":1.0000,"ird":"IRD007"}}}
{"card":{"card_details":{"card_bin":"411111","card_type":"VISA","issuer":"Chase","card_program":"Rewards"},"card_tags":[{"value":"Premium"},{"value":"Verified"}],"version":1},"transaction":{"transaction_id":"TXN001","transaction_details":{"transaction_type":"PURCHASE","merchant_data":{"merchant_id":"MERCH001","merchant_name":"Amazon","merchant_country_code":"US"},"posting_date":"2024-01-15","amount":{"currency_code":"USD"},"billing_amount":{"currency_code":"USD"},"billing_amount_no_fee":100.5000,"conversion_rate":1.0000,"ird":"IRD001"}}}
-- !result
SELECT id, rn
FROM (
    SELECT 
        id,
        ROW_NUMBER() OVER (ORDER BY transaction_info) AS rn
    FROM transaction_data
) t
WHERE rn <= 5;
-- result:
6	1
3	2
4	3
1	4
7	5
-- !result
SELECT transaction_info 
FROM transaction_data 
WHERE id <= 5
ORDER BY transaction_info;
-- result:
{"card":{"card_details":{"card_bin":"370000","card_type":"AMEX","issuer":"American Express","card_program":"Platinum"},"card_tags":[{"value":"Premium"},{"value":"Travel"},{"value":"Insurance"}],"version":1},"transaction":{"transaction_id":"TXN003","transaction_details":{"transaction_type":"PURCHASE","merchant_data":{"merchant_id":"MERCH003","merchant_name":"Apple Store","merchant_country_code":"US"},"posting_date":"2024-01-17","amount":{"currency_code":"USD"},"billing_amount":{"currency_code":"EUR"},"billing_amount_no_fee":1200.0000,"conversion_rate":0.9200,"ird":"IRD003"}}}
{"card":{"card_details":{"card_bin":"411111","card_type":"VISA","issuer":"Bank of America","card_program":"Travel"},"card_tags":[{"value":"Gold"}],"version":1},"transaction":{"transaction_id":"TXN004","transaction_details":{"transaction_type":"PURCHASE","merchant_data":{"merchant_id":"MERCH001","merchant_name":"Amazon","merchant_country_code":"UK"},"posting_date":"2024-01-18","amount":{"currency_code":"GBP"},"billing_amount":{"currency_code":"USD"},"billing_amount_no_fee":85.7500,"conversion_rate":1.2700,"ird":"IRD004"}}}
{"card":{"card_details":{"card_bin":"411111","card_type":"VISA","issuer":"Chase","card_program":"Rewards"},"card_tags":[{"value":"Premium"},{"value":"Verified"}],"version":1},"transaction":{"transaction_id":"TXN001","transaction_details":{"transaction_type":"PURCHASE","merchant_data":{"merchant_id":"MERCH001","merchant_name":"Amazon","merchant_country_code":"US"},"posting_date":"2024-01-15","amount":{"currency_code":"USD"},"billing_amount":{"currency_code":"USD"},"billing_amount_no_fee":100.5000,"conversion_rate":1.0000,"ird":"IRD001"}}}
{"card":{"card_details":{"card_bin":"520000","card_type":"MASTERCARD","issuer":"Citi","card_program":"CashBack"},"card_tags":[{"value":"Standard"}],"version":2},"transaction":{"transaction_id":"TXN002","transaction_details":{"transaction_type":"REFUND","merchant_data":{"merchant_id":"MERCH002","merchant_name":"Walmart","merchant_country_code":"US"},"posting_date":"2024-01-16","amount":{"currency_code":"USD"},"billing_amount":{"currency_code":"USD"},"billing_amount_no_fee":50.2500,"conversion_rate":1.0000,"ird":"IRD002"}}}
{"card":{"card_details":{"card_bin":"520000","card_type":"MASTERCARD","issuer":"HSBC","card_program":"Standard"},"card_tags":[{"value":"Basic"}],"version":2},"transaction":{"transaction_id":"TXN005","transaction_details":{"transaction_type":"WITHDRAWAL","merchant_data":{"merchant_id":"MERCH004","merchant_name":"ATM Network","merchant_country_code":"CN"},"posting_date":"2024-01-19","amount":{"currency_code":"CNY"},"billing_amount":{"currency_code":"USD"},"billing_amount_no_fee":500.0000,"conversion_rate":0.1400,"ird":"IRD005"}}}
-- !result
SELECT 
    id,
    transaction_info.card.card_details.card_type,
    transaction_info.card.card_details.issuer
FROM transaction_data 
ORDER BY transaction_info 
LIMIT 5;
-- result:
6	AMEX	American Express
3	AMEX	American Express
4	VISA	Bank of America
1	VISA	Chase
7	VISA	Chase
-- !result
SELECT DISTINCT transaction_info.card.card_details.card_type
FROM transaction_data
ORDER BY transaction_info.card.card_details.card_type;
-- result:
AMEX
MASTERCARD
VISA
-- !result
SELECT id, transaction_info
FROM (
    SELECT * FROM transaction_data WHERE id <= 6
) t
ORDER BY transaction_info
LIMIT 3;
-- result:
6	{"card":{"card_details":{"card_bin":"370000","card_type":"AMEX","issuer":"American Express","card_program":"Gold"},"card_tags":[{"value":"Premium"},{"value":"Concierge"}],"version":3},"transaction":{"transaction_id":"TXN006","transaction_details":{"transaction_type":"PURCHASE","merchant_data":{"merchant_id":"MERCH005","merchant_name":"Luxury Hotel","merchant_country_code":"FR"},"posting_date":"2024-01-20","amount":{"currency_code":"EUR"},"billing_amount":{"currency_code":"USD"},"billing_amount_no_fee":2500.0000,"conversion_rate":1.0800,"ird":"IRD006"}}}
3	{"card":{"card_details":{"card_bin":"370000","card_type":"AMEX","issuer":"American Express","card_program":"Platinum"},"card_tags":[{"value":"Premium"},{"value":"Travel"},{"value":"Insurance"}],"version":1},"transaction":{"transaction_id":"TXN003","transaction_details":{"transaction_type":"PURCHASE","merchant_data":{"merchant_id":"MERCH003","merchant_name":"Apple Store","merchant_country_code":"US"},"posting_date":"2024-01-17","amount":{"currency_code":"USD"},"billing_amount":{"currency_code":"EUR"},"billing_amount_no_fee":1200.0000,"conversion_rate":0.9200,"ird":"IRD003"}}}
4	{"card":{"card_details":{"card_bin":"411111","card_type":"VISA","issuer":"Bank of America","card_program":"Travel"},"card_tags":[{"value":"Gold"}],"version":1},"transaction":{"transaction_id":"TXN004","transaction_details":{"transaction_type":"PURCHASE","merchant_data":{"merchant_id":"MERCH001","merchant_name":"Amazon","merchant_country_code":"UK"},"posting_date":"2024-01-18","amount":{"currency_code":"GBP"},"billing_amount":{"currency_code":"USD"},"billing_amount_no_fee":85.7500,"conversion_rate":1.2700,"ird":"IRD004"}}}
-- !result
SELECT id, transaction_info
FROM (
    SELECT 
        id,
        transaction_info,
        ROW_NUMBER() OVER (ORDER BY transaction_info) AS rn
    FROM transaction_data
) t
WHERE rn = 5;
-- result:
7	{"card":{"card_details":{"card_bin":"411111","card_type":"VISA","issuer":"Chase","card_program":"Rewards"},"card_tags":[{"value":"Verified"}],"version":1},"transaction":{"transaction_id":"TXN007","transaction_details":{"transaction_type":"PURCHASE","merchant_data":{"merchant_id":"MERCH001","merchant_name":"Amazon","merchant_country_code":"US"},"posting_date":"2024-01-21","amount":{"currency_code":"USD"},"billing_amount":{"currency_code":"USD"},"billing_amount_no_fee":45.9900,"conversion_rate":1.0000,"ird":"IRD007"}}}
-- !result
SELECT COUNT(*)
FROM (
    SELECT transaction_info 
    FROM transaction_data 
    ORDER BY transaction_info 
    LIMIT 8
) t;
-- result:
8
-- !result
SELECT 
    id,
    transaction_info.card.version,
    transaction_info.transaction.transaction_id
FROM transaction_data
ORDER BY transaction_info, id;
-- result:
6	3	TXN006
3	1	TXN003
4	1	TXN004
1	1	TXN001
7	1	TXN007
2	2	TXN002
8	2	TXN008
5	2	TXN005
-- !result
DROP TABLE transaction_data FORCE;
-- result:
-- !result
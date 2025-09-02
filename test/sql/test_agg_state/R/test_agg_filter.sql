-- name: test_agg_filter
DROP DATABASE IF EXISTS test_agg_filter;
-- result:
-- !result
CREATE DATABASE test_agg_filter;
-- result:
-- !result
USE test_agg_filter;
-- result:
-- !result
CREATE TABLE sales (
    id INT,
    product VARCHAR(50),
    amount DECIMAL(10, 2),
    quantity INT
) properties ("replication_num"="1");
-- result:
-- !result
INSERT INTO sales (id, product, amount, quantity) VALUES
(1, 'A', 100.00, 10),
(2, 'B', 150.00, 20),
(3, 'A', 200.00, 15),
(4, 'B', 250.00, 25),
(5, 'C', 300.00, 30),
(6, 'Laptop', 500.00, 40);
-- result:
-- !result
CREATE TABLE products (
    product_id INT,
    product VARCHAR(50),
    category VARCHAR(50)
) properties ("replication_num"="1");
-- result:
-- !result
INSERT INTO products (product_id, product, category) VALUES
(1, 'Laptop', 'Electronics'),
(2, 'Smartphone', 'Electronics'),
(3, 'Desk', 'Furniture'),
(4, 'Chair', 'Furniture'),
(5, 'Headphones', 'Electronics');
-- result:
-- !result
SELECT
AVG(amount) FILTER (WHERE product = 'A') AS avg_amount_a,
COUNT(*) FILTER (WHERE quantity > 15) AS count_large_quantity,
MAX(amount) FILTER (WHERE product = 'B') AS max_amount_b,
MIN(amount) FILTER (WHERE amount > 100) AS min_amount_large,
SUM(amount) FILTER (WHERE product = 'C') AS sum_amount_c,
ARRAY_AGG(product) FILTER (WHERE quantity < 20) AS products,
ARRAY_AGG(DISTINCT product) FILTER (WHERE quantity < 20) AS distinct_products1,
ARRAY_AGG_DISTINCT(product) FILTER (WHERE quantity < 20) AS distinct_products2,
COUNT(amount) AS count_amount,
COUNT(*) FILTER (WHERE amount > (SELECT AVG(amount) FROM sales)) AS count_above_avg,
SUM(amount) FILTER (WHERE product IN (SELECT product FROM products WHERE category = 'Electronics')) AS sum_electronics
FROM sales
group by id
order by id;
-- result:
100.00000000	0	None	None	None	["A"]	["A"]	["A"]	1	0	None
None	1	150.00	150.00	None	[null]	[null]	[null]	1	0	None
200.00000000	0	None	200.00	None	["A"]	["A"]	["A"]	1	0	None
None	1	250.00	250.00	None	[null]	[null]	[null]	1	0	None
None	1	None	300.00	300.00	[null]	[null]	[null]	1	1	None
None	1	None	500.00	None	[null]	[null]	[null]	1	1	500.00
-- !result
set sql_dialect='Trino';
-- result:
-- !result
SELECT
AVG(amount) FILTER (WHERE product = 'A') AS avg_amount_a,
COUNT(*) FILTER (WHERE quantity > 15) AS count_large_quantity,
MAX(amount) FILTER (WHERE product = 'B') AS max_amount_b,
MIN(amount) FILTER (WHERE amount > 100) AS min_amount_large,
SUM(amount) FILTER (WHERE product = 'C') AS sum_amount_c,
ARRAY_AGG(product) FILTER (WHERE quantity < 20) AS products,
ARRAY_AGG(DISTINCT product) FILTER (WHERE quantity < 20) AS distinct_products1,
COUNT(amount) AS count_amount,
COUNT(*) FILTER (WHERE amount > (SELECT AVG(amount) FROM sales)) AS count_above_avg,
SUM(amount) FILTER (WHERE product IN (SELECT product FROM products WHERE category = 'Electronics')) AS sum_electronics
FROM sales
group by id
order by id;
-- result:
100.00000000	0	None	None	None	["A"]	["A"]	1	0	None
None	1	150.00	150.00	None	[null]	[null]	1	0	None
200.00000000	0	None	200.00	None	["A"]	["A"]	1	0	None
None	1	250.00	250.00	None	[null]	[null]	1	0	None
None	1	None	300.00	300.00	[null]	[null]	1	1	None
None	1	None	500.00	None	[null]	[null]	1	1	500.00
-- !result
set sql_dialect='StarRocks';
-- result:
-- !result
SELECT /*+ SET_VAR (streaming_preaggregation_mode = 'force_streaming') */
AVG(amount) FILTER (WHERE product = 'A') AS avg_amount_a,
COUNT(*) FILTER (WHERE quantity > 15) AS count_large_quantity,
MAX(amount) FILTER (WHERE product = 'B') AS max_amount_b,
MIN(amount) FILTER (WHERE amount > 100) AS min_amount_large,
SUM(amount) FILTER (WHERE product = 'C') AS sum_amount_c,
ARRAY_AGG(product) FILTER (WHERE quantity < 20) AS products,
ARRAY_AGG(DISTINCT product) FILTER (WHERE quantity < 20) AS distinct_products1,
ARRAY_AGG_DISTINCT(product) FILTER (WHERE quantity < 20) AS distinct_products2,
COUNT(amount) AS count_amount,
COUNT(*) FILTER (WHERE amount > (SELECT AVG(amount) FROM sales)) AS count_above_avg,
SUM(amount) FILTER (WHERE product IN (SELECT product FROM products WHERE category = 'Electronics')) AS sum_electronics
FROM sales
group by id
order by id;
-- result:
100.00000000	0	None	None	None	["A"]	["A"]	["A"]	1	0	None
None	1	150.00	150.00	None	[null]	[null]	[null]	1	0	None
200.00000000	0	None	200.00	None	["A"]	["A"]	["A"]	1	0	None
None	1	250.00	250.00	None	[null]	[null]	[null]	1	0	None
None	1	None	300.00	300.00	[null]	[null]	[null]	1	1	None
None	1	None	500.00	None	[null]	[null]	[null]	1	1	500.00
-- !result
drop table sales;
-- result:
-- !result
drop table products;
-- result:
-- !result
CREATE TABLE sales (
    id INT,
    product VARCHAR(50),
    category VARCHAR(50),
    amount DECIMAL(10,2),
    quantity INT,
    sale_date DATE,
    region VARCHAR(20),
    gender CHAR(1)
) properties("replication_num" = "1");
-- result:
-- !result
CREATE TABLE products (
    product VARCHAR(50),
    category VARCHAR(50),
    brand VARCHAR(30)
) properties("replication_num" = "1");;
-- result:
-- !result
CREATE TABLE customers (
    id INT,
    name VARCHAR(50),
    age INT,
    vip_level VARCHAR(10)
) PROPERTIES ("replication_num" = "1");
-- result:
-- !result
CREATE TABLE regions (
    region VARCHAR(20),
    country VARCHAR(30),
    timezone VARCHAR(20)
) PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO sales VALUES
(11, 'Laptop', 'Electronics', 1599.99, 3, '2024-02-01', 'North', 'M'),
(12, 'Dress', 'Clothing', 159.99, 8, '2024-02-02', 'South', 'F'),
(13, 'Phone Case', 'Electronics', 19.99, 50, '2024-02-03', 'East', 'M'),
(14, 'Sneakers', 'Clothing', 89.99, 22, '2024-02-04', 'West', 'F'),
(15, 'Tablet', 'Electronics', 399.99, 0, '2024-02-05', 'North', 'M'),
(16, 'Scarf', 'Clothing', 39.99, 35, '2024-02-06', 'South', 'F'),
(17, 'Monitor', 'Electronics', 299.99, 7, '2024-02-07', 'East', 'M'),
(18, NULL, 'Clothing', 99.99, 11, '2024-02-08', 'West', 'F'),
(19, 'Keyboard', 'Electronics', 79.99, NULL, '2024-02-09', 'North', 'M'),
(20, 'Hat', NULL, 29.99, 40, '2024-02-10', 'South', 'F');
-- result:
-- !result
INSERT INTO sales VALUES
(1, 'iPhone', 'Electronics', 999.99, 10, '2024-01-15', 'North', 'M'),
(2, 'MacBook', 'Electronics', 1299.99, 5, '2024-01-16', 'South', 'F'),
(3, 'Shirt', 'Clothing', 29.99, 25, '2024-01-17', 'North', 'M'),
(4, 'Jeans', 'Clothing', 79.99, 15, '2024-01-18', 'East', 'F'),
(5, 'iPad', 'Electronics', 599.99, 8, '2024-01-19', 'West', 'M'),
(6, 'Shoes', 'Clothing', 129.99, 12, '2024-01-20', 'North', 'F'),
(7, 'Watch', 'Electronics', 299.99, 20, '2024-01-21', 'South', 'M'),
(8, 'Jacket', 'Clothing', 199.99, 6, '2024-01-22', 'East', 'F'),
(9, 'Headphones', 'Electronics', 199.99, 30, '2024-01-23', 'West', 'M'),
(10, 'Bag', 'Clothing', 89.99, 18, '2024-01-24', 'North', 'F');
-- result:
-- !result
INSERT INTO products VALUES
('iPhone', 'Electronics', 'Apple'),
('MacBook', 'Electronics', 'Apple'),
('iPad', 'Electronics', 'Apple'),
('Watch', 'Electronics', 'Generic'),
('Headphones', 'Electronics', 'Sony');
-- result:
-- !result
INSERT INTO customers VALUES
(1, 'Alice', 25, 'Gold'),
(2, 'Bob', 30, 'Silver'),
(3, 'Charlie', 35, 'Bronze'),
(4, 'Diana', 28, 'Gold'),
(5, 'Eve', 32, 'Silver');
-- result:
-- !result
INSERT INTO regions VALUES
('North', 'USA', 'EST'),
('South', 'USA', 'CST'),
('East', 'USA', 'EST'),
('West', 'USA', 'PST');
-- result:
-- !result
SELECT
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE product IS NOT NULL) AS non_null_product,
    COUNT(*) FILTER (WHERE product IS NULL) AS null_product,
    COUNT(*) FILTER (WHERE category IS NOT NULL) AS non_null_category,
    SUM(amount) FILTER (WHERE quantity IS NOT NULL) AS sum_non_null_qty,
    AVG(amount) FILTER (WHERE product IS NOT NULL AND quantity IS NOT NULL) AS avg_complete_data
FROM sales;
-- result:
20	19	1	19	6669.81	364.99000000
-- !result
SELECT
    -- Zero value tests
    COUNT(*) FILTER (WHERE quantity = 0) AS zero_quantity,
    COUNT(*) FILTER (WHERE quantity > 0) AS positive_quantity,

    -- Extreme value tests
    COUNT(*) FILTER (WHERE amount >= 1000) AS expensive_items,
    COUNT(*) FILTER (WHERE amount < 50) AS cheap_items,
    COUNT(*) FILTER (WHERE amount BETWEEN 100 AND 500) AS mid_range,

    -- Exact match
    COUNT(*) FILTER (WHERE amount = 199.99) AS exact_price_match,

    -- Mathematical operations
    SUM(amount) FILTER (WHERE quantity * amount > 1000) AS high_value_transactions,
    AVG(quantity) FILTER (WHERE amount / quantity > 50) AS avg_qty_expensive_unit
FROM sales;
-- result:
1	18	2	4	7	2	6219.84	6.5
-- !result
SELECT
    category,
    COUNT(*) FILTER (WHERE product LIKE '%Phone%') AS phone_products,
    COUNT(*) FILTER (WHERE product LIKE 'i%') AS products_start_i,
    COUNT(*) FILTER (WHERE product REGEXP '^[A-H]') AS products_a_to_h,
    COUNT(*) FILTER (WHERE LENGTH(product) > 5) AS long_product_names,
    COUNT(*) FILTER (WHERE UPPER(product) = UPPER(region)) AS product_region_match,
    SUM(amount) FILTER (WHERE product NOT LIKE '%Case%') AS sum_non_case_products
FROM sales
GROUP BY category
ORDER BY category;
-- result:
None	0	0	1	0	0	29.99
Clothing	0	0	2	2	0	819.92
Electronics	2	2	1	8	0	5779.91
-- !result
SELECT
    MONTH(sale_date) AS sale_month,
    COUNT(*) AS total_sales,
    COUNT(*) FILTER (WHERE DAYOFWEEK(sale_date) IN (1,7)) AS weekend_sales,
    COUNT(*) FILTER (WHERE sale_date >= '2024-02-01') AS february_sales,
    COUNT(*) FILTER (WHERE DATEDIFF(CURDATE(), sale_date) < 30) AS recent_sales,
    SUM(amount) FILTER (WHERE YEAR(sale_date) = 2024) AS year_2024_sum,
    AVG(amount) FILTER (WHERE DAY(sale_date) <= 15) AS first_half_month_avg
FROM sales
GROUP BY MONTH(sale_date)
ORDER BY sale_month;
-- result:
1	10	2	0	0	3929.90	999.99000000
2	10	3	10	0	2819.90	281.99000000
-- !result
SELECT
    region,
    -- Scalar subquery
    COUNT(*) FILTER (WHERE amount > (SELECT AVG(amount) FROM sales)) AS above_global_avg,
    COUNT(*) FILTER (WHERE quantity > (SELECT MAX(quantity) FROM sales WHERE category = 'Electronics')) AS above_max_electronics_qty,

    -- Correlated subquery
    COUNT(*) FILTER (WHERE amount > (SELECT AVG(amount) FROM sales s2 WHERE s2.region = sales.region)) AS above_region_avg
FROM sales
GROUP BY region
ORDER BY region;
-- result:
East	0	0	2
North	3	0	2
South	1	0	1
West	1	0	1
-- !result
SELECT
    r.country,
    COUNT(*) AS total_sales,
    COUNT(*) FILTER (WHERE s.category = 'Electronics') AS electronics_count,
    SUM(s.amount) FILTER (WHERE r.timezone = 'EST') AS est_timezone_sum,
    AVG(s.amount) FILTER (WHERE s.quantity > 10 AND r.region IN ('North', 'South')) AS north_south_high_qty_avg
FROM sales s
JOIN regions r ON s.region = r.region
GROUP BY r.country
ORDER BY r.country;
-- result:
USA	20	10	3929.89	103.32333333
-- !result
SELECT
    -- AND combinations
    COUNT(*) FILTER (WHERE category = 'Electronics' AND amount > 500 AND quantity < 20) AS complex_and,

    -- OR combinations
    COUNT(*) FILTER (WHERE product LIKE '%Phone%' OR product LIKE '%Pad%' OR product LIKE '%Book%') AS apple_like_products,

    -- NOT conditions
    COUNT(*) FILTER (WHERE NOT (category = 'Clothing' AND amount < 100)) AS not_cheap_clothing,

    -- Nested conditions
    SUM(amount) FILTER (WHERE (category = 'Electronics' AND region IN ('North', 'West')) OR (category = 'Clothing' AND gender = 'F')) AS nested_conditions,

    -- CASE expressions in FILTER
    COUNT(*) FILTER (WHERE CASE WHEN quantity > 20 THEN amount > 100 ELSE amount > 50 END) AS conditional_logic
FROM sales;
-- result:
4	4	13	4769.86	15
-- !result
SELECT
    COUNT(*) FILTER (WHERE CAST(amount AS INT) > 100) AS cast_test,
    COUNT(*) FILTER (WHERE ROUND(amount, 0) = amount) AS whole_number_amounts,
    COUNT(*) FILTER (WHERE ABS(quantity - 15) < 5) AS quantity_near_15,
    SUM(amount) FILTER (WHERE COALESCE(quantity, 0) > 10) AS sum_with_coalesce,
    AVG(amount) FILTER (WHERE IFNULL(product, 'Unknown') != 'Unknown') AS avg_known_products
FROM sales;
-- result:
11	0	4	1109.89	349.99000000
-- !result
SELECT
    -- Empty result sets
    COUNT(*) FILTER (WHERE 1 = 0) AS impossible_condition,
    COUNT(*) FILTER (WHERE 1 = 1) AS always_true,

    -- Empty strings
    COUNT(*) FILTER (WHERE product != '') AS non_empty_product,

    -- Numeric boundaries
    COUNT(*) FILTER (WHERE amount > 0.01) AS above_penny,
    COUNT(*) FILTER (WHERE quantity >= 0) AS non_negative_qty,

    -- Combined null checks
    COUNT(*) FILTER (WHERE product IS NOT NULL AND category IS NOT NULL AND quantity IS NOT NULL) AS complete_records
FROM sales;
-- result:
0	20	19	20	19	17
-- !result
SELECT COUNT(*) FILTER (WHERE (SELECT region FROM sales) = 'North') FROM sales;
-- result:
[REGEX].*Expected LE 1 to be returned by expression*
-- !result
SELECT COUNT(DISTINCT product) FILTER (WHERE amount > 100) FROM sales;
-- result:
E: (1064, 'Getting syntax error. Detail message: Aggregation filter does not support COUNT DISTINCT.')
-- !result
SELECT
    category,
    ARRAY_SORT(ARRAY_AGG(product) FILTER (WHERE amount > 100)) AS expensive_products,
    ARRAY_SORT(ARRAY_AGG(DISTINCT region) FILTER (WHERE quantity > 15)) AS high_qty_regions,
    ARRAY_SORT(ARRAY_AGG(product) FILTER (WHERE gender = 'M')) AS male_products,
    ARRAY_SORT(ARRAY_AGG(DISTINCT category) FILTER (WHERE amount < 50)) AS cheap_categories
FROM (SELECT * FROM sales ORDER BY id) AS sorted_sales
GROUP BY category
ORDER BY category;
-- result:
None	[null]	["South"]	[null]	[null]
Clothing	[null,null,null,null,null,null,"Dress","Jacket","Shoes"]	[null,"North","South","West"]	[null,null,null,null,null,null,null,null,"Shirt"]	[null,"Clothing"]
Electronics	[null,null,"Headphones","Laptop","MacBook","Monitor","Tablet","Watch","iPad","iPhone"]	[null,"East","South","West"]	[null,"Headphones","Keyboard","Laptop","Monitor","Phone Case","Tablet","Watch","iPad","iPhone"]	[null,"Electronics"]
-- !result
SELECT
    region,
    ARRAY_SORT(ARRAY_AGG(product) FILTER (WHERE category = 'Electronics' AND amount > 200)) AS expensive_electronics,
    ARRAY_SORT(ARRAY_AGG(DISTINCT category) FILTER (WHERE quantity BETWEEN 10 AND 30)) AS mid_qty_categories,
    ARRAY_SORT(ARRAY_AGG(CONCAT(product, '(', CAST(amount AS STRING), ')')) FILTER (WHERE product IS NOT NULL AND amount > 150)) AS product_price_pairs,
    ARRAY_SORT(ARRAY_AGG(product) FILTER (WHERE product LIKE '%Phone%' OR product LIKE '%Laptop%')) AS tech_products
FROM (SELECT * FROM sales ORDER BY id) AS sorted_sales
GROUP BY region
ORDER BY region;
-- result:
East	[null,null,null,"Monitor"]	[null,"Clothing"]	[null,null,"Jacket(199.99)","Monitor(299.99)"]	[null,null,null,"Phone Case"]
North	[null,null,null,null,"Laptop","Tablet","iPhone"]	[null,"Clothing","Electronics"]	[null,null,null,null,"Laptop(1599.99)","Tablet(399.99)","iPhone(999.99)"]	[null,null,null,null,null,"Laptop","iPhone"]
South	[null,null,null,"MacBook","Watch"]	[null,"Electronics"]	[null,null,"Dress(159.99)","MacBook(1299.99)","Watch(299.99)"]	[null,null,null,null,null]
West	[null,null,null,"iPad"]	[null,"Clothing","Electronics"]	[null,null,"Headphones(199.99)","iPad(599.99)"]	[null,null,null,null]
-- !result
SELECT
    gender,
    ARRAY_SORT(ARRAY_AGG(product) FILTER (WHERE product IS NOT NULL)) AS non_null_products,
    ARRAY_SORT(ARRAY_AGG(category) FILTER (WHERE category IS NOT NULL AND quantity IS NOT NULL)) AS complete_data,
    ARRAY_SORT(ARRAY_AGG(DISTINCT region) FILTER (WHERE amount IS NOT NULL AND amount > 0)) AS regions_with_sales,
    ARRAY_SORT(ARRAY_AGG(product) FILTER (WHERE product IS NULL)) AS null_products
FROM (SELECT * FROM sales ORDER BY id) AS sorted_sales
GROUP BY gender
ORDER BY gender;
-- result:
F	[null,"Bag","Dress","Hat","Jacket","Jeans","MacBook","Scarf","Shoes","Sneakers"]	[null,"Clothing","Clothing","Clothing","Clothing","Clothing","Clothing","Clothing","Clothing","Electronics"]	["East","North","South","West"]	[null,null,null,null,null,null,null,null,null,null]
M	["Headphones","Keyboard","Laptop","Monitor","Phone Case","Shirt","Tablet","Watch","iPad","iPhone"]	[null,"Clothing","Electronics","Electronics","Electronics","Electronics","Electronics","Electronics","Electronics","Electronics"]	["East","North","South","West"]	[null,null,null,null,null,null,null,null,null,null]
-- !result
SELECT
    MONTH(sale_date) AS sale_month,
    ARRAY_SORT(ARRAY_AGG(product) FILTER (WHERE DAYOFWEEK(sale_date) IN (1,7))) AS weekend_products,
    ARRAY_SORT(ARRAY_AGG(DISTINCT region) FILTER (WHERE sale_date >= '2024-02-01')) AS february_regions,
    ARRAY_SORT(ARRAY_AGG(CONCAT(product, '@', region)) FILTER (WHERE DATEDIFF(CURDATE(), sale_date) < 30)) AS recent_product_regions,
    ARRAY_SORT(ARRAY_AGG(category) FILTER (WHERE DAY(sale_date) <= 15)) AS first_half_categories
FROM (SELECT * FROM sales ORDER BY id) AS sorted_sales
GROUP BY MONTH(sale_date)
ORDER BY sale_month;
-- result:
1	[null,null,null,null,null,null,null,null,"Shoes","Watch"]	[null]	[null,null,null,null,null,null,null,null,null,null]	[null,null,null,null,null,null,null,null,null,"Electronics"]
2	[null,null,null,null,null,null,null,"Hat","Phone Case","Sneakers"]	["East","North","South","West"]	[null,null,null,null,null,null,null,null,null,null]	[null,"Clothing","Clothing","Clothing","Clothing","Electronics","Electronics","Electronics","Electronics","Electronics"]
-- !result
SELECT
    category,
    ARRAY_SORT(ARRAY_AGG(product) FILTER (WHERE LENGTH(product) > 5)) AS long_name_products,
    ARRAY_SORT(ARRAY_AGG(UPPER(product)) FILTER (WHERE product REGEXP '^[A-H]')) AS products_a_to_h,
    ARRAY_SORT(ARRAY_AGG(DISTINCT SUBSTRING(product, 1, 3)) FILTER (WHERE product IS NOT NULL)) AS product_prefixes,
    ARRAY_SORT(ARRAY_AGG(product) FILTER (WHERE LOCATE('e', LOWER(product)) > 0)) AS products_with_e
FROM (SELECT * FROM sales ORDER BY id) AS sorted_sales
GROUP BY category
ORDER BY category;
-- result:
None	[null]	["HAT"]	["Hat"]	[null]
Clothing	[null,null,null,null,null,null,null,"Jacket","Sneakers"]	[null,null,null,null,null,null,null,"BAG","DRESS"]	[null,"Bag","Dre","Jac","Jea","Sca","Shi","Sho","Sne"]	[null,null,null,null,"Dress","Jacket","Jeans","Shoes","Sneakers"]
Electronics	[null,null,"Headphones","Keyboard","Laptop","MacBook","Monitor","Phone Case","Tablet","iPhone"]	[null,null,null,null,null,null,null,null,null,"HEADPHONES"]	["Hea","Key","Lap","Mac","Mon","Pho","Tab","Wat","iPa","iPh"]	[null,null,null,null,null,"Headphones","Keyboard","Phone Case","Tablet","iPhone"]
-- !result
SELECT
    region,
    ARRAY_SORT(ARRAY_AGG(product) FILTER (WHERE amount * quantity > 1000)) AS high_value_products,
    ARRAY_SORT(ARRAY_AGG(DISTINCT category) FILTER (WHERE ROUND(amount, 0) = amount)) AS whole_price_categories,
    ARRAY_SORT(ARRAY_AGG(product) FILTER (WHERE ABS(quantity - 15) < 5)) AS qty_near_15_products,
    ARRAY_SORT(ARRAY_AGG(CONCAT(product, ':', CAST(ROUND(amount/quantity, 2) AS STRING))) FILTER (WHERE quantity > 0)) AS product_unit_prices
FROM (SELECT * FROM sales ORDER BY id) AS sorted_sales
GROUP BY region
ORDER BY region;
-- result:
East	[null,"Jacket","Jeans","Monitor"]	[null]	[null,null,null,"Jeans"]	["Jacket:33.33","Jeans:5.33","Monitor:42.86","Phone Case:0.40"]
North	[null,null,null,"Bag","Laptop","Shoes","iPhone"]	[null]	[null,null,null,null,null,"Bag","Shoes"]	[null,null,"Bag:5.00","Laptop:533.33","Shirt:1.20","Shoes:10.83","iPhone:100.00"]
South	["Dress","Hat","MacBook","Scarf","Watch"]	[null]	[null,null,null,null,null]	["Dress:20.00","Hat:0.75","MacBook:260.00","Scarf:1.14","Watch:15.00"]
West	[null,"Headphones","Sneakers","iPad"]	[null]	[null,null,null,null]	[null,"Headphones:6.67","Sneakers:4.09","iPad:75.00"]
-- !result
SELECT
    -- Always false condition (should return empty arrays)
    ARRAY_SORT(ARRAY_AGG(product) FILTER (WHERE 1 = 0)) AS impossible_products,

    -- Always true condition (should return all products)
    ARRAY_SORT(ARRAY_AGG(product) FILTER (WHERE 1 = 1)) AS all_products,

    -- Empty string conditions
    ARRAY_SORT(ARRAY_AGG(product) FILTER (WHERE product != '')) AS non_empty_products,

    -- Zero and negative values
    ARRAY_SORT(ARRAY_AGG(product) FILTER (WHERE quantity = 0)) AS zero_qty_products,
    ARRAY_SORT(ARRAY_AGG(product) FILTER (WHERE amount > 0.01)) AS above_penny_products,

    -- Complex nested conditions
    ARRAY_SORT(ARRAY_AGG(DISTINCT category) FILTER (WHERE (category = 'Electronics' AND amount > 100) OR (category = 'Clothing' AND quantity > 20))) AS complex_categories
FROM (SELECT * FROM sales ORDER BY id) AS sorted_sales;
-- result:
[null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]	[null,"Bag","Dress","Hat","Headphones","Jacket","Jeans","Keyboard","Laptop","MacBook","Monitor","Phone Case","Scarf","Shirt","Shoes","Sneakers","Tablet","Watch","iPad","iPhone"]	[null,"Bag","Dress","Hat","Headphones","Jacket","Jeans","Keyboard","Laptop","MacBook","Monitor","Phone Case","Scarf","Shirt","Shoes","Sneakers","Tablet","Watch","iPad","iPhone"]	[null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,"Tablet"]	[null,"Bag","Dress","Hat","Headphones","Jacket","Jeans","Keyboard","Laptop","MacBook","Monitor","Phone Case","Scarf","Shirt","Shoes","Sneakers","Tablet","Watch","iPad","iPhone"]	[null,"Clothing","Electronics"]
-- !result

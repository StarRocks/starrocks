-- name: test_agg_filter
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

SELECT  /*+ SET_VAR (streaming_preaggregation_mode = 'force_streaming') */
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
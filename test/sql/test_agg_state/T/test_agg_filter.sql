-- name: test_agg_filter

CREATE TABLE sales (
    id INT,
    product VARCHAR(50),
    amount DECIMAL(10, 2),
    quantity INT
) properties ("replication_num"="1");

INSERT INTO sales (id, product, amount, quantity) VALUES
(1, 'A', 100.00, 10),
(2, 'B', 150.00, 20),
(3, 'A', 200.00, 15),
(4, 'B', 250.00, 25),
(5, 'C', 300.00, 30),
(6, 'Laptop', 500.00, 40);

CREATE TABLE products (
    product_id INT,
    product VARCHAR(50),
    category VARCHAR(50)
) properties ("replication_num"="1");

INSERT INTO products (product_id, product, category) VALUES
(1, 'Laptop', 'Electronics'),
(2, 'Smartphone', 'Electronics'),
(3, 'Desk', 'Furniture'),
(4, 'Chair', 'Furniture'),
(5, 'Headphones', 'Electronics');

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

set sql_dialect='Trino';

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

set sql_dialect='StarRocks';

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

drop table sales;
drop table products;


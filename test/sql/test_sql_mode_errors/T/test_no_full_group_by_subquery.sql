-- name: test_no_full_group_by_correlated_subquery
-- Test Point: correlated subquery referencing non-GROUP-BY outer columns should
-- succeed when ONLY_FULL_GROUP_BY is disabled, and be rejected when enabled (issue #70996)
create database test_db_${uuid0};
use test_db_${uuid0};
create table orders (
  order_id INT,
  customer_id INT,
  amount DECIMAL(10,2),
  status VARCHAR(20)
) ENGINE=OLAP
DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 2
PROPERTIES('replication_num'='1');
insert into orders values (1, 101, 100.00, 'complete'), (2, 101, 200.00, 'pending'), (3, 102, 300.00, 'complete'), (4, 102, 150.00, 'pending');

-- With ONLY_FULL_GROUP_BY disabled, correlated subquery referencing
-- non-GROUP-BY outer column should succeed and return correct results.
set sql_mode = '';
select customer_id, max(amount), (select count(*) from orders o2 where o2.customer_id = o.customer_id and o2.status = o.status) as cnt from orders o group by customer_id order by customer_id;

-- With ONLY_FULL_GROUP_BY enabled, the same query should be rejected.
set sql_mode = 'ONLY_FULL_GROUP_BY';
select customer_id, max(amount), (select count(*) from orders o2 where o2.customer_id = o.customer_id and o2.status = o.status) as cnt from orders o group by customer_id order by customer_id;

-- Additional: multiple non-GROUP-BY columns in subquery correlation
set sql_mode = '';
select customer_id, max(amount), (select count(*) from orders o2 where o2.customer_id = o.customer_id and o2.status = o.status and o2.amount = o.amount) as cnt from orders o group by customer_id order by customer_id;

-- Correlated subquery in HAVING with non-GROUP-BY outer column
set sql_mode = '';
select customer_id, max(amount) from orders o group by customer_id having max(amount) > (select count(*) from orders o2 where o2.customer_id = o.customer_id and o2.status = o.status) order by customer_id;

drop database test_db_${uuid0};

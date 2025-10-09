-- name: test_large_in_predicate_to_join @sequential

DROP DATABASE IF EXISTS test_large_in_predicate_to_join;
CREATE DATABASE test_large_in_predicate_to_join;
use test_large_in_predicate_to_join;

-- Set threshold to 3 to trigger LargeInPredicate transformation
set large_in_predicate_threshold=3;
set enable_large_in_predicate=true;

-- ========== Create Test Tables ==========

-- Integer test table
CREATE TABLE t_int (
    id int NOT NULL,
    tinyint_col tinyint,
    smallint_col smallint,
    int_col int,
    bigint_col bigint,
    value varchar(50)
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);

-- String test table
CREATE TABLE t_string (
    id int NOT NULL,
    varchar_col varchar(50),
    char_col char(10),
    text_col text,
    category varchar(20)
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);

-- Mixed types table for type compatibility testing
CREATE TABLE t_mixed (
    id int NOT NULL,
    int_val int,
    str_val varchar(50),
    float_val float,
    double_val double,
    decimal_val decimal(10,2),
    date_val date,
    datetime_val datetime,
    bool_val boolean
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);

-- Large table for performance testing
CREATE TABLE t_large (
    id bigint NOT NULL,
    category_id int,
    status varchar(20),
    score double,
    created_date date
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);

-- Dimension table for JOIN testing
CREATE TABLE t_dimension (
    dim_id int NOT NULL,
    dim_name varchar(50),
    dim_type varchar(20)
) ENGINE=OLAP
DUPLICATE KEY(dim_id)
DISTRIBUTED BY HASH(dim_id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);

-- ========== Insert Test Data ==========

-- Insert integer test data
INSERT INTO t_int VALUES
(1, 10, 100, 1000, 10000, 'value1'),
(2, 20, 200, 2000, 20000, 'value2'),
(3, 30, 300, 3000, 30000, 'value3'),
(4, 40, 400, 4000, 40000, 'value4'),
(5, 50, 500, 5000, 50000, 'value5'),
(6, 60, 600, 6000, 60000, 'value6'),
(7, 70, 700, 7000, 70000, 'value7'),
(8, 80, 800, 8000, 80000, 'value8'),
(9, 90, 900, 9000, 90000, 'value9'),
(10, 100, 1000, 10000, 100000, 'value10'),
(11, -10, -100, -1000, -10000, 'negative1'),
(12, -20, -200, -2000, -20000, 'negative2'),
(13, 0, 0, 0, 0, 'zero'),
(14, null, null, null, null, 'null_values'),
(15, 15, 150, 1500, 15000, 'extra1');

-- Insert string test data
INSERT INTO t_string VALUES
(1, 'apple', 'fruit', 'This is apple text', 'food'),
(2, 'banana', 'fruit', 'This is banana text', 'food'),
(3, 'carrot', 'vegetable', 'This is carrot text', 'food'),
(4, 'dog', 'animal', 'This is dog text', 'pet'),
(5, 'elephant', 'animal', 'This is elephant text', 'wild'),
(6, 'fish', 'animal', 'This is fish text', 'aquatic'),
(7, 'grape', 'fruit', 'This is grape text', 'food'),
(8, 'house', 'building', 'This is house text', 'shelter'),
(9, 'ice', 'water', 'This is ice text', 'cold'),
(10, 'jungle', 'nature', 'This is jungle text', 'wild'),
(11, 'a''b', 'special', 'Text with quote', 'test'),
(12, 'c"d', 'special', 'Text with double quote', 'test'),
(13, 'e\\f', 'special', 'Text with backslash', 'test'),
(14, '', 'empty', 'Empty string test', 'test'),
(15, null, null, null, null);

-- Insert mixed types test data
INSERT INTO t_mixed VALUES
(1, 100, 'str100', 1.1, 1.11, 100.50, '2024-01-01', '2024-01-01 10:00:00', true),
(2, 200, 'str200', 2.2, 2.22, 200.75, '2024-01-02', '2024-01-02 11:00:00', false),
(3, 300, 'str300', 3.3, 3.33, 300.25, '2024-01-03', '2024-01-03 12:00:00', true),
(4, 400, 'str400', 4.4, 4.44, 400.00, '2024-01-04', '2024-01-04 13:00:00', false),
(5, 500, 'str500', 5.5, 5.55, 500.99, '2024-01-05', '2024-01-05 14:00:00', true),
(6, 600, 'str600', 6.6, 6.66, 600.10, '2024-01-06', '2024-01-06 15:00:00', false),
(7, 700, 'str700', 7.7, 7.77, 700.20, '2024-01-07', '2024-01-07 16:00:00', true),
(8, 800, 'str800', 8.8, 8.88, 800.30, '2024-01-08', '2024-01-08 17:00:00', false),
(9, 900, 'str900', 9.9, 9.99, 900.40, '2024-01-09', '2024-01-09 18:00:00', true),
(10, 1000, 'str1000', 10.0, 10.10, 1000.50, '2024-01-10', '2024-01-10 19:00:00', false);

-- Insert large table data (more records for performance testing)
INSERT INTO t_large VALUES
(1, 1, 'active', 85.5, '2024-01-01'),
(2, 1, 'inactive', 72.3, '2024-01-02'),
(3, 2, 'active', 91.2, '2024-01-03'),
(4, 2, 'pending', 68.7, '2024-01-04'),
(5, 3, 'active', 94.1, '2024-01-05'),
(6, 3, 'inactive', 55.9, '2024-01-06'),
(7, 4, 'active', 88.8, '2024-01-07'),
(8, 4, 'pending', 77.4, '2024-01-08'),
(9, 5, 'active', 92.6, '2024-01-09'),
(10, 5, 'inactive', 63.2, '2024-01-10'),
(11, 1, 'active', 89.3, '2024-01-11'),
(12, 2, 'active', 95.7, '2024-01-12'),
(13, 3, 'pending', 71.8, '2024-01-13'),
(14, 4, 'active', 84.4, '2024-01-14'),
(15, 5, 'inactive', 59.6, '2024-01-15'),
(16, 1, 'pending', 78.9, '2024-01-16'),
(17, 2, 'active', 93.2, '2024-01-17'),
(18, 3, 'active', 87.1, '2024-01-18'),
(19, 4, 'inactive', 66.5, '2024-01-19'),
(20, 5, 'active', 90.8, '2024-01-20');

-- Insert dimension table data
INSERT INTO t_dimension VALUES
(1, 'Category A', 'primary'),
(2, 'Category B', 'secondary'),
(3, 'Category C', 'primary'),
(4, 'Category D', 'tertiary'),
(5, 'Category E', 'secondary'),
(6, 'Category F', 'primary'),
(7, 'Category G', 'tertiary'),
(8, 'Category H', 'secondary'),
(9, 'Category I', 'primary'),
(10, 'Category J', 'tertiary');

-- ========== Basic LargeInPredicate Tests ==========

-- Test 1: Basic integer IN transformation
select id, int_col, value from t_int where int_col in (1000, 2000, 3000, 4000) order by id;

-- Test 2: Basic NOT IN transformation
select id, int_col, value from t_int where int_col not in (1000, 2000, 3000, 4000) and int_col is not null order by id;

-- Test 3: String IN transformation
select id, varchar_col, category from t_string where varchar_col in ('apple', 'banana', 'carrot', 'dog') order by id;

-- Test 4: String NOT IN transformation
select id, varchar_col, category from t_string where varchar_col not in ('apple', 'banana', 'carrot', 'dog') and varchar_col is not null order by id;

-- ========== Data Type Compatibility Tests ==========

-- Test 5: Different integer types
select id, tinyint_col from t_int where tinyint_col in (10, 20, 30, 40) order by id;
select id, smallint_col from t_int where smallint_col in (100, 200, 300, 400) order by id;
select id, bigint_col from t_int where bigint_col in (10000, 20000, 30000, 40000) order by id;

-- Test 6: String types
select id, char_col from t_string where char_col in ('fruit', 'animal', 'building', 'nature') order by id;

-- ========== Complex Expression Tests ==========

-- Test 7: Arithmetic expressions
select id, int_col from t_int where (int_col + 1000) in (2000, 3000, 4000, 5000) order by id;

-- Test 8: Function calls
select id, varchar_col from t_string where upper(varchar_col) in ('APPLE', 'BANANA', 'CARROT', 'DOG') order by id;

-- Test 9: CAST expressions
select id, int_col from t_int where cast(int_col as string) in ('1000', '2000', '3000', '4000') order by id;

-- Test 10: CASE expressions
select id, int_col from t_int where case when int_col > 5000 then int_col else 0 end in (6000, 7000, 8000, 9000) order by id;

-- Test 11: COALESCE expressions
select id, tinyint_col from t_int where coalesce(tinyint_col, 0) in (10, 20, 30, 40) order by id;

-- ========== SQL Context Tests ==========

-- Test 12: HAVING clause
select int_col, count(*) as cnt from t_int group by int_col having int_col in (1000, 2000, 3000, 4000) order by int_col;

-- ========== Subquery Tests ==========

-- Test 13: Basic Subquery with LargeInPredicate
select id, int_col from t_int where id in (select id from t_int where int_col in (1000, 2000, 3000, 4000)) order by id;

-- Test 14: Subquery in WHERE clause with different tables
select id, varchar_col from t_string where id in (select id from t_int where int_col in (1000, 2000, 3000, 4000, 5000)) order by id;

-- Test 15: Nested subqueries with multiple LargeInPredicates
select id, int_col from t_int where id in (
    select id from t_large where category_id in (
        select dim_id from t_dimension where dim_id in (1, 2, 3, 4, 5)
    )
) order by id;

-- Test 16: Subquery with aggregation and LargeInPredicate
select id, int_col from t_int where int_col in (
    select max(int_col) from t_int where tinyint_col in (10, 20, 30, 40, 50) group by tinyint_col
) order by id;

-- Test 17: Multiple subqueries with LargeInPredicate
select id, int_col from t_int where 
    id in (select id from t_large where category_id in (1, 2, 3, 4)) 
    and int_col in (select int_col from t_int where tinyint_col in (10, 20, 30, 40))
order by id;

-- Test 18: Subquery with JOIN and LargeInPredicate
select id, int_col from t_int where id in (
    select l.id from t_large l 
    join t_dimension d on l.category_id = d.dim_id 
    where d.dim_id in (1, 2, 3, 4, 5)
) order by id;

-- Test 19: Complex correlated subquery with multiple conditions
select id, int_col from t_int t1 where exists (
    select 1 from t_large t2 
    where t2.id = t1.id 
    and t2.category_id in (1, 2, 3, 4, 5)
    and t2.status in ('active', 'pending', 'completed', 'processing')
) order by id;

-- Test 20: Subquery with window function and LargeInPredicate
select id, int_col from t_int where id in (
    select id from (
        select id, category_id, row_number() over (partition by category_id order by score desc) as rn
        from t_large where category_id in (1, 2, 3, 4, 5)
    ) ranked where rn <= 2
) order by id;

-- Test 21: Subquery with CASE expression and LargeInPredicate
select id, int_col from t_int where id in (
    select id from t_large where 
    case when score > 80 then category_id else 0 end in (1, 2, 3, 4, 5)
) order by id;

-- Test 22: Subquery with HAVING and LargeInPredicate
select id, int_col from t_int where id in (
    select category_id from t_large 
    group by category_id 
    having category_id in (1, 2, 3, 4, 5) and avg(score) > 75
) order by id;

-- Test 23: Complex subquery with CTE and LargeInPredicate
with high_score_categories as (
    select category_id from t_large 
    where score > 85 and category_id in (1, 2, 3, 4, 5, 6, 7, 8)
    group by category_id
)
select id, int_col from t_int where id in (
    select category_id from high_score_categories
) order by id;

-- Test 24: Scalar subquery with LargeInPredicate
select id, int_col, (
    select avg(score) from t_large where category_id in (1, 2, 3, 4, 5)
) as avg_score from t_int where id <= 5 order by id;

-- Test 25: Subquery with DISTINCT and LargeInPredicate
select id, int_col from t_int where id in (
    select distinct category_id from t_large where status in ('active', 'pending', 'completed', 'processing')
) order by id;

-- Test 26: Subquery with ORDER BY and LIMIT with LargeInPredicate
select id, int_col from t_int where id in (
    select id from t_large where category_id in (1, 2, 3, 4, 5)
    order by score desc limit 10
) order by id;

-- Test 27: Multiple level nested subqueries
select id, int_col from t_int where id in (
    select id from t_large where category_id in (
        select dim_id from t_dimension where dim_type in (
            select distinct dim_type from t_dimension where dim_id in (1, 2, 3, 4, 5)
        )
    )
) order by id;

-- Test 28: Complex correlated subquery with aggregation
select id, int_col from t_int t1 where (
    select count(*) from t_large t2 
    where t2.category_id = t1.id and t2.status in ('active', 'pending', 'completed', 'processing')
) > 0 order by id;

-- Test 29: Subquery with multiple JOINs and LargeInPredicate
select id, int_col from t_int where id in (
    select l.id from t_large l
    join t_dimension d1 on l.category_id = d1.dim_id
    join t_dimension d2 on d1.dim_id = d2.dim_id
    where l.category_id in (1, 2, 3, 4, 5) and d1.dim_type in ('primary', 'secondary', 'tertiary', 'quaternary')
) order by id;

-- Test 30: Recursive-like subquery pattern
select id, int_col from t_int where id in (
    select l1.id from t_large l1 where l1.category_id in (
        select l2.category_id from t_large l2 where l2.id in (1, 2, 3, 4, 5)
    )
) order by id;

-- Test 31: Subquery with complex CASE and multiple LargeInPredicates
select id, int_col from t_int where id in (
    select id from t_large where 
    case 
        when score > 90 then category_id
        when score > 80 then category_id + 10
        else 0
    end in (1, 2, 3, 4, 5, 11, 12, 13, 14, 15)
) order by id;

-- Test 32: CTE (Common Table Expression)
with filtered_data as (
    select id, int_col, value from t_int where int_col in (1000, 2000, 3000, 4000)
)
select * from filtered_data order by id;

-- Test 33: JOIN with LargeInPredicate
select t.id, t.int_col, d.dim_name
from t_int t
join t_dimension d on t.id = d.dim_id
where t.int_col in (1000, 2000, 3000, 4000)
order by t.id;

-- Test 34: Multiple predicates with AND
select id, int_col, bigint_col from t_int
where int_col in (1000, 2000, 3000, 4000) and bigint_col > 15000
order by id;

-- ========== Advanced SQL Features Tests ==========

-- Test 35: Window functions
select id, int_col, row_number() over (order by int_col) as rn
from t_int
where int_col in (1000, 2000, 3000, 4000)
order by id;

-- Test 36: UNION with LargeInPredicate
select id, int_col from t_int where int_col in (1000, 2000, 3000, 4000)
union all
select id, int_col from t_int where int_col in (5000, 6000, 7000, 8000)
order by id;

-- Test 37: Aggregation with LargeInPredicate
select count(*), sum(int_col), avg(int_col), min(int_col), max(int_col)
from t_int
where int_col in (1000, 2000, 3000, 4000, 5000, 6000);

-- Test 38: ORDER BY and LIMIT
select id, int_col, value from t_int
where int_col in (1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000)
order by int_col desc
limit 5;

-- ========== Special Values Tests ==========

-- Test 39: Duplicate values in IN list
select id, int_col from t_int where int_col in (1000, 1000, 2000, 2000, 3000, 3000) order by id;

-- Test 40: String with special characters
select id, varchar_col from t_string where varchar_col in ('a''b', 'c"d', 'e\\f', 'normal') order by id;

-- Test 41: Empty string
select id, varchar_col from t_string where varchar_col in ('', 'apple', 'banana', 'carrot') order by id;

-- Test 42: Large number of values (performance test)
select id, category_id from t_large
where category_id in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
order by id;


-- ========== Advanced Subquery Edge Cases ==========

-- Test 43: Subquery with empty result set
select id, int_col from t_int where id in (
    select id from t_large where category_id in (999, 998, 997, 996) -- Non-existent categories
) order by id;

-- Test 44: Subquery with NULL handling
select id, int_col from t_int where id in (
    select case when score > 100 then id else null end from t_large 
    where category_id in (1, 2, 3, 4, 5)
) order by id;

-- Test 46: Subquery with complex mathematical expressions
select id, int_col from t_int where id in (
    select floor(score / 10) from t_large 
    where category_id in (1, 2, 3, 4, 5) and score is not null
) order by id;

-- Test 47: Subquery with string functions and LargeInPredicate
select id, varchar_col from t_string where id in (
    select length(dim_name) from t_dimension 
    where dim_type in ('primary', 'secondary', 'tertiary', 'quaternary')
) order by id;

-- Test 48: Deep nested subquery (4 levels)
select id, int_col from t_int where id in (
    select l.id from t_large l where l.category_id in (
        select d.dim_id from t_dimension d where d.dim_type in (
            select distinct s.category from t_string s where s.id in (
                select i.id from t_int i where i.tinyint_col in (10, 20, 30, 40)
            )
        )
    )
) order by id;

-- Test 49: Subquery with self-join and LargeInPredicate
select id, int_col from t_int where id in (
    select t1.id from t_int t1 join t_int t2 on t1.id = t2.id + 1
    where t1.tinyint_col in (20, 30, 40, 50, 60) and t2.tinyint_col in (10, 20, 30, 40, 50)
) order by id;

-- Test 50: Subquery with complex date/time operations
select id, int_col from t_int where id in (
    select day(created_date) from t_large 
    where category_id in (1, 2, 3, 4, 5) and created_date is not null
) order by id;

-- Test 51: Complex query with multiple LargeInPredicate
select l.id, l.status, l.score
from t_large l
where l.id in (
    select id from t_large where category_id in (1, 2, 3, 4, 5)
)
order by l.id;

-- Test 52: Multi-level JOIN with LargeInPredicate
select t.id, t.int_col, d.dim_name, l.status
from t_int t
join t_dimension d on t.id = d.dim_id
join t_large l on t.id = l.id
where t.int_col in (1000, 2000, 3000, 4000, 5000)
order by t.id;

-- Test 53: NOT IN with NULL values in data
select id, int_col from t_int where int_col not in (1000, 2000, 3000, 4000) order by id;

-- ========== Fallback Scenarios Tests ==========
-- Test 54: Unsupported types - should fallback
select id, date_val from t_mixed where date_val in ('2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04') order by id;


-- Test 55: Mixed types (should fallback to regular InPredicate)
-- Note: These should not cause errors but should fallback to regular processing
select id, varchar_col from t_string where varchar_col in (1, 2, 3, 4) order by id;

-- ========== Complex Scenarios ==========

-- Test 56: Nested aggregation with LargeInPredicate
select category, count(*) as cnt, avg(score) as avg_score
from (
    select
        case
            when category_id in (1, 2, 3, 4, 5) then 'group_a'
            else 'group_b'
        end as category,
        score
    from t_large
    where status in ('active', 'pending', 'inactive', 'suspended')
) t
group by category
order by category;

-- Test 57: Window function with partitioning
select
    id,
    category_id,
    status,
    score,
    row_number() over (partition by category_id order by score desc) as rank_in_category
from t_large
where category_id in (1, 2, 3, 4, 5) and status in ('active', 'pending', 'inactive', 'completed')
order by category_id, rank_in_category;

-- Test 58: Complex JOIN with multiple conditions
select
    t.id,
    t.int_col,
    s.varchar_col,
    d.dim_name,
    l.status,
    l.score
from t_int t
join t_string s on t.id = s.id
join t_dimension d on t.id = d.dim_id
join t_large l on t.id = l.id
where t.int_col in (1000, 2000, 3000, 4000, 5000)
  and s.varchar_col in ('apple', 'banana', 'carrot', 'dog', 'elephant')
  and l.status in ('active', 'pending', 'inactive', 'completed')
order by t.id;

set large_in_predicate_threshold=100000;
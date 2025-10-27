-- name: test_large_in_predicate_to_join @sequential
DROP DATABASE IF EXISTS test_large_in_predicate_to_join;
-- result:
-- !result
CREATE DATABASE test_large_in_predicate_to_join;
-- result:
-- !result
use test_large_in_predicate_to_join;
-- result:
-- !result
set large_in_predicate_threshold=3;
-- result:
-- !result
set enable_large_in_predicate=true;
-- result:
-- !result
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
-- result:
-- !result
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
-- result:
-- !result
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
-- result:
-- !result
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
-- result:
-- !result
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
-- result:
-- !result
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
-- result:
-- !result
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
-- result:
-- !result
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
-- result:
-- !result
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
-- result:
-- !result
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
-- result:
-- !result
select id, int_col, value from t_int where int_col in (1000, 2000, 3000, 4000) order by id;
-- result:
1	1000	value1
2	2000	value2
3	3000	value3
4	4000	value4
-- !result
select id, int_col, value from t_int where int_col not in (1000, 2000, 3000, 4000) and int_col is not null order by id;
-- result:
5	5000	value5
6	6000	value6
7	7000	value7
8	8000	value8
9	9000	value9
10	10000	value10
11	-1000	negative1
12	-2000	negative2
13	0	zero
15	1500	extra1
-- !result
select id, varchar_col, category from t_string where varchar_col in ('apple', 'banana', 'carrot', 'dog') order by id;
-- result:
1	apple	food
2	banana	food
3	carrot	food
4	dog	pet
-- !result
select id, varchar_col, category from t_string where varchar_col not in ('apple', 'banana', 'carrot', 'dog') and varchar_col is not null order by id;
-- result:
5	elephant	wild
6	fish	aquatic
7	grape	food
8	house	shelter
9	ice	cold
10	jungle	wild
11	a'b	test
12	c"d	test
13	e\f	test
14		test
-- !result
select id, tinyint_col from t_int where tinyint_col in (10, 20, 30, 40) order by id;
-- result:
1	10
2	20
3	30
4	40
-- !result
select id, smallint_col from t_int where smallint_col in (100, 200, 300, 400) order by id;
-- result:
1	100
2	200
3	300
4	400
-- !result
select id, bigint_col from t_int where bigint_col in (10000, 20000, 30000, 40000) order by id;
-- result:
1	10000
2	20000
3	30000
4	40000
-- !result
select id, char_col from t_string where char_col in ('fruit', 'animal', 'building', 'nature') order by id;
-- result:
1	fruit
2	fruit
4	animal
5	animal
6	animal
7	fruit
8	building
10	nature
-- !result
select id, int_col from t_int where (int_col + 1000) in (2000, 3000, 4000, 5000) order by id;
-- result:
1	1000
2	2000
3	3000
4	4000
-- !result
select id, varchar_col from t_string where upper(varchar_col) in ('APPLE', 'BANANA', 'CARROT', 'DOG') order by id;
-- result:
1	apple
2	banana
3	carrot
4	dog
-- !result
select id, int_col from t_int where cast(int_col as string) in ('1000', '2000', '3000', '4000') order by id;
-- result:
1	1000
2	2000
3	3000
4	4000
-- !result
select id, int_col from t_int where case when int_col > 5000 then int_col else 0 end in (6000, 7000, 8000, 9000) order by id;
-- result:
6	6000
7	7000
8	8000
9	9000
-- !result
select id, tinyint_col from t_int where coalesce(tinyint_col, 0) in (10, 20, 30, 40) order by id;
-- result:
1	10
2	20
3	30
4	40
-- !result
select int_col, count(*) as cnt from t_int group by int_col having int_col in (1000, 2000, 3000, 4000) order by int_col;
-- result:
1000	1
2000	1
3000	1
4000	1
-- !result
select id, int_col from t_int where id in (select id from t_int where int_col in (1000, 2000, 3000, 4000)) order by id;
-- result:
1	1000
2	2000
3	3000
4	4000
-- !result
select id, varchar_col from t_string where id in (select id from t_int where int_col in (1000, 2000, 3000, 4000, 5000)) order by id;
-- result:
1	apple
2	banana
3	carrot
4	dog
5	elephant
-- !result
select id, int_col from t_int where id in (
    select id from t_large where category_id in (
        select dim_id from t_dimension where dim_id in (1, 2, 3, 4, 5)
    )
) order by id;
-- result:
1	1000
2	2000
3	3000
4	4000
5	5000
6	6000
7	7000
8	8000
9	9000
10	10000
11	-1000
12	-2000
13	0
14	None
15	1500
-- !result
select id, int_col from t_int where int_col in (
    select max(int_col) from t_int where tinyint_col in (10, 20, 30, 40, 50) group by tinyint_col
) order by id;
-- result:
1	1000
2	2000
3	3000
4	4000
5	5000
-- !result
select id, int_col from t_int where 
    id in (select id from t_large where category_id in (1, 2, 3, 4)) 
    and int_col in (select int_col from t_int where tinyint_col in (10, 20, 30, 40))
order by id;
-- result:
1	1000
2	2000
3	3000
4	4000
-- !result
select id, int_col from t_int where id in (
    select l.id from t_large l 
    join t_dimension d on l.category_id = d.dim_id 
    where d.dim_id in (1, 2, 3, 4, 5)
) order by id;
-- result:
1	1000
2	2000
3	3000
4	4000
5	5000
6	6000
7	7000
8	8000
9	9000
10	10000
11	-1000
12	-2000
13	0
14	None
15	1500
-- !result
select id, int_col from t_int t1 where exists (
    select 1 from t_large t2 
    where t2.id = t1.id 
    and t2.category_id in (1, 2, 3, 4, 5)
    and t2.status in ('active', 'pending', 'completed', 'processing')
) order by id;
-- result:
1	1000
3	3000
4	4000
5	5000
7	7000
8	8000
9	9000
11	-1000
12	-2000
13	0
14	None
-- !result
select id, int_col from t_int where id in (
    select id from (
        select id, category_id, row_number() over (partition by category_id order by score desc) as rn
        from t_large where category_id in (1, 2, 3, 4, 5)
    ) ranked where rn <= 2
) order by id;
-- result:
1	1000
5	5000
7	7000
9	9000
11	-1000
12	-2000
14	None
-- !result
select id, int_col from t_int where id in (
    select id from t_large where 
    case when score > 80 then category_id else 0 end in (1, 2, 3, 4, 5)
) order by id;
-- result:
1	1000
3	3000
5	5000
7	7000
9	9000
11	-1000
12	-2000
14	None
-- !result
select id, int_col from t_int where id in (
    select category_id from t_large 
    group by category_id 
    having category_id in (1, 2, 3, 4, 5) and avg(score) > 75
) order by id;
-- result:
1	1000
2	2000
3	3000
4	4000
5	5000
-- !result
with high_score_categories as (
    select category_id from t_large 
    where score > 85 and category_id in (1, 2, 3, 4, 5, 6, 7, 8)
    group by category_id
)
select id, int_col from t_int where id in (
    select category_id from high_score_categories
) order by id;
-- result:
1	1000
2	2000
3	3000
4	4000
5	5000
-- !result
select id, int_col, (
    select avg(score) from t_large where category_id in (1, 2, 3, 4, 5)
) as avg_score from t_int where id <= 5 order by id;
-- result:
1	1000	80.35
2	2000	80.35
3	3000	80.35
4	4000	80.35
5	5000	80.35
-- !result
select id, int_col from t_int where id in (
    select distinct category_id from t_large where status in ('active', 'pending', 'completed', 'processing')
) order by id;
-- result:
1	1000
2	2000
3	3000
4	4000
5	5000
-- !result
select id, int_col from t_int where id in (
    select id from t_large where category_id in (1, 2, 3, 4, 5)
    order by score desc limit 10
) order by id;
-- result:
1	1000
3	3000
5	5000
7	7000
9	9000
11	-1000
12	-2000
-- !result
select id, int_col from t_int where id in (
    select id from t_large where category_id in (
        select dim_id from t_dimension where dim_type in (
            select distinct dim_type from t_dimension where dim_id in (1, 2, 3, 4, 5)
        )
    )
) order by id;
-- result:
1	1000
2	2000
3	3000
4	4000
5	5000
6	6000
7	7000
8	8000
9	9000
10	10000
11	-1000
12	-2000
13	0
14	None
15	1500
-- !result
select id, int_col from t_int t1 where (
    select count(*) from t_large t2 
    where t2.category_id = t1.id and t2.status in ('active', 'pending', 'completed', 'processing')
) > 0 order by id;
-- result:
1	1000
2	2000
3	3000
4	4000
5	5000
-- !result
select id, int_col from t_int where id in (
    select l.id from t_large l
    join t_dimension d1 on l.category_id = d1.dim_id
    join t_dimension d2 on d1.dim_id = d2.dim_id
    where l.category_id in (1, 2, 3, 4, 5) and d1.dim_type in ('primary', 'secondary', 'tertiary', 'quaternary')
) order by id;
-- result:
1	1000
2	2000
3	3000
4	4000
5	5000
6	6000
7	7000
8	8000
9	9000
10	10000
11	-1000
12	-2000
13	0
14	None
15	1500
-- !result
select id, int_col from t_int where id in (
    select l1.id from t_large l1 where l1.category_id in (
        select l2.category_id from t_large l2 where l2.id in (1, 2, 3, 4, 5)
    )
) order by id;
-- result:
1	1000
2	2000
3	3000
4	4000
5	5000
6	6000
11	-1000
12	-2000
13	0
-- !result
select id, int_col from t_int where id in (
    select id from t_large where 
    case 
        when score > 90 then category_id
        when score > 80 then category_id + 10
        else 0
    end in (1, 2, 3, 4, 5, 11, 12, 13, 14, 15)
) order by id;
-- result:
1	1000
3	3000
5	5000
7	7000
9	9000
11	-1000
12	-2000
14	None
-- !result
with filtered_data as (
    select id, int_col, value from t_int where int_col in (1000, 2000, 3000, 4000)
)
select * from filtered_data order by id;
-- result:
1	1000	value1
2	2000	value2
3	3000	value3
4	4000	value4
-- !result
select t.id, t.int_col, d.dim_name
from t_int t
join t_dimension d on t.id = d.dim_id
where t.int_col in (1000, 2000, 3000, 4000)
order by t.id;
-- result:
1	1000	Category A
2	2000	Category B
3	3000	Category C
4	4000	Category D
-- !result
select id, int_col, bigint_col from t_int
where int_col in (1000, 2000, 3000, 4000) and bigint_col > 15000
order by id;
-- result:
2	2000	20000
3	3000	30000
4	4000	40000
-- !result
select id, int_col, row_number() over (order by int_col) as rn
from t_int
where int_col in (1000, 2000, 3000, 4000)
order by id;
-- result:
1	1000	1
2	2000	2
3	3000	3
4	4000	4
-- !result
select id, int_col from t_int where int_col in (1000, 2000, 3000, 4000)
union all
select id, int_col from t_int where int_col in (5000, 6000, 7000, 8000)
order by id;
-- result:
1	1000
2	2000
3	3000
4	4000
5	5000
6	6000
7	7000
8	8000
-- !result
select count(*), sum(int_col), avg(int_col), min(int_col), max(int_col)
from t_int
where int_col in (1000, 2000, 3000, 4000, 5000, 6000);
-- result:
6	21000	3500.0	1000	6000
-- !result
select id, int_col, value from t_int
where int_col in (1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000)
order by int_col desc
limit 5;
-- result:
8	8000	value8
7	7000	value7
6	6000	value6
5	5000	value5
4	4000	value4
-- !result
select id, int_col from t_int where int_col in (1000, 1000, 2000, 2000, 3000, 3000) order by id;
-- result:
1	1000
2	2000
3	3000
-- !result
select id, varchar_col from t_string where varchar_col in ('a''b', 'c"d', 'e\\f', 'normal') order by id;
-- result:
11	a'b
12	c"d
13	e\f
-- !result
select id, varchar_col from t_string where varchar_col in ('', 'apple', 'banana', 'carrot') order by id;
-- result:
1	apple
2	banana
3	carrot
14	
-- !result
select id, category_id from t_large
where category_id in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
order by id;
-- result:
1	1
2	1
3	2
4	2
5	3
6	3
7	4
8	4
9	5
10	5
11	1
12	2
13	3
14	4
15	5
16	1
17	2
18	3
19	4
20	5
-- !result
select id, int_col from t_int where id in (
    select id from t_large where category_id in (999, 998, 997, 996) -- Non-existent categories
) order by id;
-- result:
-- !result
select id, int_col from t_int where id in (
    select case when score > 100 then id else null end from t_large 
    where category_id in (1, 2, 3, 4, 5)
) order by id;
-- result:
-- !result
select id, int_col from t_int where id in (
    select floor(score / 10) from t_large 
    where category_id in (1, 2, 3, 4, 5) and score is not null
) order by id;
-- result:
5	5000
6	6000
7	7000
8	8000
9	9000
-- !result
select id, varchar_col from t_string where id in (
    select length(dim_name) from t_dimension 
    where dim_type in ('primary', 'secondary', 'tertiary', 'quaternary')
) order by id;
-- result:
10	jungle
-- !result
select id, int_col from t_int where id in (
    select l.id from t_large l where l.category_id in (
        select d.dim_id from t_dimension d where d.dim_type in (
            select distinct s.category from t_string s where s.id in (
                select i.id from t_int i where i.tinyint_col in (10, 20, 30, 40)
            )
        )
    )
) order by id;
-- result:
-- !result
select id, int_col from t_int where id in (
    select t1.id from t_int t1 join t_int t2 on t1.id = t2.id + 1
    where t1.tinyint_col in (20, 30, 40, 50, 60) and t2.tinyint_col in (10, 20, 30, 40, 50)
) order by id;
-- result:
2	2000
3	3000
4	4000
5	5000
6	6000
-- !result
select id, int_col from t_int where id in (
    select day(created_date) from t_large 
    where category_id in (1, 2, 3, 4, 5) and created_date is not null
) order by id;
-- result:
1	1000
2	2000
3	3000
4	4000
5	5000
6	6000
7	7000
8	8000
9	9000
10	10000
11	-1000
12	-2000
13	0
14	None
15	1500
-- !result
select l.id, l.status, l.score
from t_large l
where l.id in (
    select id from t_large where category_id in (1, 2, 3, 4, 5)
)
order by l.id;
-- result:
1	active	85.5
2	inactive	72.3
3	active	91.2
4	pending	68.7
5	active	94.1
6	inactive	55.9
7	active	88.8
8	pending	77.4
9	active	92.6
10	inactive	63.2
11	active	89.3
12	active	95.7
13	pending	71.8
14	active	84.4
15	inactive	59.6
16	pending	78.9
17	active	93.2
18	active	87.1
19	inactive	66.5
20	active	90.8
-- !result
select t.id, t.int_col, d.dim_name, l.status
from t_int t
join t_dimension d on t.id = d.dim_id
join t_large l on t.id = l.id
where t.int_col in (1000, 2000, 3000, 4000, 5000)
order by t.id;
-- result:
1	1000	Category A	active
2	2000	Category B	inactive
3	3000	Category C	active
4	4000	Category D	pending
5	5000	Category E	active
-- !result
select id, int_col from t_int where int_col not in (1000, 2000, 3000, 4000) order by id;
-- result:
5	5000
6	6000
7	7000
8	8000
9	9000
10	10000
11	-1000
12	-2000
13	0
15	1500
-- !result
select id, date_val from t_mixed where date_val in ('2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04') order by id;
-- result:
1	2024-01-01
2	2024-01-02
3	2024-01-03
4	2024-01-04
-- !result
select id, varchar_col from t_string where varchar_col in (1, 2, 3, 4) order by id;
-- result:
-- !result
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
-- result:
group_a	20	80.35
-- !result
select
    id,
    category_id,
    status,
    score,
    row_number() over (partition by category_id order by score desc) as rank_in_category
from t_large
where category_id in (1, 2, 3, 4, 5) and status in ('active', 'pending', 'inactive', 'completed')
order by category_id, rank_in_category;
-- result:
11	1	active	89.3	1
1	1	active	85.5	2
16	1	pending	78.9	3
2	1	inactive	72.3	4
12	2	active	95.7	1
17	2	active	93.2	2
3	2	active	91.2	3
4	2	pending	68.7	4
5	3	active	94.1	1
18	3	active	87.1	2
13	3	pending	71.8	3
6	3	inactive	55.9	4
7	4	active	88.8	1
14	4	active	84.4	2
8	4	pending	77.4	3
19	4	inactive	66.5	4
9	5	active	92.6	1
20	5	active	90.8	2
10	5	inactive	63.2	3
15	5	inactive	59.6	4
-- !result
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
-- result:
1	1000	apple	Category A	active	85.5
2	2000	banana	Category B	inactive	72.3
3	3000	carrot	Category C	active	91.2
4	4000	dog	Category D	pending	68.7
5	5000	elephant	Category E	active	94.1
-- !result
set enable_query_cache=true;
-- result:
-- !result
select dim_id, count(dim_id) from t_dimension where dim_id in (1,2,3,4,5) group by dim_id order by dim_id;
-- result:
1	1
2	1
3	1
4	1
5	1
-- !result
set enable_query_cache=false;
-- result:
-- !result
set large_in_predicate_threshold=100000;
-- result:
-- !result
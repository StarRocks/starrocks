-- name: test_complex_default_all_paths @slow
DROP DATABASE IF EXISTS test_complex_default_db;
-- result:
-- !result
CREATE DATABASE test_complex_default_db;
-- result:
-- !result
USE test_complex_default_db;
-- result:
-- !result
CREATE TABLE fast_schema_evolution (
    id INT NOT NULL
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO fast_schema_evolution (id) VALUES (1), (2), (3);
-- result:
-- !result
ALTER TABLE fast_schema_evolution ADD COLUMN arr_col ARRAY<INT> DEFAULT [10, 20, 30];
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
ALTER TABLE fast_schema_evolution ADD COLUMN map_col MAP<INT, VARCHAR(20)> DEFAULT map{1: 'apple', 2: 'banana'};
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
ALTER TABLE fast_schema_evolution ADD COLUMN struct_col STRUCT<f1 INT, f2 VARCHAR(20)> DEFAULT row(100, 'hello');
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
SELECT * FROM fast_schema_evolution ORDER BY id;
-- result:
1	[10,20,30]	{1:"apple",2:"banana"}	{"f1":100,"f2":"hello"}
2	[10,20,30]	{1:"apple",2:"banana"}	{"f1":100,"f2":"hello"}
3	[10,20,30]	{1:"apple",2:"banana"}	{"f1":100,"f2":"hello"}
-- !result
CREATE TABLE nested_complex (
    id INT NOT NULL
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO nested_complex (id) VALUES (1), (2);
-- result:
-- !result
ALTER TABLE nested_complex ADD COLUMN nested_array ARRAY<ARRAY<INT>> DEFAULT [[1, 2], [3, 4, 5]];
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
ALTER TABLE nested_complex ADD COLUMN map_with_array MAP<INT, ARRAY<VARCHAR(20)>> DEFAULT map{1: ['a', 'b'], 2: ['c', 'd']};
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
ALTER TABLE nested_complex ADD COLUMN complex_struct STRUCT<
    id INT, 
    scores ARRAY<INT>, 
    tags MAP<VARCHAR(20), INT>
> DEFAULT row(999, [100, 200], map{'k1': 1, 'k2': 2});
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
SELECT * FROM nested_complex ORDER BY id;
-- result:
1	[[1,2],[3,4,5]]	{1:["a","b"],2:["c","d"]}	{"id":999,"scores":[100,200],"tags":{"k1":1,"k2":2}}
2	[[1,2],[3,4,5]]	{1:["a","b"],2:["c","d"]}	{"id":999,"scores":[100,200],"tags":{"k1":1,"k2":2}}
-- !result
CREATE TABLE map_struct_order (
    id INT NOT NULL
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO map_struct_order (id) VALUES (1), (2);
-- result:
-- !result
ALTER TABLE map_struct_order ADD COLUMN data MAP<INT, STRUCT<s4 INT, ks ARRAY<INT>>> 
DEFAULT map{1: row(2, [1, 2, 3, 4])};
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
ALTER TABLE map_struct_order ADD COLUMN complex_data MAP<INT, STRUCT<
    field_b VARCHAR(20),
    field_a INT,
    nested STRUCT<z INT, a VARCHAR(20)>
>> DEFAULT map{10: row('hello', 100, row(999, 'world'))};
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
SELECT * FROM map_struct_order ORDER BY id;
-- result:
1	{1:{"s4":2,"ks":[1,2,3,4]}}	{10:{"field_b":"hello","field_a":100,"nested":{"z":999,"a":"world"}}}
2	{1:{"s4":2,"ks":[1,2,3,4]}}	{10:{"field_b":"hello","field_a":100,"nested":{"z":999,"a":"world"}}}
-- !result
CREATE TABLE empty_collections (
    id INT NOT NULL
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO empty_collections (id) VALUES (1), (2);
-- result:
-- !result
ALTER TABLE empty_collections ADD COLUMN empty_arr ARRAY<INT> DEFAULT [];
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
ALTER TABLE empty_collections ADD COLUMN empty_map MAP<INT, VARCHAR(20)> DEFAULT map{};
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
ALTER TABLE empty_collections ADD COLUMN struct_with_empty STRUCT<
    id INT,
    arr ARRAY<INT>,
    mp MAP<VARCHAR(20), INT>
> DEFAULT row(0, [], map{});
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
SELECT * FROM empty_collections ORDER BY id;
-- result:
1	[]	{}	{"id":0,"arr":[],"mp":{}}
2	[]	{}	{"id":0,"arr":[],"mp":{}}
-- !result
CREATE TABLE all_primitive_types (
    id INT NOT NULL
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO all_primitive_types (id) VALUES (1);
-- result:
-- !result
ALTER TABLE all_primitive_types ADD COLUMN arr_int ARRAY<INT> DEFAULT [1, 2, 3];
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
ALTER TABLE all_primitive_types ADD COLUMN arr_bigint ARRAY<BIGINT> DEFAULT [1000000000, 2000000000];
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
ALTER TABLE all_primitive_types ADD COLUMN arr_string ARRAY<VARCHAR(20)> DEFAULT ['hello', 'world'];
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
ALTER TABLE all_primitive_types ADD COLUMN arr_double ARRAY<DOUBLE> DEFAULT [1.1, 2.2, 3.3];
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
ALTER TABLE all_primitive_types ADD COLUMN arr_bool ARRAY<BOOLEAN> DEFAULT [true, false, true];
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
ALTER TABLE all_primitive_types ADD COLUMN arr_decimal ARRAY<DECIMAL(10, 2)> DEFAULT [99.99, 199.99, 299.99];
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
ALTER TABLE all_primitive_types ADD COLUMN map_int_str MAP<INT, VARCHAR(20)> DEFAULT map{1: 'one', 2: 'two'};
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
ALTER TABLE all_primitive_types ADD COLUMN map_str_int MAP<VARCHAR(20), INT> DEFAULT map{'a': 10, 'b': 20};
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
ALTER TABLE all_primitive_types ADD COLUMN map_str_bool MAP<VARCHAR(20), BOOLEAN> DEFAULT map{'flag1': true, 'flag2': false};
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
ALTER TABLE all_primitive_types ADD COLUMN struct_mixed STRUCT<
    f_int INT,
    f_bigint BIGINT,
    f_string VARCHAR(20),
    f_double DOUBLE,
    f_bool BOOLEAN,
    f_decimal DECIMAL(10, 2)
> DEFAULT row(100, 1000000000, 'text', 99.99, true, 123.45);
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
SELECT * FROM all_primitive_types;
-- result:
1	[1,2,3]	[1000000000,2000000000]	["hello","world"]	[1.1,2.2,3.3]	[1,0,1]	[99.99,199.99,299.99]	{1:"one",2:"two"}	{"a":10,"b":20}	{"flag1":1,"flag2":0}	{"f_int":100,"f_bigint":1000000000,"f_string":"text","f_double":99.99,"f_bool":1,"f_decimal":123.45}
-- !result
CREATE TABLE nullable_complex (
    id INT NOT NULL
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO nullable_complex (id) VALUES (1), (2);
-- result:
-- !result
ALTER TABLE nullable_complex ADD COLUMN nullable_arr ARRAY<INT> NULL DEFAULT [100, 200];
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
ALTER TABLE nullable_complex ADD COLUMN nullable_map MAP<INT, VARCHAR(20)> NULL DEFAULT map{1: 'test'};
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
ALTER TABLE nullable_complex ADD COLUMN nullable_struct STRUCT<f1 INT> NULL DEFAULT row(999);
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
SELECT * FROM nullable_complex ORDER BY id;
-- result:
1	[100,200]	{1:"test"}	{"f1":999}
2	[100,200]	{1:"test"}	{"f1":999}
-- !result
INSERT INTO nullable_complex (id, nullable_arr, nullable_map, nullable_struct) VALUES (3, NULL, NULL, NULL);
-- result:
-- !result
SELECT * FROM nullable_complex ORDER BY id;
-- result:
1	[100,200]	{1:"test"}	{"f1":999}
2	[100,200]	{1:"test"}	{"f1":999}
3	None	None	None
-- !result
CREATE TABLE reorder_test (
    id INT NOT NULL,
    name VARCHAR(50)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO reorder_test (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
-- result:
-- !result
ALTER TABLE reorder_test ADD COLUMN arr ARRAY<INT> DEFAULT [1, 2, 3];
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
ALTER TABLE reorder_test ADD COLUMN mp MAP<INT, VARCHAR(20)> DEFAULT map{10: 'ten'};
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
ALTER TABLE reorder_test ADD COLUMN st STRUCT<f1 INT, f2 VARCHAR(20)> DEFAULT row(100, 'test');
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
ALTER TABLE reorder_test ORDER BY (id, mp, st, arr, name);
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
SELECT SLEEP(2);
-- result:
1
-- !result
SELECT count(*) FROM reorder_test;
-- result:
3
-- !result
CREATE TABLE pk_basic_defaults (
    user_id INT NOT NULL,
    username VARCHAR(50),
    scores ARRAY<INT> DEFAULT [80, 90, 85],
    tags MAP<VARCHAR(20), VARCHAR(20)> DEFAULT map{'status': 'active', 'level': 'basic'},
    profile STRUCT<age INT, city VARCHAR(20)> DEFAULT row(18, 'Shanghai')
) PRIMARY KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO pk_basic_defaults (user_id, username) VALUES (1, 'user1');
-- result:
-- !result
INSERT INTO pk_basic_defaults (user_id, username) VALUES (2, 'user2');
-- result:
-- !result
SELECT * FROM pk_basic_defaults ORDER BY user_id;
-- result:
1	user1	[80,90,85]	{"level":"basic","status":"active"}	{"age":18,"city":"Shanghai"}
2	user2	[80,90,85]	{"level":"basic","status":"active"}	{"age":18,"city":"Shanghai"}
-- !result
INSERT INTO pk_basic_defaults VALUES 
    (3, 'user3', [100, 95, 98], map{'status': 'vip', 'level': 'premium'}, row(30, 'Beijing'));
-- result:
-- !result
SELECT * FROM pk_basic_defaults ORDER BY user_id;
-- result:
1	user1	[80,90,85]	{"level":"basic","status":"active"}	{"age":18,"city":"Shanghai"}
2	user2	[80,90,85]	{"level":"basic","status":"active"}	{"age":18,"city":"Shanghai"}
3	user3	[100,95,98]	{"level":"premium","status":"vip"}	{"age":30,"city":"Beijing"}
-- !result
CREATE TABLE pk_column_mode (
    order_id INT NOT NULL,
    product_name VARCHAR(50),
    quantity INT DEFAULT '1',
    tags ARRAY<VARCHAR(20)> DEFAULT ['new', 'available'],
    config MAP<VARCHAR(20), INT> DEFAULT map{'priority': 1, 'score': 100},
    details STRUCT<category VARCHAR(20), rating INT> DEFAULT row('electronics', 5)
) PRIMARY KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
-- result:
-- !result
SET partial_update_mode = 'column';
-- result:
-- !result
INSERT INTO pk_column_mode (order_id, product_name) VALUES (1, 'laptop');
-- result:
-- !result
INSERT INTO pk_column_mode (order_id, product_name) VALUES (2, 'phone');
-- result:
-- !result
INSERT INTO pk_column_mode (order_id, product_name) VALUES (3, 'tablet');
-- result:
-- !result
SELECT * FROM pk_column_mode ORDER BY order_id;
-- result:
1	laptop	1	["new","available"]	{"priority":1,"score":100}	{"category":"electronics","rating":5}
2	phone	1	["new","available"]	{"priority":1,"score":100}	{"category":"electronics","rating":5}
3	tablet	1	["new","available"]	{"priority":1,"score":100}	{"category":"electronics","rating":5}
-- !result
ALTER TABLE pk_column_mode ADD COLUMN extras STRUCT<discount DOUBLE, items ARRAY<INT>> DEFAULT row(0.1, [1, 2, 3]);
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
INSERT INTO pk_column_mode (order_id, product_name) VALUES (4, 'mouse');
-- result:
-- !result
INSERT INTO pk_column_mode (order_id, product_name) VALUES (5, 'keyboard');
-- result:
-- !result
SELECT * FROM pk_column_mode ORDER BY order_id;
-- result:
1	laptop	1	["new","available"]	{"priority":1,"score":100}	{"category":"electronics","rating":5}	{"discount":0.1,"items":[1,2,3]}
2	phone	1	["new","available"]	{"priority":1,"score":100}	{"category":"electronics","rating":5}	{"discount":0.1,"items":[1,2,3]}
3	tablet	1	["new","available"]	{"priority":1,"score":100}	{"category":"electronics","rating":5}	{"discount":0.1,"items":[1,2,3]}
4	mouse	1	["new","available"]	{"priority":1,"score":100}	{"category":"electronics","rating":5}	{"discount":0.1,"items":[1,2,3]}
5	keyboard	1	["new","available"]	{"priority":1,"score":100}	{"category":"electronics","rating":5}	{"discount":0.1,"items":[1,2,3]}
-- !result
SET partial_update_mode = 'auto';
-- result:
-- !result
CREATE TABLE pk_row_mode (
    id INT NOT NULL,
    name VARCHAR(50),
    score INT DEFAULT '100',
    level VARCHAR(20) DEFAULT 'bronze',
    tags ARRAY<VARCHAR(20)> DEFAULT ['default', 'new'],
    config MAP<VARCHAR(20), INT> DEFAULT map{'level': 1, 'score': 100},
    details STRUCT<category VARCHAR(20), active BOOLEAN> DEFAULT row('general', true)
) PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO pk_row_mode VALUES 
    (1, 'item1', 500, 'gold', ['premium'], map{'level': 5}, row('special', false));
-- result:
-- !result
SELECT * FROM pk_row_mode ORDER BY id;
-- result:
1	item1	500	gold	["premium"]	{"level":5}	{"category":"special","active":0}
-- !result
INSERT INTO pk_row_mode (id, name) VALUES (2, 'item2');
-- result:
-- !result
INSERT INTO pk_row_mode (id, name) VALUES (3, 'item3');
-- result:
-- !result
SELECT * FROM pk_row_mode ORDER BY id;
-- result:
1	item1	500	gold	["premium"]	{"level":5}	{"category":"special","active":0}
2	item2	100	bronze	["default","new"]	{"level":1,"score":100}	{"category":"general","active":1}
3	item3	100	bronze	["default","new"]	{"level":1,"score":100}	{"category":"general","active":1}
-- !result
CREATE TABLE pk_nested_complex (
    id INT NOT NULL,
    name VARCHAR(50),
    nested_arr ARRAY<ARRAY<INT>> DEFAULT [[1, 2], [3, 4, 5]],
    map_with_struct MAP<INT, STRUCT<val INT, description VARCHAR(20)>> DEFAULT map{1: row(100, 'default')},
    complex_struct STRUCT<id INT, data MAP<VARCHAR(20), ARRAY<INT>>> DEFAULT row(999, map{'scores': [90, 95, 100]})
) PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
-- !result
SET partial_update_mode = 'column';
-- result:
-- !result
INSERT INTO pk_nested_complex (id, name) VALUES (1, 'user1');
-- result:
-- !result
INSERT INTO pk_nested_complex (id, name) VALUES (2, 'user2');
-- result:
-- !result
SELECT * FROM pk_nested_complex ORDER BY id;
-- result:
1	user1	[[1,2],[3,4,5]]	{1:{"val":100,"description":"default"}}	{"id":999,"data":{"scores":[90,95,100]}}
2	user2	[[1,2],[3,4,5]]	{1:{"val":100,"description":"default"}}	{"id":999,"data":{"scores":[90,95,100]}}
-- !result
SET partial_update_mode = 'auto';
-- result:
-- !result
INSERT INTO pk_nested_complex (id, name) VALUES (3, 'user3');
-- result:
-- !result
SELECT * FROM pk_nested_complex ORDER BY id;
-- result:
1	user1	[[1,2],[3,4,5]]	{1:{"val":100,"description":"default"}}	{"id":999,"data":{"scores":[90,95,100]}}
2	user2	[[1,2],[3,4,5]]	{1:{"val":100,"description":"default"}}	{"id":999,"data":{"scores":[90,95,100]}}
3	user3	[[1,2],[3,4,5]]	{1:{"val":100,"description":"default"}}	{"id":999,"data":{"scores":[90,95,100]}}
-- !result
CREATE TABLE pk_empty_collections (
    id INT NOT NULL,
    status VARCHAR(20),
    empty_arr ARRAY<INT> DEFAULT [],
    empty_map MAP<INT, VARCHAR(20)> DEFAULT map{},
    struct_with_empty STRUCT<id INT, arr ARRAY<VARCHAR(20)>, mp MAP<VARCHAR(20), INT>> DEFAULT row(0, [], map{})
) PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
-- !result
SET partial_update_mode = 'column';
-- result:
-- !result
INSERT INTO pk_empty_collections (id, status) VALUES (1, 'active');
-- result:
-- !result
INSERT INTO pk_empty_collections (id, status) VALUES (2, 'inactive');
-- result:
-- !result
SELECT * FROM pk_empty_collections ORDER BY id;
-- result:
1	active	[]	{}	{"id":0,"arr":[],"mp":{}}
2	inactive	[]	{}	{"id":0,"arr":[],"mp":{}}
-- !result
SET partial_update_mode = 'auto';
-- result:
-- !result
CREATE TABLE pk_multi_complex (
    id INT NOT NULL,
    name VARCHAR(50),
    arr_int ARRAY<INT> DEFAULT [10, 20, 30],
    arr_str ARRAY<VARCHAR(20)> DEFAULT ['a', 'b', 'c'],
    map_int_str MAP<INT, VARCHAR(20)> DEFAULT map{1: 'one', 2: 'two'},
    map_str_int MAP<VARCHAR(20), INT> DEFAULT map{'x': 100, 'y': 200},
    struct_simple STRUCT<f1 INT, f2 VARCHAR(20)> DEFAULT row(999, 'default'),
    struct_complex STRUCT<id INT, tags ARRAY<VARCHAR(20)>> DEFAULT row(1, ['tag1', 'tag2'])
) PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
-- !result
SET partial_update_mode = 'column';
-- result:
-- !result
INSERT INTO pk_multi_complex (id, name) VALUES (1, 'test1');
-- result:
-- !result
INSERT INTO pk_multi_complex (id, name) VALUES (2, 'test2');
-- result:
-- !result
SELECT * FROM pk_multi_complex ORDER BY id;
-- result:
1	test1	[10,20,30]	["a","b","c"]	{1:"one",2:"two"}	{"x":100,"y":200}	{"f1":999,"f2":"default"}	{"id":1,"tags":["tag1","tag2"]}
2	test2	[10,20,30]	["a","b","c"]	{1:"one",2:"two"}	{"x":100,"y":200}	{"f1":999,"f2":"default"}	{"id":1,"tags":["tag1","tag2"]}
-- !result
INSERT INTO pk_multi_complex (id, name, arr_int) VALUES (3, 'test3', [100, 200]);
-- result:
-- !result
SELECT id, name, arr_int, arr_str FROM pk_multi_complex ORDER BY id;
-- result:
1	test1	[10,20,30]	["a","b","c"]
2	test2	[10,20,30]	["a","b","c"]
3	test3	[100,200]	["a","b","c"]
-- !result
SET partial_update_mode = 'auto';
-- result:
-- !result
CREATE TABLE pk_decimal_complex (
    id INT NOT NULL,
    name VARCHAR(50),
    prices ARRAY<DECIMAL(10, 2)> DEFAULT [99.99, 199.99, 299.99],
    price_map MAP<VARCHAR(20), DECIMAL(10, 2)> DEFAULT map{'min': 10.00, 'max': 1000.00},
    price_info STRUCT<base DECIMAL(10, 2), tax DECIMAL(5, 2)> DEFAULT row(100.00, 8.25)
) PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
-- !result
SET partial_update_mode = 'column';
-- result:
-- !result
INSERT INTO pk_decimal_complex (id, name) VALUES (1, 'product1');
-- result:
-- !result
INSERT INTO pk_decimal_complex (id, name) VALUES (2, 'product2');
-- result:
-- !result
SELECT * FROM pk_decimal_complex ORDER BY id;
-- result:
1	product1	[99.99,199.99,299.99]	{"max":1000.00,"min":10.00}	{"base":100.00,"tax":8.25}
2	product2	[99.99,199.99,299.99]	{"max":1000.00,"min":10.00}	{"base":100.00,"tax":8.25}
-- !result
SET partial_update_mode = 'auto';
-- result:
-- !result
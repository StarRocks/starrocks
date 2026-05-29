-- name: test_complex_default_correctness @native
DROP DATABASE IF EXISTS test_correctness_db;
-- result:
-- !result
CREATE DATABASE test_correctness_db;
-- result:
-- !result
USE test_correctness_db;
-- result:
-- !result
CREATE TABLE array_primitives (id INT) DUPLICATE KEY(id) 
DISTRIBUTED BY HASH(id) BUCKETS 1 
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO array_primitives VALUES (1), (2);
-- result:
-- !result
ALTER TABLE array_primitives ADD COLUMN arr_tinyint ARRAY<TINYINT> DEFAULT [1, 2, 127];
-- result:
-- !result
ALTER TABLE array_primitives ADD COLUMN arr_smallint ARRAY<SMALLINT> DEFAULT [100, 200, 32767];
-- result:
-- !result
ALTER TABLE array_primitives ADD COLUMN arr_int ARRAY<INT> DEFAULT [1000, 2000, 2147483647];
-- result:
-- !result
ALTER TABLE array_primitives ADD COLUMN arr_bigint ARRAY<BIGINT> DEFAULT [1000000, 2000000, 9223372036854775807];
-- result:
-- !result
ALTER TABLE array_primitives ADD COLUMN arr_float ARRAY<FLOAT> DEFAULT [1.1, 2.2, 3.3];
-- result:
-- !result
ALTER TABLE array_primitives ADD COLUMN arr_double ARRAY<DOUBLE> DEFAULT [1.123456789, 2.987654321, 3.141592653];
-- result:
-- !result
ALTER TABLE array_primitives ADD COLUMN arr_varchar ARRAY<VARCHAR(50)> DEFAULT ['hello', 'world', '你好世界'];
-- result:
-- !result
ALTER TABLE array_primitives ADD COLUMN arr_char ARRAY<CHAR(10)> DEFAULT ['abc', 'def', 'ghi'];
-- result:
-- !result
ALTER TABLE array_primitives ADD COLUMN arr_bool ARRAY<BOOLEAN> DEFAULT [true, false, true, false];
-- result:
-- !result
ALTER TABLE array_primitives ADD COLUMN arr_decimal32 ARRAY<DECIMAL(9, 2)> DEFAULT [123.45, 678.90, 999.99];
-- result:
-- !result
ALTER TABLE array_primitives ADD COLUMN arr_decimal64 ARRAY<DECIMAL(18, 4)> DEFAULT [12345678.1234, 87654321.4321];
-- result:
-- !result
ALTER TABLE array_primitives ADD COLUMN arr_decimal128 ARRAY<DECIMAL(38, 10)> DEFAULT [1234567890.1234567890, 9876543210.0987654321];
-- result:
-- !result
ALTER TABLE array_primitives ADD COLUMN arr_decimal256 ARRAY<DECIMAL(55, 10)> DEFAULT [1123123234567890123412345678901234.1234567890, 98765432109876.0987654321];
-- result:
-- !result
SELECT * FROM array_primitives ORDER BY id;
-- result:
1	[1,2,127]	[100,200,32767]	[1000,2000,2147483647]	[1000000,2000000,9223372036854775807]	[1.1,2.2,3.3]	[1.123456789,2.987654321,3.141592653]	["hello","world","你好世界"]	["abc","def","ghi"]	[1,0,1,0]	[123.45,678.90,999.99]	[12345678.1234,87654321.4321]	[1234567890.1234567890,9876543210.0987654321]	[1123123234567890123412345678901234.1234567890,98765432109876.0987654321]
2	[1,2,127]	[100,200,32767]	[1000,2000,2147483647]	[1000000,2000000,9223372036854775807]	[1.1,2.2,3.3]	[1.123456789,2.987654321,3.141592653]	["hello","world","你好世界"]	["abc","def","ghi"]	[1,0,1,0]	[123.45,678.90,999.99]	[12345678.1234,87654321.4321]	[1234567890.1234567890,9876543210.0987654321]	[1123123234567890123412345678901234.1234567890,98765432109876.0987654321]
-- !result
CREATE TABLE map_types (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO map_types VALUES (1), (2);
-- result:
-- !result
ALTER TABLE map_types ADD COLUMN map_int_str MAP<INT, VARCHAR(20)> DEFAULT map{1: 'one', 2: 'two', 100: 'hundred'};
-- result:
-- !result
ALTER TABLE map_types ADD COLUMN map_str_int MAP<VARCHAR(20), INT> DEFAULT map{'alice': 25, 'bob': 30, 'charlie': 35};
-- result:
-- !result
ALTER TABLE map_types ADD COLUMN map_int_double MAP<INT, DOUBLE> DEFAULT map{1: 1.1, 2: 2.2, 3: 3.3};
-- result:
-- !result
ALTER TABLE map_types ADD COLUMN map_str_bool MAP<VARCHAR(20), BOOLEAN> DEFAULT map{'active': true, 'disabled': false};
-- result:
-- !result
ALTER TABLE map_types ADD COLUMN map_int_decimal MAP<INT, DECIMAL(10, 2)> DEFAULT map{1: 99.99, 2: 199.99, 3: 299.99};
-- result:
-- !result
SELECT * FROM map_types ORDER BY id;
-- result:
1	{1:"one",2:"two",100:"hundred"}	{"alice":25,"bob":30,"charlie":35}	{1:1.1,2:2.2,3:3.3}	{"active":1,"disabled":0}	{1:99.99,2:199.99,3:299.99}
2	{1:"one",2:"two",100:"hundred"}	{"alice":25,"bob":30,"charlie":35}	{1:1.1,2:2.2,3:3.3}	{"active":1,"disabled":0}	{1:99.99,2:199.99,3:299.99}
-- !result
CREATE TABLE struct_field_order (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO struct_field_order VALUES (1), (2);
-- result:
-- !result
ALTER TABLE struct_field_order ADD COLUMN st1 STRUCT<z INT, a VARCHAR(20)> DEFAULT row(999, 'test');
-- result:
-- !result
ALTER TABLE struct_field_order ADD COLUMN st2 STRUCT<field_c INT, field_b VARCHAR(20), field_a DOUBLE> 
DEFAULT row(100, 'middle', 3.14);
-- result:
-- !result
ALTER TABLE struct_field_order ADD COLUMN st3 STRUCT<s4 INT, ks ARRAY<INT>> DEFAULT row(2, [1, 2, 3, 4]);
-- result:
-- !result
ALTER TABLE struct_field_order ADD COLUMN st4 STRUCT<
    zebra INT,
    apple VARCHAR(20),
    monkey BOOLEAN,
    banana DOUBLE
> DEFAULT row(10, 'fruit', true, 2.5);
-- result:
-- !result
SELECT * FROM struct_field_order ORDER BY id;
-- result:
1	{"z":999,"a":"test"}	{"field_c":100,"field_b":"middle","field_a":3.14}	{"s4":2,"ks":[1,2,3,4]}	{"zebra":10,"apple":"fruit","monkey":1,"banana":2.5}
2	{"z":999,"a":"test"}	{"field_c":100,"field_b":"middle","field_a":3.14}	{"s4":2,"ks":[1,2,3,4]}	{"zebra":10,"apple":"fruit","monkey":1,"banana":2.5}
-- !result
CREATE TABLE nested_arrays (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO nested_arrays VALUES (1), (2);
-- result:
-- !result
ALTER TABLE nested_arrays ADD COLUMN arr2_int ARRAY<ARRAY<INT>> DEFAULT [[1, 2, 3], [4, 5], [6, 7, 8, 9]];
-- result:
-- !result
ALTER TABLE nested_arrays ADD COLUMN arr2_str ARRAY<ARRAY<VARCHAR(20)>> DEFAULT [['a', 'b'], ['c', 'd', 'e'], ['f']];
-- result:
-- !result
ALTER TABLE nested_arrays ADD COLUMN arr3_int ARRAY<ARRAY<ARRAY<INT>>> DEFAULT [[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]];
-- result:
-- !result
ALTER TABLE nested_arrays ADD COLUMN arr2_empty ARRAY<ARRAY<INT>> DEFAULT [[], [1, 2], []];
-- result:
-- !result
SELECT * FROM nested_arrays ORDER BY id;
-- result:
1	[[1,2,3],[4,5],[6,7,8,9]]	[["a","b"],["c","d","e"],["f"]]	[[[1,2],[3]],[[4,5,6]],[[7],[8,9]]]	[[],[1,2],[]]
2	[[1,2,3],[4,5],[6,7,8,9]]	[["a","b"],["c","d","e"],["f"]]	[[[1,2],[3]],[[4,5,6]],[[7],[8,9]]]	[[],[1,2],[]]
-- !result
CREATE TABLE array_struct (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO array_struct VALUES (1), (2);
-- result:
-- !result
ALTER TABLE array_struct ADD COLUMN arr_st1 ARRAY<STRUCT<id INT, name VARCHAR(20)>> 
DEFAULT [row(1, 'alice'), row(2, 'bob'), row(3, 'charlie')];
-- result:
-- !result
ALTER TABLE array_struct ADD COLUMN arr_st2 ARRAY<STRUCT<score INT, age INT, name VARCHAR(20)>> 
DEFAULT [row(95, 25, 'student1'), row(88, 26, 'student2')];
-- result:
-- !result
ALTER TABLE array_struct ADD COLUMN arr_st3 ARRAY<STRUCT<id INT, tags ARRAY<VARCHAR(20)>>> 
DEFAULT [row(1, ['tag1', 'tag2']), row(2, ['tag3', 'tag4', 'tag5'])];
-- result:
-- !result
SELECT * FROM array_struct ORDER BY id;
-- result:
1	[{"id":1,"name":"alice"},{"id":2,"name":"bob"},{"id":3,"name":"charlie"}]	[{"score":95,"age":25,"name":"student1"},{"score":88,"age":26,"name":"student2"}]	[{"id":1,"tags":["tag1","tag2"]},{"id":2,"tags":["tag3","tag4","tag5"]}]
2	[{"id":1,"name":"alice"},{"id":2,"name":"bob"},{"id":3,"name":"charlie"}]	[{"score":95,"age":25,"name":"student1"},{"score":88,"age":26,"name":"student2"}]	[{"id":1,"tags":["tag1","tag2"]},{"id":2,"tags":["tag3","tag4","tag5"]}]
-- !result
CREATE TABLE map_array (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO map_array VALUES (1), (2);
-- result:
-- !result
ALTER TABLE map_array ADD COLUMN mp_arr1 MAP<INT, ARRAY<VARCHAR(20)>> 
DEFAULT map{1: ['a', 'b', 'c'], 2: ['d', 'e'], 3: ['f', 'g', 'h', 'i']};
-- result:
-- !result
ALTER TABLE map_array ADD COLUMN mp_arr2 MAP<VARCHAR(20), ARRAY<INT>> 
DEFAULT map{'scores': [90, 85, 92], 'ages': [25, 30, 35]};
-- result:
-- !result
ALTER TABLE map_array ADD COLUMN mp_arr3 MAP<INT, ARRAY<ARRAY<INT>>> 
DEFAULT map{1: [[1, 2], [3, 4]], 2: [[5], [6, 7, 8]]};
-- result:
-- !result
SELECT * FROM map_array ORDER BY id;
-- result:
1	{1:["a","b","c"],2:["d","e"],3:["f","g","h","i"]}	{"ages":[25,30,35],"scores":[90,85,92]}	{1:[[1,2],[3,4]],2:[[5],[6,7,8]]}
2	{1:["a","b","c"],2:["d","e"],3:["f","g","h","i"]}	{"ages":[25,30,35],"scores":[90,85,92]}	{1:[[1,2],[3,4]],2:[[5],[6,7,8]]}
-- !result
CREATE TABLE map_struct (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO map_struct VALUES (1), (2);
-- result:
-- !result
ALTER TABLE map_struct ADD COLUMN mp_st1 MAP<INT, STRUCT<name VARCHAR(20), age INT>> 
DEFAULT map{1: row('alice', 25), 2: row('bob', 30)};
-- result:
-- !result
ALTER TABLE map_struct ADD COLUMN mp_st2 MAP<INT, STRUCT<z INT, a VARCHAR(20)>> 
DEFAULT map{10: row(999, 'test'), 20: row(888, 'demo')};
-- result:
-- !result
ALTER TABLE map_struct ADD COLUMN mp_st3 MAP<INT, STRUCT<s4 INT, ks ARRAY<INT>>> 
DEFAULT map{1: row(2, [1, 2, 3, 4]), 2: row(5, [10, 20, 30])};
-- result:
-- !result
ALTER TABLE map_struct ADD COLUMN mp_st4 MAP<INT, STRUCT<
    field_b VARCHAR(20),
    field_a INT,
    nested STRUCT<z INT, a VARCHAR(20)>
>> DEFAULT map{10: row('hello', 100, row(999, 'world'))};
-- result:
-- !result
SELECT * FROM map_struct ORDER BY id;
-- result:
1	{1:{"name":"alice","age":25},2:{"name":"bob","age":30}}	{10:{"z":999,"a":"test"},20:{"z":888,"a":"demo"}}	{1:{"s4":2,"ks":[1,2,3,4]},2:{"s4":5,"ks":[10,20,30]}}	{10:{"field_b":"hello","field_a":100,"nested":{"z":999,"a":"world"}}}
2	{1:{"name":"alice","age":25},2:{"name":"bob","age":30}}	{10:{"z":999,"a":"test"},20:{"z":888,"a":"demo"}}	{1:{"s4":2,"ks":[1,2,3,4]},2:{"s4":5,"ks":[10,20,30]}}	{10:{"field_b":"hello","field_a":100,"nested":{"z":999,"a":"world"}}}
-- !result
CREATE TABLE struct_nested (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO struct_nested VALUES (1), (2);
-- result:
-- !result
ALTER TABLE struct_nested ADD COLUMN st_arr STRUCT<id INT, scores ARRAY<INT>, name VARCHAR(20)> 
DEFAULT row(1, [90, 85, 95], 'student');
-- result:
-- !result
ALTER TABLE struct_nested ADD COLUMN st_map STRUCT<id INT, attributes MAP<VARCHAR(20), VARCHAR(20)>> 
DEFAULT row(1, map{'color': 'red', 'size': 'large'});
-- result:
-- !result
ALTER TABLE struct_nested ADD COLUMN st_both STRUCT<
    tags ARRAY<VARCHAR(20)>,
    id INT,
    metadata MAP<VARCHAR(20), INT>
> DEFAULT row(['tag1', 'tag2'], 100, map{'version': 1, 'priority': 5});
-- result:
-- !result
ALTER TABLE struct_nested ADD COLUMN st_deep STRUCT<
    name VARCHAR(20),
    items ARRAY<STRUCT<id INT, value VARCHAR(20)>>
> DEFAULT row('container', [row(1, 'item1'), row(2, 'item2')]);
-- result:
-- !result
SELECT * FROM struct_nested ORDER BY id;
-- result:
1	{"id":1,"scores":[90,85,95],"name":"student"}	{"id":1,"attributes":{"color":"red","size":"large"}}	{"tags":["tag1","tag2"],"id":100,"metadata":{"priority":5,"version":1}}	{"name":"container","items":[{"id":1,"value":"item1"},{"id":2,"value":"item2"}]}
2	{"id":1,"scores":[90,85,95],"name":"student"}	{"id":1,"attributes":{"color":"red","size":"large"}}	{"tags":["tag1","tag2"],"id":100,"metadata":{"priority":5,"version":1}}	{"name":"container","items":[{"id":1,"value":"item1"},{"id":2,"value":"item2"}]}
-- !result
CREATE TABLE three_level_nesting (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO three_level_nesting VALUES (1), (2);
-- result:
-- !result
ALTER TABLE three_level_nesting ADD COLUMN arr3 ARRAY<ARRAY<ARRAY<INT>>> 
DEFAULT [[[1, 2], [3]], [[4, 5]]];
-- result:
-- !result
ALTER TABLE three_level_nesting ADD COLUMN arr_map_arr ARRAY<MAP<INT, ARRAY<INT>>> 
DEFAULT [map{1: [1, 2], 2: [3, 4]}, map{10: [10, 20]}];
-- result:
-- !result
ALTER TABLE three_level_nesting ADD COLUMN map_arr_st MAP<INT, ARRAY<STRUCT<id INT, name VARCHAR(20)>>> 
DEFAULT map{1: [row(1, 'a'), row(2, 'b')]};
-- result:
-- !result
ALTER TABLE three_level_nesting ADD COLUMN st_map_arr STRUCT<
    id INT,
    data MAP<VARCHAR(20), ARRAY<INT>>
> DEFAULT row(1, map{'scores': [90, 95, 100]});
-- result:
-- !result
ALTER TABLE three_level_nesting ADD COLUMN map_st_arr MAP<INT, STRUCT<
    name VARCHAR(20),
    vals ARRAY<INT>
>> DEFAULT map{1: row('test', [1, 2, 3])};
-- result:
-- !result
SELECT * FROM three_level_nesting ORDER BY id;
-- result:
1	[[[1,2],[3]],[[4,5]]]	[{1:[1,2],2:[3,4]},{10:[10,20]}]	{1:[{"id":1,"name":"a"},{"id":2,"name":"b"}]}	{"id":1,"data":{"scores":[90,95,100]}}	{1:{"name":"test","vals":[1,2,3]}}
2	[[[1,2],[3]],[[4,5]]]	[{1:[1,2],2:[3,4]},{10:[10,20]}]	{1:[{"id":1,"name":"a"},{"id":2,"name":"b"}]}	{"id":1,"data":{"scores":[90,95,100]}}	{1:{"name":"test","vals":[1,2,3]}}
-- !result
CREATE TABLE empty_collections (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO empty_collections VALUES (1), (2);
-- result:
-- !result
ALTER TABLE empty_collections ADD COLUMN arr_empty ARRAY<INT> DEFAULT [];
-- result:
-- !result
ALTER TABLE empty_collections ADD COLUMN map_empty MAP<INT, VARCHAR(20)> DEFAULT map{};
-- result:
-- !result
ALTER TABLE empty_collections ADD COLUMN st_empty_arr STRUCT<id INT, tags ARRAY<VARCHAR(20)>> 
DEFAULT row(1, []);
-- result:
-- !result
ALTER TABLE empty_collections ADD COLUMN st_empty_map STRUCT<id INT, attrs MAP<VARCHAR(20), INT>> 
DEFAULT row(1, map{});
-- result:
-- !result
ALTER TABLE empty_collections ADD COLUMN st_all_empty STRUCT<
    id INT,
    arr ARRAY<INT>,
    mp MAP<VARCHAR(20), INT>
> DEFAULT row(0, [], map{});
-- result:
-- !result
ALTER TABLE empty_collections ADD COLUMN arr_mixed ARRAY<ARRAY<INT>> 
DEFAULT [[], [1, 2], [], [3, 4, 5]];
-- result:
-- !result
ALTER TABLE empty_collections ADD COLUMN map_empty_arr MAP<INT, ARRAY<VARCHAR(20)>> 
DEFAULT map{1: [], 2: ['a', 'b'], 3: []};
-- result:
-- !result
SELECT * FROM empty_collections ORDER BY id;
-- result:
1	[]	{}	{"id":1,"tags":[]}	{"id":1,"attrs":{}}	{"id":0,"arr":[],"mp":{}}	[[],[1,2],[],[3,4,5]]	{1:[],2:["a","b"],3:[]}
2	[]	{}	{"id":1,"tags":[]}	{"id":1,"attrs":{}}	{"id":0,"arr":[],"mp":{}}	[[],[1,2],[],[3,4,5]]	{1:[],2:["a","b"],3:[]}
-- !result
CREATE TABLE decimal_complex (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO decimal_complex VALUES (1), (2);
-- result:
-- !result
ALTER TABLE decimal_complex ADD COLUMN arr_dec32 ARRAY<DECIMAL(9, 2)> DEFAULT [99.99, 199.99, 299.99];
-- result:
-- !result
ALTER TABLE decimal_complex ADD COLUMN arr_dec64 ARRAY<DECIMAL(18, 4)> DEFAULT [9999.9999, 19999.9999];
-- result:
-- !result
ALTER TABLE decimal_complex ADD COLUMN arr_dec128 ARRAY<DECIMAL(38, 10)> DEFAULT [123456.1234567890, 654321.0987654321];
-- result:
-- !result
ALTER TABLE decimal_complex ADD COLUMN arr_dec256 ARRAY<DECIMAL(45, 10)> DEFAULT [123456789012.1234567890, 987654321098.0987654321];
-- result:
-- !result
ALTER TABLE decimal_complex ADD COLUMN map_dec MAP<VARCHAR(20), DECIMAL(10, 2)> 
DEFAULT map{'price': 99.99, 'tax': 8.25, 'total': 108.24};
-- result:
-- !result
ALTER TABLE decimal_complex ADD COLUMN map_dec256 MAP<INT, DECIMAL(45, 10)> 
DEFAULT map{1: 99999999999.9999999999, 2: 12345678901.1234567890};
-- result:
-- !result
ALTER TABLE decimal_complex ADD COLUMN st_dec STRUCT<
    amount DECIMAL(10, 2),
    rate DECIMAL(5, 4),
    result DECIMAL(15, 6),
    large DECIMAL(45, 10)
> DEFAULT row(1000.50, 0.0525, 52.526250, 123456789012.1234567890);
-- result:
-- !result
ALTER TABLE decimal_complex ADD COLUMN arr_st_dec ARRAY<STRUCT<id INT, price DECIMAL(10, 2)>> 
DEFAULT [row(1, 99.99), row(2, 199.99), row(3, 299.99)];
-- result:
-- !result
SELECT * FROM decimal_complex ORDER BY id;
-- result:
1	[99.99,199.99,299.99]	[9999.9999,19999.9999]	[123456.1234567890,654321.0987654321]	[123456789012.1234567890,987654321098.0987654321]	{"price":99.99,"tax":8.25,"total":108.24}	{1:99999999999.9999999999,2:12345678901.1234567890}	{"amount":1000.50,"rate":0.0525,"result":52.526250,"large":123456789012.1234567890}	[{"id":1,"price":99.99},{"id":2,"price":199.99},{"id":3,"price":299.99}]
2	[99.99,199.99,299.99]	[9999.9999,19999.9999]	[123456.1234567890,654321.0987654321]	[123456789012.1234567890,987654321098.0987654321]	{"price":99.99,"tax":8.25,"total":108.24}	{1:99999999999.9999999999,2:12345678901.1234567890}	{"amount":1000.50,"rate":0.0525,"result":52.526250,"large":123456789012.1234567890}	[{"id":1,"price":99.99},{"id":2,"price":199.99},{"id":3,"price":299.99}]
-- !result
CREATE TABLE large_defaults (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO large_defaults VALUES (1);
-- result:
-- !result
ALTER TABLE large_defaults ADD COLUMN large_arr ARRAY<INT> 
DEFAULT [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20];
-- result:
-- !result
ALTER TABLE large_defaults ADD COLUMN large_map MAP<INT, VARCHAR(20)> 
DEFAULT map{1: 'one', 2: 'two', 3: 'three', 4: 'four', 5: 'five', 
            6: 'six', 7: 'seven', 8: 'eight', 9: 'nine', 10: 'ten'};
-- result:
-- !result
ALTER TABLE large_defaults ADD COLUMN large_struct STRUCT<
    f1 INT, f2 INT, f3 INT, f4 INT, f5 INT,
    f6 VARCHAR(20), f7 VARCHAR(20), f8 DOUBLE, f9 BOOLEAN, f10 DECIMAL(10, 2)
> DEFAULT row(1, 2, 3, 4, 5, 'six', 'seven', 8.8, true, 10.50);
-- result:
-- !result
ALTER TABLE large_defaults ADD COLUMN complex_combo MAP<INT, STRUCT<
    id INT,
    tags ARRAY<VARCHAR(20)>,
    scores ARRAY<INT>,
    metadata MAP<VARCHAR(20), VARCHAR(20)>
>> DEFAULT map{
    1: row(100, ['tag1', 'tag2', 'tag3'], [90, 85, 95], map{'type': 'A', 'level': 'high'}),
    2: row(200, ['tag4', 'tag5'], [80, 88], map{'type': 'B', 'level': 'medium'})
};
-- result:
-- !result
SELECT * FROM large_defaults;
-- result:
1	[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]	{1:"one",2:"two",3:"three",4:"four",5:"five",6:"six",7:"seven",8:"eight",9:"nine",10:"ten"}	{"f1":1,"f2":2,"f3":3,"f4":4,"f5":5,"f6":"six","f7":"seven","f8":8.8,"f9":1,"f10":10.50}	{1:{"id":100,"tags":["tag1","tag2","tag3"],"scores":[90,85,95],"metadata":{"level":"high","type":"A"}},2:{"id":200,"tags":["tag4","tag5"],"scores":[80,88],"metadata":{"level":"medium","type":"B"}}}
-- !result
CREATE TABLE special_strings (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO special_strings VALUES (1);
-- result:
-- !result
ALTER TABLE special_strings ADD COLUMN arr_unicode ARRAY<VARCHAR(50)> 
DEFAULT ['你好', '世界', 'こんにちは', '안녕하세요', '🚀', '⭐'];
-- result:
-- !result
ALTER TABLE special_strings ADD COLUMN arr_special ARRAY<VARCHAR(50)> 
DEFAULT ['it\'s', 'path\\to\\file', '"quoted"'];
-- result:
-- !result
ALTER TABLE special_strings ADD COLUMN map_unicode MAP<VARCHAR(20), VARCHAR(20)> 
DEFAULT map{'中文': '测试', 'emoji': '😀🎉'};
-- result:
-- !result
ALTER TABLE special_strings ADD COLUMN st_unicode STRUCT<name VARCHAR(50), description VARCHAR(100)> 
DEFAULT row('用户名', '这是一个包含中文的描述信息');
-- result:
-- !result
SELECT * FROM special_strings;
-- result:
1	["你好","世界","こんにちは","안녕하세요","🚀","⭐"]	["it's","path\\to\\file","\"quoted\""]	{"emoji":"😀🎉","中文":"测试"}	{"name":"用户名","description":"这是一个包含中文的描述信息"}
-- !result
CREATE TABLE deep_nesting_1 (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO deep_nesting_1 VALUES (1);
-- result:
-- !result
ALTER TABLE deep_nesting_1 ADD COLUMN arr_st_map_arr ARRAY<STRUCT<
    id INT,
    data MAP<INT, ARRAY<INT>>
>> DEFAULT [
    row(1, map{10: [1, 2, 3], 20: [4, 5]}),
    row(2, map{30: [6, 7, 8, 9]})
];
-- result:
-- !result
ALTER TABLE deep_nesting_1 ADD COLUMN map_st_arr_map MAP<INT, STRUCT<
    tags ARRAY<VARCHAR(20)>,
    attrs MAP<VARCHAR(20), INT>
>> DEFAULT map{
    1: row(['tag1', 'tag2'], map{'score': 90, 'level': 5}),
    2: row(['tag3'], map{'score': 85, 'level': 3})
};
-- result:
-- !result
ALTER TABLE deep_nesting_1 ADD COLUMN st_map_st_arr STRUCT<
    name VARCHAR(20),
    data MAP<INT, STRUCT<id INT, vals ARRAY<INT>>>
> DEFAULT row('test', map{1: row(100, [1, 2, 3]), 2: row(200, [4, 5])});
-- result:
-- !result
SELECT * FROM deep_nesting_1;
-- result:
1	[{"id":1,"data":{10:[1,2,3],20:[4,5]}},{"id":2,"data":{30:[6,7,8,9]}}]	{1:{"tags":["tag1","tag2"],"attrs":{"level":5,"score":90}},2:{"tags":["tag3"],"attrs":{"level":3,"score":85}}}	{"name":"test","data":{1:{"id":100,"vals":[1,2,3]},2:{"id":200,"vals":[4,5]}}}
-- !result
CREATE TABLE deep_nesting_2 (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO deep_nesting_2 VALUES (1);
-- result:
-- !result
ALTER TABLE deep_nesting_2 ADD COLUMN st_map_arr_map STRUCT<
    id INT,
    nested MAP<INT, ARRAY<MAP<VARCHAR(20), INT>>>
> DEFAULT row(1, map{
    10: [map{'a': 1, 'b': 2}, map{'c': 3}],
    20: [map{'d': 4, 'e': 5, 'f': 6}]
});
-- result:
-- !result
ALTER TABLE deep_nesting_2 ADD COLUMN arr_map_st_map_arr ARRAY<MAP<INT, STRUCT<
    name VARCHAR(20),
    data MAP<VARCHAR(20), ARRAY<INT>>
>>> DEFAULT [
    map{1: row('item1', map{'scores': [90, 85], 'ages': [25, 30]})},
    map{2: row('item2', map{'scores': [95, 92, 88]})}
];
-- result:
-- !result
ALTER TABLE deep_nesting_2 ADD COLUMN map_arr_st_arr_st MAP<INT, ARRAY<STRUCT<
    id INT,
    items ARRAY<STRUCT<k VARCHAR(20), v INT>>
>>> DEFAULT map{
    1: [row(10, [row('a', 1), row('b', 2)])],
    2: [row(20, [row('c', 3)]), row(30, [row('d', 4), row('e', 5)])]
};
-- result:
-- !result
SELECT * FROM deep_nesting_2;
-- result:
1	{"id":1,"nested":{10:[{"a":1,"b":2},{"c":3}],20:[{"d":4,"e":5,"f":6}]}}	[{1:{"name":"item1","data":{"ages":[25,30],"scores":[90,85]}}},{2:{"name":"item2","data":{"scores":[95,92,88]}}}]	{1:[{"id":10,"items":[{"k":"a","v":1},{"k":"b","v":2}]}],2:[{"id":20,"items":[{"k":"c","v":3}]},{"id":30,"items":[{"k":"d","v":4},{"k":"e","v":5}]}]}
-- !result
CREATE TABLE complex_field_order (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO complex_field_order VALUES (1);
-- result:
-- !result
ALTER TABLE complex_field_order ADD COLUMN arr_st_order ARRAY<STRUCT<
    zebra INT,
    apple VARCHAR(20),
    monkey BOOLEAN,
    banana ARRAY<INT>
>> DEFAULT [
    row(10, 'fruit', true, [1, 2, 3]),
    row(20, 'animal', false, [4, 5])
];
-- result:
-- !result
ALTER TABLE complex_field_order ADD COLUMN map_st_nested MAP<INT, STRUCT<
    outer_z INT,
    outer_a VARCHAR(20),
    nested_st STRUCT<inner_y INT, inner_b VARCHAR(20)>
>> DEFAULT map{
    1: row(100, 'test', row(200, 'nested'))
};
-- result:
-- !result
ALTER TABLE complex_field_order ADD COLUMN st_map_st STRUCT<
    id INT,
    data MAP<INT, STRUCT<s4 INT, ks ARRAY<INT>, aa VARCHAR(20)>>
> DEFAULT row(1, map{
    10: row(2, [1, 2, 3, 4], 'test'),
    20: row(5, [10, 20], 'demo')
});
-- result:
-- !result
SELECT * FROM complex_field_order;
-- result:
1	[{"zebra":10,"apple":"fruit","monkey":1,"banana":[1,2,3]},{"zebra":20,"apple":"animal","monkey":0,"banana":[4,5]}]	{1:{"outer_z":100,"outer_a":"test","nested_st":{"inner_y":200,"inner_b":"nested"}}}	{"id":1,"data":{10:{"s4":2,"ks":[1,2,3,4],"aa":"test"},20:{"s4":5,"ks":[10,20],"aa":"demo"}}}
-- !result
CREATE TABLE mixed_empty_nested (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO mixed_empty_nested VALUES (1);
-- result:
-- !result
ALTER TABLE mixed_empty_nested ADD COLUMN arr3_mixed ARRAY<ARRAY<ARRAY<INT>>> 
DEFAULT [[[1, 2], []], [[]], [[3], [4, 5]]];
-- result:
-- !result
ALTER TABLE mixed_empty_nested ADD COLUMN map_arr_st_empty MAP<INT, ARRAY<STRUCT<
    id INT,
    tags ARRAY<VARCHAR(20)>
>>> DEFAULT map{
    1: [row(1, []), row(2, ['tag1', 'tag2'])],
    2: []
};
-- result:
-- !result
ALTER TABLE mixed_empty_nested ADD COLUMN st_mixed_empty STRUCT<
    id INT,
    empty_arr ARRAY<INT>,
    empty_map MAP<VARCHAR(20), INT>,
    non_empty_arr ARRAY<INT>,
    non_empty_map MAP<VARCHAR(20), INT>
> DEFAULT row(1, [], map{}, [1, 2], map{'k': 100});
-- result:
-- !result
ALTER TABLE mixed_empty_nested ADD COLUMN arr_map_st_empty ARRAY<MAP<INT, STRUCT<
    id INT,
    data ARRAY<INT>
>>> DEFAULT [
    map{},
    map{1: row(10, [1, 2, 3])},
    map{},
    map{2: row(20, [])}
];
-- result:
-- !result
SELECT * FROM mixed_empty_nested;
-- result:
1	[[[1,2],[]],[[]],[[3],[4,5]]]	{1:[{"id":1,"tags":[]},{"id":2,"tags":["tag1","tag2"]}],2:[]}	{"id":1,"empty_arr":[],"empty_map":{},"non_empty_arr":[1,2],"non_empty_map":{"k":100}}	[{},{1:{"id":10,"data":[1,2,3]}},{},{2:{"id":20,"data":[]}}]
-- !result
CREATE TABLE six_level_nesting (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO six_level_nesting VALUES (1);
-- result:
-- !result
ALTER TABLE six_level_nesting ADD COLUMN level6_1 ARRAY<MAP<INT, STRUCT<
    id INT,
    nested ARRAY<MAP<INT, ARRAY<INT>>>
>>> DEFAULT [
    map{1: row(100, [map{1: [1, 2]}, map{2: [3, 4]}])}
];
-- result:
-- !result
ALTER TABLE six_level_nesting ADD COLUMN level6_2 MAP<INT, STRUCT<
    name VARCHAR(20),
    data MAP<INT, ARRAY<STRUCT<id INT, vals ARRAY<INT>>>>
>> DEFAULT map{
    1: row('test', map{10: [row(1, [1, 2, 3])]})
};
-- result:
-- !result
ALTER TABLE six_level_nesting ADD COLUMN level6_3 STRUCT<
    id INT,
    deep ARRAY<MAP<INT, STRUCT<
        name VARCHAR(20),
        data MAP<INT, ARRAY<INT>>
    >>>
> DEFAULT row(1, [
    map{1: row('item', map{10: [1, 2], 20: [3, 4]})}
]);
-- result:
-- !result
SELECT * FROM six_level_nesting;
-- result:
1	[{1:{"id":100,"nested":[{1:[1,2]},{2:[3,4]}]}}]	{1:{"name":"test","data":{10:[{"id":1,"vals":[1,2,3]}]}}}	{"id":1,"deep":[{1:{"name":"item","data":{10:[1,2],20:[3,4]}}}]}
-- !result
CREATE TABLE decimal256_nested (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO decimal256_nested VALUES (1);
-- result:
-- !result
ALTER TABLE decimal256_nested ADD COLUMN arr_st_dec256 ARRAY<STRUCT<
    id INT,
    amount DECIMAL(45, 10)
>> DEFAULT [
    row(1, 12345678901234.1234567890),
    row(2, 98765432109876.0987654321)
];
-- result:
-- !result
ALTER TABLE decimal256_nested ADD COLUMN map_st_dec256 MAP<INT, STRUCT<
    price DECIMAL(45, 10),
    history ARRAY<DECIMAL(45, 10)>
>> DEFAULT map{
    1: row(99999999999.9999999999, [11111111111.1111111111, 22222222222.2222222222])
};
-- result:
-- !result
ALTER TABLE decimal256_nested ADD COLUMN st_map_arr_dec256 STRUCT<
    name VARCHAR(20),
    data MAP<INT, ARRAY<STRUCT<id INT, val DECIMAL(45, 10)>>>
> DEFAULT row('test', map{
    1: [row(10, 12345.1234567890), row(20, 67890.0987654321)]
});
-- result:
-- !result
SELECT * FROM decimal256_nested;
-- result:
1	[{"id":1,"amount":12345678901234.1234567890},{"id":2,"amount":98765432109876.0987654321}]	{1:{"price":99999999999.9999999999,"history":[11111111111.1111111111,22222222222.2222222222]}}	{"name":"test","data":{1:[{"id":10,"val":12345.1234567890},{"id":20,"val":67890.0987654321}]}}
-- !result
SELECT 
    id,
    arr_tinyint[1] AS first_tinyint,
    arr_tinyint[2] AS second_tinyint,
    arr_varchar[1] AS first_varchar,
    arr_decimal256[1] AS first_decimal256
FROM test_correctness_db.array_primitives
ORDER BY id;
-- result:
1	1	2	hello	1123123234567890123412345678901234.1234567890
2	1	2	hello	1123123234567890123412345678901234.1234567890
-- !result
SELECT
    id,
    arr2_int[1] AS first_subarray,
    arr2_int[1][1] AS first_elem_of_first_subarray,
    arr2_int[2][2] AS second_elem_of_second_subarray,
    arr3_int[1][1][1] AS deeply_nested_element
FROM test_correctness_db.nested_arrays
ORDER BY id;
-- result:
1	[1,2,3]	1	5	1
2	[1,2,3]	1	5	1
-- !result
SELECT
    id,
    map_int_str[1] AS value_for_key_1,
    map_int_str[100] AS value_for_key_100,
    map_str_int['alice'] AS alice_value,
    map_int_decimal[2] AS decimal_for_key_2
FROM test_correctness_db.map_types
ORDER BY id;
-- result:
1	one	hundred	25	199.99
2	one	hundred	25	199.99
-- !result
SELECT
    id,
    st1.z AS st1_field_z,
    st1.a AS st1_field_a,
    st3.s4 AS st3_field_s4,
    st3.ks AS st3_field_ks_array,
    st3.ks[1] AS st3_first_ks_element
FROM test_correctness_db.struct_field_order
ORDER BY id;
-- result:
1	999	test	2	[1,2,3,4]	1
2	999	test	2	[1,2,3,4]	1
-- !result
SELECT
    id,
    st_arr.id AS struct_id,
    st_arr.scores AS scores_array,
    st_arr.scores[1] AS first_score,
    st_map.attributes['color'] AS color_value,
    st_deep.items[1].id AS first_item_id,
    st_deep.items[2].value AS second_item_value
FROM struct_nested
ORDER BY id;
-- result:
1	1	[90,85,95]	90	red	1	item2
2	1	[90,85,95]	90	red	1	item2
-- !result
SELECT
    id,
    arr_st1[1].id AS first_person_id,
    arr_st1[1].name AS first_person_name,
    arr_st2[2].score AS second_student_score,
    arr_st3[1].tags[2] AS first_record_second_tag
FROM test_correctness_db.array_struct
ORDER BY id;
-- result:
1	1	alice	88	tag2
2	1	alice	88	tag2
-- !result
SELECT
    id,
    mp_arr1[1] AS array_for_key_1,
    mp_arr1[1][2] AS second_elem_of_array_for_key_1,
    mp_arr2['scores'][1] AS first_score,
    mp_arr3[1][1][2] AS nested_array_element
FROM test_correctness_db.map_array
ORDER BY id;
-- result:
1	["a","b","c"]	b	90	2
2	["a","b","c"]	b	90	2
-- !result
SELECT
    id,
    mp_st1[1].name AS person_1_name,
    mp_st1[2].age AS person_2_age,
    mp_st3[1].s4 AS key_1_s4_value,
    mp_st3[1].ks[3] AS key_1_ks_third_element,
    mp_st4[10].nested.z AS nested_struct_z_field
FROM test_correctness_db.map_struct
ORDER BY id;
-- result:
1	alice	30	2	3	999
2	alice	30	2	3	999
-- !result
SELECT
    id,
    arr_st_map_arr[1].data[10][2] AS deep_array_access,
    map_st_arr_map[1].tags[1] AS map_struct_array_elem,
    st_map_st_arr.data[1].vals[2] AS struct_map_struct_array_elem,
    map_arr_st_arr_st[1][1].items[1].k AS very_deep_struct_field
FROM test_correctness_db.deep_nesting_1, test_correctness_db.deep_nesting_2
ORDER BY id;
-- result:
E: (1064, "Getting analyzing error. Detail message: Column 'id' is ambiguous.")
-- !result
SELECT
    id,
    arr_st_dec256[1].amount AS first_amount,
    map_st_dec256[1].price AS price_for_key_1,
    map_st_dec256[1].history[2] AS second_history_value,
    st_map_arr_dec256.data[1][1].val AS nested_decimal_value
FROM test_correctness_db.decimal256_nested
ORDER BY id;
-- result:
1	12345678901234.1234567890	99999999999.9999999999	22222222222.2222222222	12345.1234567890
-- !result
CREATE TABLE t_ns (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num"="1", "fast_schema_evolution"="true");
-- result:
-- !result
INSERT INTO t_ns VALUES (1), (2);
-- result:
-- !result
ALTER TABLE t_ns
ADD COLUMN st_outer STRUCT<
  k1 INT,
  mid STRUCT<
    a INT,
    sub STRUCT<x INT, y VARCHAR(20)>
  >,
  k2 VARCHAR(20)
>
DEFAULT row(10, row(1, row(7, 'yy')), 'end');
-- result:
-- !result
SELECT
  id,
  st_outer.mid,
  st_outer.mid.sub
FROM t_ns
ORDER BY id;
-- result:
1	{"a":1,"sub":{"x":7,"y":"yy"}}	{"x":7,"y":"yy"}
2	{"a":1,"sub":{"x":7,"y":"yy"}}	{"x":7,"y":"yy"}
-- !result
SELECT
  id,
  st_outer.mid,
  st_outer.mid.sub.x
FROM t_ns
ORDER BY id;
-- result:
1	{"a":1,"sub":{"x":7,"y":"yy"}}	7
2	{"a":1,"sub":{"x":7,"y":"yy"}}	7
-- !result
SELECT
  id,
  st_outer.mid.sub
FROM t_ns
ORDER BY id;
-- result:
1	{"x":7,"y":"yy"}
2	{"x":7,"y":"yy"}
-- !result
SET cbo_prune_subfield=true;
-- result:
-- !result
CREATE TABLE t_prune_mix (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num"="1", "fast_schema_evolution"="true");
-- result:
-- !result
INSERT INTO t_prune_mix VALUES (1), (2);
-- result:
-- !result
ALTER TABLE t_prune_mix
ADD COLUMN st_mix STRUCT<
  sid INT,
  arr ARRAY<STRUCT<s INT, meta STRUCT<x INT, y VARCHAR(10)>>>,
  mp MAP<INT, STRUCT<m VARCHAR(10), vals ARRAY<INT>>>,
  name VARCHAR(10)
>
DEFAULT row(
  1,
  [row(10, row(7, 'a')), row(20, row(8, 'b'))],
  map{1: row('v', [1, 2, 3]), 2: row('w', [9])},
  'n'
);
-- result:
-- !result
SELECT
  id,
  st_mix.arr[1] AS first_elem,
  st_mix.arr[1].meta.x AS first_elem_meta_x
FROM t_prune_mix
ORDER BY id;
-- result:
1	{"s":10,"meta":{"x":7,"y":"a"}}	7
2	{"s":10,"meta":{"x":7,"y":"a"}}	7
-- !result
SELECT
  id,
  st_mix.arr[2].meta.y AS second_elem_meta_y
FROM t_prune_mix
ORDER BY id;
-- result:
1	b
2	b
-- !result
SELECT
  id,
  st_mix.mp[1].m AS m1,
  st_mix.mp[1].vals[2] AS mp1_vals_2
FROM t_prune_mix
ORDER BY id;
-- result:
1	v	2
2	v	2
-- !result
SELECT
  id,
  st_mix.mp AS mp_all,
  st_mix.mp[2].vals AS mp2_vals
FROM t_prune_mix
ORDER BY id;
-- result:
1	{1:{"m":"v","vals":[1,2,3]},2:{"m":"w","vals":[9]}}	[9]
2	{1:{"m":"v","vals":[1,2,3]},2:{"m":"w","vals":[9]}}	[9]
-- !result
SELECT
  id,
  st_mix.name AS name,
  st_mix.arr[1].s AS first_s
FROM t_prune_mix
ORDER BY id;
-- result:
1	n	10
2	n	10
-- !result
CREATE TABLE t_prune_arr_mp (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num"="1", "fast_schema_evolution"="true");
-- result:
-- !result
INSERT INTO t_prune_arr_mp VALUES (1), (2);
-- result:
-- !result
ALTER TABLE t_prune_arr_mp
ADD COLUMN arr_mp ARRAY<MAP<INT, STRUCT<a INT, b STRUCT<x INT, y VARCHAR(10)>>>>
DEFAULT [
  map{1: row(10, row(7, 'aa')), 2: row(20, row(8, 'bb'))}
];
-- result:
-- !result
SELECT
  id,
  arr_mp[1][2].b.y AS y_2
FROM t_prune_arr_mp
ORDER BY id;
-- result:
1	bb
2	bb
-- !result
SELECT
  id,
  arr_mp[1][1] AS v1,
  arr_mp[1][1].b.x AS v1_b_x
FROM t_prune_arr_mp
ORDER BY id;
-- result:
1	{"a":10,"b":{"x":7,"y":"aa"}}	7
2	{"a":10,"b":{"x":7,"y":"aa"}}	7
-- !result
CREATE TABLE t_prune_mp_arr (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num"="1", "fast_schema_evolution"="true");
-- result:
-- !result
INSERT INTO t_prune_mp_arr VALUES (1), (2);
-- result:
-- !result
ALTER TABLE t_prune_mp_arr
ADD COLUMN mp_arr MAP<VARCHAR(10), ARRAY<STRUCT<id INT, meta STRUCT<p INT, q VARCHAR(10)>>>>
DEFAULT map{
  'k': [row(1, row(7, 'qq')), row(2, row(8, 'ww'))]
};
-- result:
-- !result
SELECT
  id,
  mp_arr['k'][2].meta.q AS q2
FROM t_prune_mp_arr
ORDER BY id;
-- result:
1	ww
2	ww
-- !result
SELECT
  id,
  mp_arr['k'][1] AS e1,
  mp_arr['k'][1].meta.p AS e1_p
FROM t_prune_mp_arr
ORDER BY id;
-- result:
1	{"id":1,"meta":{"p":7,"q":"qq"}}	7
2	{"id":1,"meta":{"p":7,"q":"qq"}}	7
-- !result
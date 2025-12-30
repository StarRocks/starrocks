-- name: test_varbinary_default
DROP DATABASE if exists test_varbinary_default_db;
-- result:
-- !result
CREATE DATABASE test_varbinary_default_db;
-- result:
-- !result
USE test_varbinary_default_db;
-- result:
-- !result
CREATE TABLE test_varbinary_create (
    id INT NOT NULL,
    name VARCHAR(50),
    binary_col VARBINARY DEFAULT "",
    binary_no_default VARBINARY
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO test_varbinary_create (id, name) VALUES (1, 'user1');
-- result:
-- !result
INSERT INTO test_varbinary_create (id, name, binary_no_default) VALUES (2, 'user2', 'custom');
-- result:
-- !result
INSERT INTO test_varbinary_create VALUES (3, 'user3', 'explicit', 'data');
-- result:
-- !result
INSERT INTO test_varbinary_create VALUES (4, 'user4', '', NULL);
-- result:
-- !result
SELECT 
    id,
    name,
    binary_col,
    binary_col IS NULL AS col_is_null,
    HEX(binary_col),
    LENGTH(binary_col) AS col_len,
    binary_no_default,
    binary_no_default IS NULL AS no_default_is_null,
    LENGTH(binary_no_default) AS no_default_len,
    HEX(binary_col) AS col_hex
FROM test_varbinary_create
ORDER BY id;
-- result:
1	user1		0		0	None	1	None	
2	user2		0		0	custom	0	6	
3	user3	explicit	0	6578706C69636974	8	data	0	4	6578706C69636974
4	user4		0		0	None	1	None	
-- !result
CREATE TABLE test_varbinary_alter (
    id INT NOT NULL,
    name VARCHAR(50)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
-- result:
-- !result
INSERT INTO test_varbinary_alter VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
-- result:
-- !result
ALTER TABLE test_varbinary_alter ADD COLUMN binary_col VARBINARY DEFAULT "";
-- result:
-- !result
SELECT 
    id,
    name,
    binary_col,
    binary_col IS NULL AS is_null,
    LENGTH(binary_col) AS len
FROM test_varbinary_alter
ORDER BY id;
-- result:
1	alice		0	0
2	bob		0	0
3	charlie		0	0
-- !result
INSERT INTO test_varbinary_alter VALUES (4, 'david', 'new_data');
-- result:
-- !result
SELECT 
    id,
    name,
    CAST(binary_col AS VARCHAR) AS binary_str,
    LENGTH(binary_col) AS len
FROM test_varbinary_alter
ORDER BY id;
-- result:
1	alice		0
2	bob		0
3	charlie		0
4	david	new_data	8
-- !result
CREATE TABLE test_varbinary_traditional (
    id INT NOT NULL,
    name VARCHAR(50)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "false");
-- result:
-- !result
INSERT INTO test_varbinary_traditional VALUES (1, 'eve'), (2, 'frank');
-- result:
-- !result
ALTER TABLE test_varbinary_traditional ADD COLUMN binary_col VARBINARY DEFAULT "";
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
select count(*) from test_varbinary_traditional;
-- result:
2
-- !result
CREATE TABLE test_varbinary_primary (
    order_id INT NOT NULL,
    customer VARCHAR(50),
    signature VARBINARY DEFAULT ""
) PRIMARY KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO test_varbinary_primary (order_id, customer) VALUES (1, 'customer1');
-- result:
-- !result
INSERT INTO test_varbinary_primary VALUES (2, 'customer2', 'sign123');
-- result:
-- !result
INSERT INTO test_varbinary_primary VALUES (3, 'customer3', '');
-- result:
-- !result
SELECT 
    order_id,
    customer,
    CAST(signature AS VARCHAR) AS sign_str,
    LENGTH(signature) AS sign_len,
    signature IS NULL AS is_null
FROM test_varbinary_primary
ORDER BY order_id;
-- result:
1	customer1		0	0
2	customer2	sign123	7	0
3	customer3		0	0
-- !result
CREATE TABLE test_varbinary_edge (
    id INT NOT NULL,
    binary_col VARBINARY DEFAULT ""
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO test_varbinary_edge (id) VALUES (1);
-- result:
-- !result
INSERT INTO test_varbinary_edge VALUES (2, NULL);
-- result:
-- !result
INSERT INTO test_varbinary_edge VALUES (3, '');
-- result:
-- !result
SELECT 
    id,
    binary_col IS NULL AS is_null,
    LENGTH(binary_col) AS len,
    HEX(binary_col) AS hex,
    CAST(binary_col AS VARCHAR) AS str
FROM test_varbinary_edge
ORDER BY id;
-- result:
1	0	0		
2	1	None	None	None
3	0	0		
-- !result
CREATE TABLE test_varbinary_invalid1 (
    id INT,
    binary_col VARBINARY DEFAULT "test"
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
E: (1064, 'Getting analyzing error. Detail message: Invalid default value for \'binary_col\': Type \'VARBINARY\' only supports empty string "" as default value.')
-- !result
CREATE TABLE test_varbinary_invalid2 (
    id INT,
    binary_col VARBINARY DEFAULT x'CAFE'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
E: (1064, "Getting syntax error at line 3, column 33. Detail message: Unexpected input 'x'CAFE'', the most similar input is {'NULL', 'CURRENT_TIMESTAMP', SINGLE_QUOTED_TEXT, DOUBLE_QUOTED_TEXT, '('}.")
-- !result
CREATE TABLE test_varbinary_sized (
    id INT NOT NULL,
    small_binary VARBINARY(10) DEFAULT "",
    large_binary VARBINARY(100) DEFAULT ""
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO test_varbinary_sized (id) VALUES (1);
-- result:
-- !result
INSERT INTO test_varbinary_sized VALUES (2, 'short', 'this_is_longer');
-- result:
-- !result
SELECT 
    id,
    CAST(small_binary AS VARCHAR) AS small_val,
    LENGTH(small_binary) AS small_len,
    CAST(large_binary AS VARCHAR) AS large_val,
    LENGTH(large_binary) AS large_len
FROM test_varbinary_sized
ORDER BY id;
-- result:
1		0		0
2	short	5	this_is_longer	14
-- !result
CREATE TABLE with_default (
    id INT,
    binary_col VARBINARY DEFAULT ""
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
-- !result
CREATE TABLE without_default (
    id INT,
    binary_col VARBINARY
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO with_default (id) VALUES (1);
-- result:
-- !result
INSERT INTO without_default (id) VALUES (1);
-- result:
-- !result
SELECT 
    'with_default' AS type,
    binary_col IS NULL AS is_null,
    LENGTH(binary_col) AS len
FROM with_default
UNION ALL
SELECT 
    'without_default',
    binary_col IS NULL,
    LENGTH(binary_col)
FROM without_default;
-- result:
without_default	1	None
with_default	0	0
-- !result
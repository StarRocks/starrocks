-- name: test_complex_default_all_paths
-- Comprehensive test for complex type (ARRAY/MAP/STRUCT) default values

DROP DATABASE IF EXISTS test_complex_default_db;
CREATE DATABASE test_complex_default_db;
USE test_complex_default_db;

-- ====================================================================================
-- SECTION 1: ALTER TABLE ADD COLUMN with Complex Type Defaults
-- ====================================================================================

CREATE TABLE fast_schema_evolution (
    id INT NOT NULL
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");

INSERT INTO fast_schema_evolution (id) VALUES (1), (2), (3);

ALTER TABLE fast_schema_evolution ADD COLUMN arr_col ARRAY<INT> DEFAULT [10, 20, 30];
ALTER TABLE fast_schema_evolution ADD COLUMN map_col MAP<INT, VARCHAR(20)> DEFAULT map{1: 'apple', 2: 'banana'};
ALTER TABLE fast_schema_evolution ADD COLUMN struct_col STRUCT<f1 INT, f2 VARCHAR(20)> DEFAULT row(100, 'hello');

SELECT * FROM fast_schema_evolution ORDER BY id;

-- ====================================================================================
-- SECTION 2: Nested Complex Types
-- ====================================================================================

CREATE TABLE nested_complex (
    id INT NOT NULL
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");

INSERT INTO nested_complex (id) VALUES (1), (2);

ALTER TABLE nested_complex ADD COLUMN nested_array ARRAY<ARRAY<INT>> DEFAULT [[1, 2], [3, 4, 5]];
ALTER TABLE nested_complex ADD COLUMN map_with_array MAP<INT, ARRAY<VARCHAR(20)>> DEFAULT map{1: ['a', 'b'], 2: ['c', 'd']};
ALTER TABLE nested_complex ADD COLUMN complex_struct STRUCT<
    id INT, 
    scores ARRAY<INT>, 
    tags MAP<VARCHAR(20), INT>
> DEFAULT row(999, [100, 200], map{'k1': 1, 'k2': 2});

SELECT * FROM nested_complex ORDER BY id;

-- ====================================================================================
-- SECTION 3: MAP with STRUCT - Field Order Preservation Test
-- ====================================================================================
CREATE TABLE map_struct_order (
    id INT NOT NULL
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");

INSERT INTO map_struct_order (id) VALUES (1), (2);

ALTER TABLE map_struct_order ADD COLUMN www123 MAP<INT, STRUCT<s4 INT, ks ARRAY<INT>>>
DEFAULT map{1: row(2, [1, 2, 3, 4])};

ALTER TABLE map_struct_order ADD COLUMN complex_data MAP<INT, STRUCT<
    field_b VARCHAR(20),
    field_a INT,
    nested STRUCT<z INT, a VARCHAR(20)>
>> DEFAULT map{10: row('hello', 100, row(999, 'world'))};

SELECT * FROM map_struct_order ORDER BY id;

-- ====================================================================================
-- SECTION 4: Empty Collections
-- ====================================================================================

CREATE TABLE empty_collections (
    id INT NOT NULL
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");

INSERT INTO empty_collections (id) VALUES (1), (2);

ALTER TABLE empty_collections ADD COLUMN empty_arr ARRAY<INT> DEFAULT [];
ALTER TABLE empty_collections ADD COLUMN empty_map MAP<INT, VARCHAR(20)> DEFAULT map{};
ALTER TABLE empty_collections ADD COLUMN struct_with_empty STRUCT<
    id INT,
    arr ARRAY<INT>,
    mp MAP<VARCHAR(20), INT>
> DEFAULT row(0, [], map{});

SELECT * FROM empty_collections ORDER BY id;

-- ====================================================================================
-- SECTION 5: All Primitive Types in Complex Types
-- ====================================================================================

CREATE TABLE all_primitive_types (
    id INT NOT NULL
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");

INSERT INTO all_primitive_types (id) VALUES (1);

ALTER TABLE all_primitive_types ADD COLUMN arr_int ARRAY<INT> DEFAULT [1, 2, 3];
ALTER TABLE all_primitive_types ADD COLUMN arr_bigint ARRAY<BIGINT> DEFAULT [1000000000, 2000000000];
ALTER TABLE all_primitive_types ADD COLUMN arr_string ARRAY<VARCHAR(20)> DEFAULT ['hello', 'world'];
ALTER TABLE all_primitive_types ADD COLUMN arr_double ARRAY<DOUBLE> DEFAULT [1.1, 2.2, 3.3];
ALTER TABLE all_primitive_types ADD COLUMN arr_bool ARRAY<BOOLEAN> DEFAULT [true, false, true];
ALTER TABLE all_primitive_types ADD COLUMN arr_decimal ARRAY<DECIMAL(10, 2)> DEFAULT [99.99, 199.99, 299.99];

ALTER TABLE all_primitive_types ADD COLUMN map_int_str MAP<INT, VARCHAR(20)> DEFAULT map{1: 'one', 2: 'two'};
ALTER TABLE all_primitive_types ADD COLUMN map_str_int MAP<VARCHAR(20), INT> DEFAULT map{'a': 10, 'b': 20};
ALTER TABLE all_primitive_types ADD COLUMN map_str_bool MAP<VARCHAR(20), BOOLEAN> DEFAULT map{'flag1': true, 'flag2': false};

ALTER TABLE all_primitive_types ADD COLUMN struct_mixed STRUCT<
    f_int INT,
    f_bigint BIGINT,
    f_string VARCHAR(20),
    f_double DOUBLE,
    f_bool BOOLEAN,
    f_decimal DECIMAL(10, 2)
> DEFAULT row(100, 1000000000, 'text', 99.99, true, 123.45);

SELECT * FROM all_primitive_types;

-- ====================================================================================
-- SECTION 6: Nullable Complex Types
-- ====================================================================================

CREATE TABLE nullable_complex (
    id INT NOT NULL
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");

INSERT INTO nullable_complex (id) VALUES (1), (2);

ALTER TABLE nullable_complex ADD COLUMN nullable_arr ARRAY<INT> NULL DEFAULT [100, 200];
ALTER TABLE nullable_complex ADD COLUMN nullable_map MAP<INT, VARCHAR(20)> NULL DEFAULT map{1: 'test'};
ALTER TABLE nullable_complex ADD COLUMN nullable_struct STRUCT<f1 INT> NULL DEFAULT row(999);

SELECT * FROM nullable_complex ORDER BY id;

INSERT INTO nullable_complex (id, nullable_arr, nullable_map, nullable_struct) VALUES (3, NULL, NULL, NULL);
SELECT * FROM nullable_complex ORDER BY id;

-- ====================================================================================
-- SECTION 7: REORDER COLUMNS with Complex Type Defaults
-- ====================================================================================

CREATE TABLE reorder_test (
    id INT NOT NULL,
    name VARCHAR(50)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");

INSERT INTO reorder_test (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');

ALTER TABLE reorder_test ADD COLUMN arr ARRAY<INT> DEFAULT [1, 2, 3];
ALTER TABLE reorder_test ADD COLUMN mp MAP<INT, VARCHAR(20)> DEFAULT map{10: 'ten'};
ALTER TABLE reorder_test ADD COLUMN st STRUCT<f1 INT, f2 VARCHAR(20)> DEFAULT row(100, 'test');

ALTER TABLE reorder_test ORDER BY (id, mp, st, arr, name);

SELECT SLEEP(2);

SELECT count(*) FROM reorder_test;

-- ====================================================================================
-- SECTION 8: PRIMARY KEY Table - INSERT with Partial Columns
-- ====================================================================================

CREATE TABLE pk_basic_defaults (
    user_id INT NOT NULL,
    username VARCHAR(50),
    scores ARRAY<INT> DEFAULT [80, 90, 85],
    tags MAP<VARCHAR(20), VARCHAR(20)> DEFAULT map{'status': 'active', 'level': 'basic'},
    profile STRUCT<age INT, city VARCHAR(20)> DEFAULT row(18, 'Shanghai')
) PRIMARY KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO pk_basic_defaults (user_id, username) VALUES (1, 'user1');
INSERT INTO pk_basic_defaults (user_id, username) VALUES (2, 'user2');

SELECT * FROM pk_basic_defaults ORDER BY user_id;

INSERT INTO pk_basic_defaults VALUES 
    (3, 'user3', [100, 95, 98], map{'status': 'vip', 'level': 'premium'}, row(30, 'Beijing'));

SELECT * FROM pk_basic_defaults ORDER BY user_id;

-- ====================================================================================
-- SECTION 9: PRIMARY KEY Table - Column Mode Partial Update
-- ====================================================================================

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

SET partial_update_mode = 'column';

INSERT INTO pk_column_mode (order_id, product_name) VALUES (1, 'laptop');
INSERT INTO pk_column_mode (order_id, product_name) VALUES (2, 'phone');
INSERT INTO pk_column_mode (order_id, product_name) VALUES (3, 'tablet');

SELECT * FROM pk_column_mode ORDER BY order_id;

ALTER TABLE pk_column_mode ADD COLUMN extras STRUCT<discount DOUBLE, items ARRAY<INT>> DEFAULT row(0.1, [1, 2, 3]);

INSERT INTO pk_column_mode (order_id, product_name) VALUES (4, 'mouse');
INSERT INTO pk_column_mode (order_id, product_name) VALUES (5, 'keyboard');

SELECT * FROM pk_column_mode ORDER BY order_id;

SET partial_update_mode = 'auto';

-- ====================================================================================
-- SECTION 10: PRIMARY KEY Table - Row Mode Partial Update
-- ====================================================================================

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

INSERT INTO pk_row_mode VALUES 
    (1, 'item1', 500, 'gold', ['premium'], map{'level': 5}, row('special', false));

SELECT * FROM pk_row_mode ORDER BY id;

INSERT INTO pk_row_mode (id, name) VALUES (2, 'item2');
INSERT INTO pk_row_mode (id, name) VALUES (3, 'item3');

SELECT * FROM pk_row_mode ORDER BY id;

-- ====================================================================================
-- SECTION 11: PRIMARY KEY Table - Nested Complex Types
-- ====================================================================================

CREATE TABLE pk_nested_complex (
    id INT NOT NULL,
    name VARCHAR(50),
    nested_arr ARRAY<ARRAY<INT>> DEFAULT [[1, 2], [3, 4, 5]],
    map_with_struct MAP<INT, STRUCT<val INT, description VARCHAR(20)>> DEFAULT map{1: row(100, 'default')},
    complex_struct STRUCT<id INT, data MAP<VARCHAR(20), ARRAY<INT>>> DEFAULT row(999, map{'scores': [90, 95, 100]})
) PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

SET partial_update_mode = 'column';

INSERT INTO pk_nested_complex (id, name) VALUES (1, 'user1');
INSERT INTO pk_nested_complex (id, name) VALUES (2, 'user2');

SELECT * FROM pk_nested_complex ORDER BY id;

SET partial_update_mode = 'auto';

INSERT INTO pk_nested_complex (id, name) VALUES (3, 'user3');

SELECT * FROM pk_nested_complex ORDER BY id;

-- ====================================================================================
-- SECTION 12: PRIMARY KEY Table - Empty Collections
-- ====================================================================================

CREATE TABLE pk_empty_collections (
    id INT NOT NULL,
    status VARCHAR(20),
    empty_arr ARRAY<INT> DEFAULT [],
    empty_map MAP<INT, VARCHAR(20)> DEFAULT map{},
    struct_with_empty STRUCT<id INT, arr ARRAY<VARCHAR(20)>, mp MAP<VARCHAR(20), INT>> DEFAULT row(0, [], map{})
) PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

SET partial_update_mode = 'column';

INSERT INTO pk_empty_collections (id, status) VALUES (1, 'active');
INSERT INTO pk_empty_collections (id, status) VALUES (2, 'inactive');

SELECT * FROM pk_empty_collections ORDER BY id;

SET partial_update_mode = 'auto';

-- ====================================================================================
-- SECTION 13: PRIMARY KEY Table - Multiple Complex Types
-- ====================================================================================

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

SET partial_update_mode = 'column';

INSERT INTO pk_multi_complex (id, name) VALUES (1, 'test1');
INSERT INTO pk_multi_complex (id, name) VALUES (2, 'test2');

SELECT * FROM pk_multi_complex ORDER BY id;

INSERT INTO pk_multi_complex (id, name, arr_int) VALUES (3, 'test3', [100, 200]);

SELECT id, name, arr_int, arr_str FROM pk_multi_complex ORDER BY id;

SET partial_update_mode = 'auto';

-- ====================================================================================
-- SECTION 14: PRIMARY KEY Table - DECIMAL Types
-- ====================================================================================

CREATE TABLE pk_decimal_complex (
    id INT NOT NULL,
    name VARCHAR(50),
    prices ARRAY<DECIMAL(10, 2)> DEFAULT [99.99, 199.99, 299.99],
    price_map MAP<VARCHAR(20), DECIMAL(10, 2)> DEFAULT map{'min': 10.00, 'max': 1000.00},
    price_info STRUCT<base DECIMAL(10, 2), tax DECIMAL(5, 2)> DEFAULT row(100.00, 8.25)
) PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

SET partial_update_mode = 'column';

INSERT INTO pk_decimal_complex (id, name) VALUES (1, 'product1');
INSERT INTO pk_decimal_complex (id, name) VALUES (2, 'product2');

SELECT * FROM pk_decimal_complex ORDER BY id;

SET partial_update_mode = 'auto';

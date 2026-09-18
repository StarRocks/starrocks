CREATE TABLE {schema}.probe (id integer PRIMARY KEY, items text[], labels varchar(100)[]);
INSERT INTO {schema}.probe VALUES
 (1, ARRAY['abc','def'], ARRAY['abc','def']::varchar[]),
 (2, NULL, NULL),
 (3, ARRAY[]::text[], ARRAY[]::varchar[]),
 (4, ARRAY[NULL,'NULL',''], ARRAY[NULL,'NULL','']::varchar[]),
 (5, ARRAY['a,b','{x}','quote"',E'slash\\',E'line\nbreak',E'tab\there','行🙂','trailing '],
     ARRAY['a,b','{x}','quote"',E'slash\\',E'line\nbreak',E'tab\there','行🙂','trailing ']::varchar[]),
 (6, '[0:1]={abc,def}'::text[], '[0:1]={abc,def}'::varchar[]),
 (7, '[-2:-1]={abc,def}'::text[], '[-2:-1]={abc,def}'::varchar[]),
 (8, '[5:6]={abc,def}'::text[], '[5:6]={abc,def}'::varchar[]),
 (9, ARRAY[NULL]::text[], ARRAY[NULL]::varchar[]);
INSERT INTO {schema}.probe
 SELECT i, ARRAY['row-' || i, CASE WHEN i % 3 = 0 THEN NULL ELSE 'second' END],
           ARRAY['row-' || i, CASE WHEN i % 3 = 0 THEN NULL ELSE 'second' END]::varchar[]
 FROM generate_series(100,2249) i;
CREATE TABLE {schema}.multidimensional (id integer, items text[]);
INSERT INTO {schema}.multidimensional VALUES (1, ARRAY[['a','b'],['c',NULL]]), (2, ARRAY['valid']);
CREATE TABLE {schema}.unsupported (id integer, numbers integer[], padded char(3)[]);
INSERT INTO {schema}.unsupported VALUES (1, ARRAY[1,2], ARRAY['a','b']::char(3)[]);

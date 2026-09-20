-- Copyright 2021-present StarRocks, Inc. All rights reserved.
-- Licensed under the Apache License, Version 2.0.

CREATE TABLE {table} (
    id integer PRIMARY KEY,
    grp integer,
    amount integer,
    customer_id integer,
    name text COLLATE "C",
    created_at timestamp without time zone,
    created_date date,
    created_z timestamp with time zone,
    ratio double precision,
    price numeric(12, 2),
    big_key bigint,
    -- label takes the database's default collation, unlike name: the remote sort has to add
    -- COLLATE "C" for its order to match the byte order StarRocks applies locally.
    label text,
    active boolean,
    -- numeric without a precision: read strictly as DECIMAL(38,18), which keeps every
    -- pushdown off a scan that selects it, so its TopN stays local.
    unbounded_amount numeric,
    -- Arrays whose lower bound is 1, the only bound PostgreSQL ever builds for itself, plus a
    -- NULL and an empty value. A subscript over these answers the same thing wherever it is
    -- evaluated, so they pin the push-down without depending on the lower bound at all.
    tags text[],
    -- Lower bounds 1, 0 and 5 in one column, plus a NULL and an empty value. ARRAY[...] always
    -- yields a lower bound of 1, so the other two have to be written as explicitly bounded
    -- literals, whose braces are doubled because this file is str.format()ed for {{table}}.
    -- Reading the column drops the bound and rebases the value to 1, which is why a pushed-down
    -- subscript and a local one disagree here and nowhere else.
    odd text[],
    -- A two-dimensional value in a column the catalog maps as a one-dimensional array. Nothing
    -- can gate on it: PostgreSQL records no dimension on the column. Pushing a subscript down
    -- reads NULL; reading the column itself raises in the JDBC bridge.
    md text[]
);

-- Include ties, NULL group keys, an all-NULL group (grp = 2), and large
-- integers that would lose precision if the result comparison used float.
INSERT INTO {table}
SELECT n,
       CASE WHEN n % 11 = 0 THEN NULL ELSE n % 7 END,
       CASE WHEN n % 7 = 2 THEN NULL ELSE (n * 17 % 101) - 50 END,
       n % 9,
       'name_' || (n % 7),
       CASE WHEN n % 13 = 0 THEN NULL
            ELSE timestamp '2026-03-08 01:30:00'
                 + n * interval '3 minutes' + (n % 7) * interval '1 microsecond' END,
       CASE WHEN n % 13 = 0 THEN NULL ELSE date '2026-01-01' + n END,
       timestamptz '2026-01-01 00:00:00+00' + n * interval '1 minute',
       n::double precision / 10,
       n * 1.25,
       CASE WHEN n % 2 = 0 THEN 9223372036854775807::bigint - n
            ELSE (-9223372036854775807::bigint - 1) + n END,
       -- Mixed case and a leading digit: a linguistic collation interleaves these, C does not.
       CASE WHEN n % 17 = 0 THEN NULL
            ELSE (ARRAY['Alpha', 'alpha', 'BETA', 'beta', '_under', '9nine'])[1 + n % 6]
                 || '_' || (n % 5) END,
       CASE WHEN n % 19 = 0 THEN NULL ELSE n % 3 = 0 END,
       -- Values whose string order differs from their numeric order (-1, 1, 10, 2 vs -1, 1, 2, 10).
       CASE WHEN n % 23 = 0 THEN NULL ELSE (n % 13) - 1 + (n % 7) * 0.000000000000000001 END,
       -- Lower bound 1 throughout, with a NULL at 29 and 58 and an empty array at 31 and 62.
       CASE WHEN n % 29 = 0 THEN NULL
            WHEN n % 31 = 0 THEN ARRAY[]::text[]
            ELSE ARRAY['t' || (n % 5), 't' || (n % 3), 'tail'] END,
       -- Lower bound 5, 1, 0, then NULL or empty, cycling every eight rows.
       CASE WHEN n % 4 = 0 THEN '[0:2]={{zero,one,two}}'::text[]
            WHEN n % 4 = 1 THEN '[5:7]={{a,b,c}}'::text[]
            WHEN n % 4 = 2 THEN ARRAY['p1', 'p2', 'p3']
            WHEN n % 8 = 3 THEN NULL
            ELSE ARRAY[]::text[] END,
       ARRAY[ARRAY['a' || (n % 3), 'b'], ARRAY['c', 'd']]
FROM generate_series(1, 80) AS n;

-- Valid proleptic Gregorian and AD boundaries, including the old JDBC
-- GregorianCalendar cutover and a daylight-saving gap in America/New_York.
INSERT INTO {table} VALUES
    (81, 0, 1, 1, 'name_1', '0001-01-01 00:00:00.000001', '0001-01-01',
     '2026-01-01 00:00:00+00', 8.1, 101.25, -9223372036854775808,
     'Zulu', true, 99999999999999999999.999999999999999999,
     ARRAY['alpha', 'beta', 'tail'], '[0:2]={{zero,one,two}}'::text[], ARRAY[ARRAY['a0', 'b'], ARRAY['c', 'd']]),
    (82, 0, 2, 2, 'name_2', '1582-10-10 12:34:56.123456', '1582-10-10',
     '2026-01-01 00:00:00+00', 8.2, 102.50, 9223372036854775807,
     'aardvark', false, -99999999999999999999.999999999999999999,
     NULL, '[5:7]={{a,b,c}}'::text[], ARRAY[ARRAY['a0', 'b'], ARRAY['c', 'd']]),
    (83, 1, 3, 3, 'name_3', '9999-12-31 23:59:59.999999', '9999-12-31',
     '2026-01-01 00:00:00+00', 8.3, 103.75, 0, '', NULL, 0,
     ARRAY[]::text[], ARRAY['p1', 'p2', 'p3'], ARRAY[ARRAY['a0', 'b'], ARRAY['c', 'd']]),
    (84, 1, 4, 4, 'name_4', '2026-03-08 02:30:00.123456', '2026-03-08',
     '2026-01-01 00:00:00+00', 8.4, 105.00, 1, NULL, true, NULL,
     ARRAY['omega', 'psi', 'tail'], NULL, ARRAY[ARRAY['a0', 'b'], ARRAY['c', 'd']]);

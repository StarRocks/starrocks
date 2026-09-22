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
    -- real, not double precision. A FLOAT key is bound as java.sql.Types.REAL, and n/10 has no
    -- exact binary form, so it only matches if the text carries the shortest decimal that reads
    -- back as that exact single-precision value -- the property std::to_string does not have.
    -- Rows 81..84 add the single-precision extremes, a NaN and a negative zero.
    ratio_f real,
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
    md text[],
    -- One column per non-text element type the reader maps. Each is its own path: the bridge
    -- hands the backend the element class that element's logical type expects, with no VARCHAR
    -- staging step in between, so no two of them share a conversion.
    nums integer[],
    bigs bigint[],
    shorts smallint[],
    flags boolean[],
    dbls double precision[],
    f4s real[],
    -- char(n)[] arrives as String[] like text[] does, and was only ever excluded by name. Its
    -- elements keep PostgreSQL's blank padding.
    bps char(4)[],
    days date[],
    stamps timestamp without time zone[],
    -- Deliberately left unmapped, and the reason a case selects it and expects an error: the
    -- driver returns java.sql.Timestamp[] for this exactly as it does for timestamp[], so the
    -- type name is the only thing separating them.
    zoned timestamp with time zone[],
    -- Join key for the runtime filter cases. C collation so the byte order StarRocks uses
    -- and the order PostgreSQL uses agree, and values a naive string-splicing filter would
    -- corrupt: a backslash, both quote characters, the LIKE metacharacters, and non-ASCII.
    -- The runtime filter writes its values into the statement as literals, so these are what
    -- prove the escaping: the single quote has to come back doubled, the rest untouched. The
    -- backslash is the one that cannot be escaped safely and refuses the whole filter instead,
    -- which is what rf_backslash_value_veto asserts.
    join_key text COLLATE "C",
    -- PostgreSQL stores a uuid as 16 bytes and renders it as the 36-character canonical text,
    -- which is also what the JDBC bridge hands StarRocks -- UDFHelper writes UUID.toString(). The
    -- two orders coincide: the text is fixed width with the hyphens in fixed positions, and its
    -- lowercase hex digits sort in ASCII the way the bytes they spell sort, so comparing the text
    -- byte by byte is comparing the values. A pushed comparison therefore needs no collation, and
    -- must not be given one: PostgreSQL answers "collations are not supported by type uuid".
    uid uuid
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
       n::real / 10,
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
       ARRAY[ARRAY['a' || (n % 3), 'b'], ARRAY['c', 'd']],
       -- NULL arrays at 29 and 58, empty arrays at 31 and 62, and a NULL element throughout, so
       -- every element type meets the three shapes that move the parent offsets.
       CASE WHEN n % 29 = 0 THEN NULL WHEN n % 31 = 0 THEN ARRAY[]::integer[]
            ELSE ARRAY[n, NULL, -n] END,
       ARRAY[9223372036854775807 - n, (-9223372036854775807 - 1) + n, NULL]::bigint[],
       ARRAY[(n % 100)::smallint, NULL, (-(n % 100))::smallint],
       ARRAY[n % 2 = 0, NULL, n % 3 = 0],
       -- Halves and quarters only: exactly representable, so the text of the value does not
       -- depend on either engine's shortest-round-trip formatting.
       ARRAY[n / 4.0, NULL, -n / 2.0]::double precision[],
       ARRAY[(n / 4.0)::real, NULL, (-n / 2.0)::real],
       ARRAY['c' || (n % 5), NULL]::char(4)[],
       CASE WHEN n % 29 = 0 THEN NULL WHEN n % 31 = 0 THEN ARRAY[]::date[]
            ELSE ARRAY[date '2026-01-01' + n, NULL] END,
       CASE WHEN n % 29 = 0 THEN NULL WHEN n % 31 = 0 THEN ARRAY[]::timestamp[]
            ELSE ARRAY[timestamp '2026-03-08 01:30:00' + n * interval '3 minutes'
                       + (n % 7) * interval '1 microsecond', NULL] END,
       ARRAY[timestamptz '2026-01-01 00:00:00+00' + n * interval '1 minute'],
       -- Every sixth row is NULL so a null-safe string join has probe-side NULLs too.
       CASE WHEN n % 6 = 0 THEN NULL
            ELSE (ARRAY[$$back\slash$$, $$quote'single$$, $$double"quote$$,
                        $$percent%underscore_$$, $$中文字符串$$])[1 + n % 5] END,
       -- md5 spreads the values over the whole range and keeps them deterministic, which the
       -- ordered row diff needs; 32 hex digits cast to uuid render canonically hyphenated.
       -- NULL at 37 and 74.
       CASE WHEN n % 37 = 0 THEN NULL ELSE md5(n::text)::uuid END
FROM generate_series(1, 80) AS n;

-- Valid proleptic Gregorian and AD boundaries, including the old JDBC
-- GregorianCalendar cutover and a daylight-saving gap in America/New_York.
INSERT INTO {table} VALUES
    (81, 0, 1, 1, 'name_1', '0001-01-01 00:00:00.000001', '0001-01-01',
     '2026-01-01 00:00:00+00', 8.1, '3.4028235e38'::real, 101.25, -9223372036854775808,
     'Zulu', true, 99999999999999999999.999999999999999999,
     ARRAY['alpha', 'beta', 'tail'], '[0:2]={{zero,one,two}}'::text[], ARRAY[ARRAY['a0', 'b'], ARRAY['c', 'd']],
     ARRAY[2147483647, NULL, -2147483648], ARRAY[9223372036854775807]::bigint[],
     ARRAY[32767::smallint, (-32768)::smallint], ARRAY[true, NULL, false],
     ARRAY[0.5, -0.25]::double precision[], ARRAY[0.5::real, 0.1::real], ARRAY['ab', NULL]::char(4)[],
     -- The AD boundaries and the Gregorian cutover, which java.sql.Date moves by ten days.
     ARRAY['0001-01-01', '1582-10-10', '9999-12-31']::date[],
     ARRAY['0001-01-01 00:00:00.000001', '1582-10-10 12:34:56.123456',
           '9999-12-31 23:59:59.999999']::timestamp[],
     ARRAY[timestamptz '2026-01-01 00:00:00+00'], $$back\slash$$,
     -- The smallest uuid there is, so a range comparison has something below every generated one.
     '00000000-0000-0000-0000-000000000000'),
    (82, 0, 2, 2, 'name_2', '1582-10-10 12:34:56.123456', '1582-10-10',
     '2026-01-01 00:00:00+00', 8.2, '1.1754944e-38'::real, 102.50, 9223372036854775807,
     'aardvark', false, -99999999999999999999.999999999999999999,
     NULL, '[5:7]={{a,b,c}}'::text[], ARRAY[ARRAY['a0', 'b'], ARRAY['c', 'd']],
     ARRAY[0], ARRAY[0]::bigint[], ARRAY[0::smallint], ARRAY[NULL::boolean],
     ARRAY[0]::double precision[], ARRAY[0]::real[], ARRAY['']::char(4)[],
     -- A daylight-saving gap in America/New_York: 02:30 does not exist there that day.
     ARRAY['2026-03-08']::date[], ARRAY['2026-03-08 02:30:00.123456']::timestamp[],
     ARRAY[timestamptz '2026-01-01 00:00:00+00'], $$plain$$,
     -- The largest, and all-letters: it sorts above every digit-leading value in both orders only
     -- because ASCII puts 'a'-'f' after '0'-'9', which is the whole reason the two agree.
     'ffffffff-ffff-ffff-ffff-ffffffffffff'),
    (83, 1, 3, 3, 'name_3', '9999-12-31 23:59:59.999999', '9999-12-31',
     '2026-01-01 00:00:00+00', 8.3, 'NaN'::real, 103.75, 0, '', NULL, 0,
     ARRAY[]::text[], ARRAY['p1', 'p2', 'p3'], ARRAY[ARRAY['a0', 'b'], ARRAY['c', 'd']],
     ARRAY[]::integer[], ARRAY[]::bigint[], ARRAY[]::smallint[], ARRAY[]::boolean[],
     ARRAY[]::double precision[], ARRAY[]::real[], ARRAY[]::char(4)[], ARRAY[]::date[],
     ARRAY[]::timestamp[], ARRAY[]::timestamptz[], NULL,
     -- Digits and letters mixed in every group, below the median of the generated values.
     '0f9b7c21-4d3e-4a5b-8c6d-7e8f90a1b2c3'),
    (84, 1, 4, 4, 'name_4', '2026-03-08 02:30:00.123456', '2026-03-08',
     '2026-01-01 00:00:00+00', 8.4, '-0.0'::real, 105.00, 1, NULL, true, NULL,
     ARRAY['omega', 'psi', 'tail'], NULL, ARRAY[ARRAY['a0', 'b'], ARRAY['c', 'd']],
     NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, $$plain$$, NULL),
    -- The two instants that fall into the same wall clock when the session time zone observes the
    -- end of daylight saving: 2026-11-01 05:30+00 and 06:30+00 are both 01:30 in America/New_York.
    -- StarRocks reads a timestamptz as that wall clock, so binding it back could only match one of
    -- them -- which is why a timestamptz column is never offered for a runtime filter.
    (85, 0, 5, NULL, 'name_5', '2026-03-08 03:00:00', '2026-04-01',
     '2026-11-01 05:30:00+00', 0.85, 0.85, 106.25, 5, NULL, true, 3,
     NULL, NULL, ARRAY[ARRAY['a0', 'b'], ARRAY['c', 'd']],
     NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    (86, 1, 6, NULL, 'name_6', '2026-03-08 03:01:00', '2026-04-02',
     '2026-11-01 06:30:00+00', 0.86, 0.86, 107.50, 6, NULL, false, 4,
     NULL, NULL, ARRAY[ARRAY['a0', 'b'], ARRAY['c', 'd']],
     NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    -- Two join keys outside the Basic Multilingual Plane, which UTF-8 encodes in four bytes and
    -- modified UTF-8 -- what the bridge's NewStringUTF handoff decodes -- encodes as a six-byte
    -- surrogate pair. A filter carrying one of these has to be dropped rather than rendered,
    -- because the handoff would truncate the statement and lose the matching rows.
    -- U+20001 is a CJK Extension B character, the real-world case: those appear in personal names.
    (87, 0, 7, NULL, 'name_7', '2026-03-09 03:00:00', '2026-04-03',
     '2026-01-01 00:00:00+00', 0.87, 0.87, 108.75, 7, NULL, true, 5,
     NULL, NULL, ARRAY[ARRAY['a0', 'b'], ARRAY['c', 'd']],
     NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, U&'\+01F600', NULL),
    (88, 1, 8, NULL, 'name_8', '2026-03-09 03:01:00', '2026-04-04',
     '2026-01-01 00:00:00+00', 0.88, 0.88, 110.00, 8, NULL, false, 6,
     NULL, NULL, ARRAY[ARRAY['a0', 'b'], ARRAY['c', 'd']],
     NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, U&'\+020001', NULL);

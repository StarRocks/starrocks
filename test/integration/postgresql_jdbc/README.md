# PostgreSQL JDBC pushdown integration test

This opt-in test executes 66 queries against PostgreSQL and an existing
StarRocks PostgreSQL JDBC catalog. The 49 pushdown queries run with all four
combinations of `enable_jdbc_agg_push_down` and `enable_jdbc_topn_push_down`,
plus whatever session variables the case pins for itself; the 17 runtime filter
queries run with `enable_jdbc_runtime_filter_push_down` off and on instead.
Every execution must return the same ordered rows as PostgreSQL and satisfy the
pushdown or fallback assertions on `EXPLAIN VERBOSE`'s JDBC `QUERY`, and a
runtime filter query additionally has to match what its scan's profile says it
actually sent to PostgreSQL.

The existing `test/sql/test_jdbc_catalog` T/R suite requires MySQL as its JDBC
source. This separate entry point adds PostgreSQL coverage without adding a
mandatory service to that suite or changing its configuration.

## Requirements

- Python 3.8 or later, with no additional Python packages.
- `mysql` and `psql` command-line clients. `--mysql` and `--psql` accept an
  executable path, including a wrapper that forwards all arguments and stdin.
- A running StarRocks cluster with this TopN rule and updated JDBC bridge on
  every BE/CN. The bridge update is required for the plain DATE/TIMESTAMP cases.
- An existing PostgreSQL JDBC catalog using `org.postgresql.Driver`, connected
  to the same database that `psql` uses. The runner does not create a catalog.
- The PostgreSQL fixture account needs permission to create a schema and table
  in that database. The catalog account must be able to read those objects.
  If the accounts differ, pass `--pg-reader-role` to grant that existing role
  `USAGE` on the new schema and `SELECT` on its tables. The runner changes no
  global PostgreSQL settings or role definitions.
- The StarRocks account needs permission to query the catalog and set session
  variables. Project pushdown is enabled in all four modes. The runtime filter
  cases additionally enable profiling for their own statements and read the
  result back with `get_query_profile`, so the account has to be allowed to do
  both; nothing else in the run turns profiling on.

## Run

Use normal libpq environment variables and authentication for PostgreSQL.
For example, configure `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER` and
`PGPASSFILE` (or an existing `.pgpass`). Do not put passwords in command lines.
For StarRocks, use the client's normal authentication configuration, or pass
`--mysql-defaults-file /path/to/client.cnf`. Host, port, and user come from
`--sr-host`, `--sr-port`, and `--sr-user`, or `SR_HOST`, `SR_PORT`, and `SR_USER`.

```bash
python3 test/integration/postgresql_jdbc/run.py --list

python3 test/integration/postgresql_jdbc/run.py \
  --catalog postgres_jdbc \
  --sr-host 127.0.0.1 --sr-port 9030 --sr-user root \
  --pg-reader-role catalog_reader \
  --output output/postgresql-jdbc-results.json
```

Omit `--pg-reader-role` when the catalog uses the fixture account or already
has access. The default per-client timeout is 60 seconds; `--timeout` changes
both the client timeout and the server query timeout.

Each invocation creates a unique `sr_pg_pushdown_<uuid>` schema and removes
only that schema on completion, including after a query failure. A cleanup
failure makes the run fail and is recorded separately from the original
failure. The JSON includes the schema name for manual cleanup if needed.
No changes are made to StarRocks catalog definitions or global variables.

## Case fields

A case is an object in `cases.json`. `name` and `sql` are required; `{table}` in
`sql` is replaced with the catalog-qualified fixture table. The rest are optional:

| field | meaning |
|---|---|
| `pg_sql` | reference query run against PostgreSQL instead of `sql`. Use it where the two engines are expected to disagree before any pushdown: a string sort key needs `COLLATE "C"`, and an array subscript over a lower bound other than 1 needs `a[array_lower(a, 1) + k - 1]` to say what StarRocks means by `a[k]`. |
| `session` | session variables set for this case, in every mode, after the five the runner always sets. `{"enable_jdbc_array_lower_bound_correction": true}` renders as `SET enable_jdbc_array_lower_bound_correction = true;`. Booleans and numbers go out bare, strings quoted. A case without the field sends exactly what it sent before the field existed. |
| `aggregate` | `"group"` or `"scalar"`: remote aggregation is required in the two modes that enable it, and forbidden in the other two. |
| `remote_having`, `topn_limit`, `topn_requires_aggregate`, `remote_collate` | the TopN and HAVING boundaries, described under Coverage below. |
| `remote_sql_contains`, `remote_sql_excludes` | case-sensitive substrings required, or required absent, in the `QUERY:` line. Quoting is part of the substring, so `"name" = 'name_1'` does not pass on a differently quoted name. This is the only way to state what the remote statement asked for: rows cannot tell a subscript PostgreSQL evaluated from one StarRocks evaluated locally, nor show which columns were fetched. Applies to every dialect, not only PostgreSQL. |
| `expect_error` | the case has no reference rows; instead StarRocks must fail with a message containing this substring. Its plan is still asserted. Reading a multidimensional PostgreSQL array raises in the JDBC bridge, and an error is not a row. |
| `comment` | free text for a case whose shape only makes sense with its reason attached, kept next to the case rather than in this file. The runner ignores it. |

## Coverage and evidence

`cases.json` contains the SQL and expected pushdown boundaries. `fixtures.sql`
contains deterministic data with ties, NULL keys, an all-NULL aggregate
group, BIGINT extrema, and valid AD temporal values including the Gregorian
cutover and a daylight-saving gap. Every limited result has a deterministic
tie breaker. Empty-input scalar and grouped aggregates are covered separately.

Three array columns carry the PostgreSQL array cases. `tags` holds lower-bound-1
values, plus a NULL and an empty array. `odd` holds lower bounds 1, 0 and 5 in
one column: `ARRAY[...]` always yields a lower bound of 1, so the other two are
written as explicitly bounded literals such as `'[0:2]={zero,one,two}'::text[]`.
`md` holds two-dimensional values in a column the catalog maps as a
one-dimensional array; PostgreSQL records no dimension on a column, so nothing
can gate on it.

The array cases pin the three states of a constant subscript separately, since
only a real PostgreSQL can say what it answers for each. With the push-down on
and the correction off -- the shipped default -- the pushed `a[k]` is compared
against PostgreSQL's own `a[k]`, which is the accepted divergence from local
evaluation. With `enable_jdbc_array_lower_bound_correction` on, and again with
`enable_jdbc_array_subscript_push_down` off, it is compared against
`a[array_lower(a, 1) + k - 1]`, which is what StarRocks means by `a[k]` once the
driver has dropped the bound. A fourth case sets both, which must answer exactly
what the push-down being off answers on its own, since with nothing pushed there
is nothing left for the correction to spell. A variable subscript stays local,
`element_at(a, k)` must answer as `a[k]`, and a subscript under GROUP BY must
survive being folded into a pushed-down aggregate. The multidimensional value is
the one documented behaviour change that is not a row difference: pushed down it
reads NULL, and with the push-down off reading the column raises
`only one-dimensional`, which `expect_error` states.

Two of the array cases exist for a boundary that no projection-shaped case
reaches. `enable_jdbc_array_subscript_push_down` is read in the gate both
push-down paths share rather than in either rule, so the case that holds it there
is a *predicate* -- `WHERE odd[1] = 'zero'` with the push-down off -- which must
send no subscript and fetch the array column instead: only a switch the filter
path also consults can do that, and a projection case stays green without it.
Conversely a subscript selected *beside the bare array column* -- `SELECT id,
tags[1], tags` -- must still push the subscript, which holds only while an
identity passthrough is exempt from the dialect gate; gating it makes the whole
projection fall back and silently takes the subscript push-down with it, without
changing a single row.

One case belongs to no dialect in particular: a query selecting `id` while
filtering on `name` must send a remote SELECT list of `"id"` alone, with `name`
only in the WHERE. A scalar column says it better than an array does, because
nothing but the pruning could remove it.

The cases include basic and multi-column TopN, hidden sorting columns,
GROUP BY with SUM/COUNT/MIN/MAX, HAVING, DISTINCT, multiple COUNT DISTINCT,
and projection expressions. An OFFSET is pushed with its LIMIT rather than
kept local, so the two OFFSET cases assert the remote OFFSET and compare the
rows it skipped. Required fallback cases include local filters and HAVING
expressions, string/float/decimal/timestamptz order keys, and derived
timestamps. An aggregate above an already limited subquery checks that the
inner TopN is pushed, that the aggregate follows it only once the scan carries
that LIMIT (`aggregate_requires_topn`), and that the outer limit -- ordered by
an expression the rule cannot take, so it stays local in every mode
(`local_topn_retained`) -- never reaches the remote SQL, where it would
truncate the rows before the GROUP BY.

An order key can be incomparable to a naive reference query and then carries
its own `pg_sql`. `unbounded_amount`, an unconstrained `numeric`, is no longer
one of them: it maps to DECIMAL(38,18) and both sides order it numerically, so
the case only has to stay off the pushdown path, which the strict read already
guarantees. A pushed MIN/MAX over a string names `COLLATE "C"` for the same
reason a pushed ORDER BY does, counted per case as `aggregate_collate`; the
`label` fixture is chosen so the C answer (`beta_*`) and the database
collation's answer (`_under_*`) actually differ.

The runner compares exact numeric values without converting BIGINT to float,
normalizes insignificant timestamp fractional zeros, and otherwise compares
rows and columns in order. Fixtures avoid tabs, newlines, and the literal
string `NULL`, which would be ambiguous in the clients' TSV output. Floating
point fallback cases project only integer IDs; no approximate tolerance is
used. The scalar AVG fixture has an exactly representable expected value.

JSON evidence contains the PostgreSQL reference SQL and rows, each StarRocks
query and result, full plans, extracted JDBC SQL, assertion failures, and a
summary. Plans must contain exactly one JDBC scan query. Positive cases require
remote ORDER BY, the expected LIMIT and OFFSET, explicit NULL ordering, and no
StarRocks TopN left behind except the one a case declares unpushable. Negative
cases reject remote ORDER BY, LIMIT and OFFSET. Aggregate and HAVING assertions
are checked independently of TopN, and each expected `COLLATE "C"` is counted
against the switch that put it there. A failed client command, timeout, result
mismatch, or plan mismatch returns a nonzero exit status; execution errors are
never treated as an expected result.

The output path must not already exist, so reruns retain prior evidence.

Inspect `passed` and `summary` in the JSON. A successful run has 230 completed
and passed executions, no setup/reference errors, and no cleanup error.

## Runtime filter coverage

A join runtime filter is a pure optimization, so a runtime filter case is run
with the switch off and on and both executions are compared against the same
PostgreSQL reference: the two have to agree with each other and with the
remote database. That is only possible because the build side is a `VALUES`
list rather than a StarRocks table, so PostgreSQL can execute the same join.
`{bc}` expands to StarRocks' `[broadcast]` join hint and to nothing for
PostgreSQL; only a broadcast hash join builds the in-filter these cases are
about. Each case also sets `runtime_filter_scan_wait_time` to 3000ms, because
at the 20ms default a JDBC scan frequently starts reading before the filter
arrives, which would make the ON executions pass or fail at random.

Whether a filter was really rendered into the remote SQL is read from the
scan's own profile (`get_query_profile`), which reports the finished remote
statement plus `PushdownRuntimeFilters`, `PushdownRuntimeFilterValues`,
`PushdownRuntimeFilterColumns` and `PushdownRuntimeFilterSkipped`. A plan is
printed before any filter exists, so `EXPLAIN VERBOSE` can only show whether
the FE authorized the pushdown; both halves are asserted. Reading the profile
rather than PostgreSQL's statement log keeps the assertion independent of the
remote server's logging configuration.

The values are rendered into the statement rather than bound to it, so
`remote_predicate` carries the literals themselves and states the quoting each
type gets: an integer bare, everything else -- floating point included --
quoted. What it cannot state is their order, which is the iteration order of
the BE's hash set and is not defined, so the runner sorts the values inside
every `IN (...)` on both sides before matching. Nothing is bound any more, so
every remote statement is also required to be free of `?` whether or not a
filter reached it: a placeholder that reaches the driver is a parameter the
bridge has no value for.

The cases cover a plain integer key; the three correctness rules -- a scan
carrying a row limit must never take a filter, a null-safe join must render
`IN (...) OR IS NULL` rather than a NULL inside the list, and a string key
whose values contain both quotes, `%`, `_` and non-ASCII must come back with
exactly the rows PostgreSQL returns, with its own single quote doubled inside
the literal the statement carries; a string key holding a backslash, which is
refused whole because doubling the quote is only correct while the remote
reads a backslash as an ordinary character, and `standard_conforming_strings`
and `NO_BACKSLASH_ESCAPES` are session settings the BE cannot see; a string
key holding a character outside the Basic Multilingual Plane, refused for a
different reason, because the handoff that carries the finished statement
decodes modified UTF-8 and would cut a four-byte sequence short -- now
mid-literal, leaving a quote unclosed; a filter landing outside an already
pushed aggregate and an already pushed join; a build side above
`max_pushdown_conditions_per_column`, where no filter is built at all; and a
join on the `unbounded_amount` column, a PostgreSQL `numeric` with no
precision that maps to DECIMAL(38,18) and is read strictly. The last one is
the FE veto seen from the other end: DECIMAL is outside the type whitelist, so
the filter reaches the scan and is reported as skipped rather than rendered,
which is also what the strict read needs -- a remote filter over such a column
could drop the very value that does not fit the mapping before the reader ever
sees it.

The type whitelist has a case per newly supported key type. A `real` key joins
on values -- 0.1, 0.2 and the single-precision maximum -- that only match if
the rendered text is the shortest decimal that reads back as the same 32-bit
value, and only while it is quoted: bare, PostgreSQL reads `0.1` as `numeric`
and widens the column to meet it. A `double precision` key does the same at
double width. A `date` key joins on 0001-01-01, the Gregorian cutover date
1582-10-10 and 9999-12-31, which is the range the bridge's reader accepts and
the range `java.sql.Date` would have shifted -- quoted text PostgreSQL parses
itself never reaches that calendar. A `timestamp without time zone` key joins
on values carrying microseconds and on the wall clock 2026-03-08 02:30, which
does not exist in America/New_York and which `setTimestamp` would have moved
by resolving it in the JVM's default zone.

Two cases are the veto seen from both ends of the same StarRocks type. A
`timestamp with time zone` key is never offered: rows 85 and 86 hold the two
instants that become the same wall clock when daylight saving ends, the case
runs with `time_zone` set to America/New_York on both sides, and its PostgreSQL
reference converts explicitly with `AT TIME ZONE` because that -- not
`timestamptz = timestamp` -- is what StarRocks computes locally. Remove the FE
gate and the remote `IN` answers with one row where the local filter keeps two.
And on the derived table a pushed GROUP BY builds, where the remote source type
names are gone, a `date` key still pushes down while a `timestamp` key stops:
the first cannot be anything but a PostgreSQL `date`, the second could be either
of the two timestamp types.

# PostgreSQL JDBC pushdown integration test

This opt-in test executes 49 queries against PostgreSQL and an existing
StarRocks PostgreSQL JDBC catalog. Each query runs with all four combinations
of `enable_jdbc_agg_push_down` and `enable_jdbc_topn_push_down`, plus whatever
session variables the case pins for itself. Every execution must return the same
ordered rows as PostgreSQL and satisfy the pushdown or fallback assertions on
`EXPLAIN VERBOSE`'s JDBC `QUERY`.

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
  variables. Project pushdown is enabled in all four modes.

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
and projection expressions. Required fallback cases include local filters
and HAVING expressions, OFFSET, string/float/decimal/timestamptz order keys,
and derived timestamps. An aggregate above an already limited subquery checks
that only the inner TopN can be pushed; the outer limit must not truncate the
rows before the local aggregate.

The runner compares exact numeric values without converting BIGINT to float,
normalizes insignificant timestamp fractional zeros, and otherwise compares
rows and columns in order. Fixtures avoid tabs, newlines, and the literal
string `NULL`, which would be ambiguous in the clients' TSV output. Floating
point fallback cases project only integer IDs; no approximate tolerance is
used. The scalar AVG fixture has an exactly representable expected value.

JSON evidence contains the PostgreSQL reference SQL and rows, each StarRocks
query and result, full plans, extracted JDBC SQL, assertion failures, and a
summary. Plans must contain exactly one JDBC scan query. Positive cases require
remote ORDER BY, the expected LIMIT, explicit NULL ordering, and the retained
StarRocks TopN. Negative cases reject remote ORDER BY/LIMIT. Aggregate and
HAVING assertions are checked independently of TopN. A failed client command,
timeout, result mismatch, or plan mismatch returns a nonzero exit status;
execution errors are never treated as an expected result.

The output path must not already exist, so reruns retain prior evidence.

Inspect `passed` and `summary` in the JSON. A successful run has 196 completed
and passed executions, no setup/reference errors, and no cleanup error.

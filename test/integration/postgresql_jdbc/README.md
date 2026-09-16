# PostgreSQL JDBC pushdown integration test

This opt-in test executes 36 queries against PostgreSQL and an existing
StarRocks PostgreSQL JDBC catalog. Each query runs with all four combinations
of `enable_jdbc_agg_push_down` and `enable_jdbc_topn_push_down`. Every execution
must return the same ordered rows as PostgreSQL and satisfy the pushdown or
fallback assertions on `EXPLAIN VERBOSE`'s JDBC `QUERY`.

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

## Coverage and evidence

`cases.json` contains the SQL and expected pushdown boundaries. `fixtures.sql`
contains deterministic data with ties, NULL keys, an all-NULL aggregate
group, BIGINT extrema, and valid AD temporal values including the Gregorian
cutover and a daylight-saving gap. Every limited result has a deterministic
tie breaker. Empty-input scalar and grouped aggregates are covered separately.

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

Inspect `passed` and `summary` in the JSON. A successful run has 108 completed
and passed executions, no setup/reference errors, and no cleanup error.

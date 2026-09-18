# PostgreSQL JDBC string-array regression

Opt-in end-to-end coverage for PostgreSQL `text[]` and `varchar[]` mapped to `ARRAY<VARCHAR>`. Requires Python 3.8+, `mysql`, `psql`, an existing PostgreSQL JDBC catalog, and the updated FE, BE/CN, JDBC bridge and packaged `type_checker_config.xml`. No TopN patch or variable is required.

```bash
python3 test/integration/postgresql_jdbc_arrays/run.py --list
PGHOST=127.0.0.1 PGPORT=5432 PGDATABASE=jdbc_test PGUSER=postgres \
  python3 test/integration/postgresql_jdbc_arrays/run.py \
  --catalog pg_jdbc --sr-host 127.0.0.1 --pg-reader-role jdbc_reader \
  --output /tmp/pg-array-results.json
```

Use `.pgpass`/`PGPASSFILE` for PostgreSQL authentication and `--mysql-defaults-file` for StarRocks authentication. `--psql` and `--mysql` can select forwarding wrappers. Passwords are not accepted as command-line arguments. The PG admin needs permission to create/drop a temporary schema and grant the catalog reader access. All fixture tables use a unique schema, which is dropped in `finally`; existing tables and catalog definitions are preserved. Existing output files are never overwritten.

The suite compares 24 queries in all four combinations of aggregate and join pushdown (96 executions), and checks `EXPLAIN VERBOSE` to ensure array operations stay local. It includes 2,159 rows with `SET chunk_size=1024` to cross scanner chunks, both string element types, NULL/empty arrays, NULL elements, escaping, UTF-8, out-of-range indexes, filters, group/distinct aggregation, and joins. A multidimensional value must fail clearly, and a subsequent valid query must succeed.

PostgreSQL arrays can have lower bounds other than 1. The fixture includes bounds 0, -2, and 5. StarRocks discards these bounds and starts positions at 1. The PostgreSQL oracle reconstructs values with `unnest(...) WITH ORDINALITY` in ordinal order before comparing results. It deliberately does not use the original PostgreSQL subscripts or array equality/grouping, which include the source bounds.

Evidence includes returned values, the normalized PG reference, plans and generated remote queries, the multidimensional error, and cleanup/recovery status. The runner returns nonzero on any mismatch, unexpected pushdown, or setup/cleanup failure. It is not connected to default CI because it requires a real PostgreSQL service and configured JDBC catalog.

For full-array result comparisons, each string element is encoded as uppercase UTF-8 HEX on both systems before formatting the array. This verifies exact bytes and preserves NULL arrays, empty arrays, NULL elements and empty strings without depending on escaping of tabs/newlines in StarRocks array display text. Subscript, predicate and grouping operations still run on the original array values.

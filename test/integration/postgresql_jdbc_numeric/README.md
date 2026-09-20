# PostgreSQL unconstrained numeric regression

This opt-in suite requires PostgreSQL, a StarRocks JDBC catalog pointing at that database, `psql`, and Python with `pymysql`. It creates an isolated schema and removes it in `finally`. It does not create or reconfigure the catalog.

```bash
# Configure psql with PGHOST/PGPORT/PGDATABASE/PGUSER and a private PGPASSFILE.
python3 run.py --catalog pg_jdbc --sr-host 127.0.0.1 --pg-reader-role reader --output results.json
```

Use `SR_PASSWORD` if the StarRocks account requires a password. `PG_COMMAND` can optionally contain a JSON argument array for a psql wrapper; it is executed without a shell. Do not put credentials in the argument list.

The suite covers exact Decimal128(38,18) boundaries, numeric sorting/filtering/grouping, projection and join, view/native-query metadata, multiple chunks, NULL, trailing zeros, rejected overflow/rounding/NaN/infinities, query recovery, and explicit numeric / aggregate regressions. It compares all eight aggregate/project/join switch combinations. This runner is not part of default integration CI.

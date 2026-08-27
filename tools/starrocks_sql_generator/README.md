# StarRocks SQL Generator

Generate schema-aware StarRocks SQL for grammar coverage testing, based on
[`StarRocks.g4`](../../fe/fe-grammar/src/main/antlr/com/starrocks/grammar/StarRocks.g4).

## Features

- Enumerates major query/DML grammar paths (SELECT/JOIN/GROUP BY/CTE/compound/EXPLAIN, INSERT/UPDATE/DELETE/MERGE).
- Uses table/column metadata to emit queryable SQL (table names, column refs, type-aware literals).
- Supports recursion depth limits for expression/subquery/join/CTE expansion via `--max-depth`.
- Emits optional manifest JSON mapping each SQL to its grammar path tag.

## Quick Start

```bash
python3 -m tools.starrocks_sql_generator \
  --schema tools/starrocks_sql_generator/sample_schema.json \
  --mode paths \
  --max-depth 3 \
  --max-outputs 200 \
  --with-semicolon \
  --output /tmp/starrocks_generated.sql \
  --manifest /tmp/starrocks_generated.manifest.json
```

## Schema Input

### JSON schema

```json
{
  "database": "demo",
  "tables": [
    {
      "name": "t1",
      "columns": [
        {"name": "id", "type": "BIGINT", "nullable": false},
        {"name": "name", "type": "VARCHAR(64)"}
      ]
    }
  ]
}
```

### SQL schema

You can also pass a SQL DDL file:

```bash
python3 -m tools.starrocks_sql_generator --schema-sql my_tables.sql ...
```

## CLI Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--schema` | sample schema | JSON schema path |
| `--schema-sql` | - | SQL DDL schema path |
| `--mode` | `paths` | `paths`, `random`, or `all` |
| `--categories` | `query,dml` | `query`, `dml`, or `all` |
| `--max-depth` | `3` | Max recursion for expression/subquery/CTE |
| `--max-list-items` | `2` | Max list size in SELECT/GROUP BY/etc. |
| `--max-outputs` | `500` | Output statement cap |
| `--seed` | `0` | Random seed |
| `--with-semicolon` | off | Append `;` |
| `--output` | stdout | Output SQL file |
| `--manifest` | - | Output path metadata JSON |

## Covered Grammar Paths (Query)

- `queryStatement`, `withClause`, `queryNoWith`, `queryPrimary`, `querySpecification`
- `setQuantifier`, `selectItem` (`*`, `EXCEPT`, alias)
- `fromClause` (`DUAL`, table, subquery, `VALUES`)
- `joinRelation` (INNER/LEFT/RIGHT/FULL/CROSS/SEMI/ANTI/ASOF/LATERAL)
- `where`, `groupingElement` (ROLLUP/CUBE/GROUPING SETS/ALL)
- `having`, `qualify`, `order by`, `limitElement`
- set operations (`UNION`, `INTERSECT`, `EXCEPT`, `MINUS`)
- `explainDesc`

## Covered Grammar Paths (DML)

- `insertStatement` (`INTO`/`OVERWRITE`, `SELECT`/`VALUES`, `BY NAME`)
- `updateStatement` (with optional `FROM`)
- `deleteStatement` (with optional `USING`)
- `mergeIntoStatement`

## Notes

- Generated SQL aims for parser/planner coverage; not every statement is guaranteed to execute successfully on all clusters.
- Increase `--max-depth` for deeper nested subqueries/expressions.
- Use `--mode all` to combine deterministic path coverage with random variants.

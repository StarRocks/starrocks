# Iceberg TPC-DS 1T query dumps

One query dump per TPC-DS query (103 files, `qNN.json.gz`), captured on a 1T iceberg cluster with real, analyzed
statistics. `IcebergTPCDS1TReplayTest` replays every dump, so an optimizer change can be checked against real 1T
statistics in FE UT instead of a benchmark round.

## Source

- Cluster: 1 FE + 4 BE (16 cores each), shared-nothing, captured 2026-10-01. FE and BE were built from the PR that
  added this directory (base `e8eacab`), plus the statistics-cache fix of #64254.
  The backend count matters: broadcast vs shuffle and parallelism depend on it. `IcebergTPCDS1TReplayTest` mocks the
  dump's real backend count, not the 3 the other dump tests use.
- Database: `iceberg.zz_iceberg_tpcds_sf1000_iceberg_parquet_zstd`, iceberg format V3, parquet + zstd, HMS catalog.
  The fact tables are partitioned by their date key (1824-2185 partitions; inventory 261).
- Queries: the 103 TPC-DS query texts of the StarRocks sql-bench suite (`q01` ... `q99`, with the multi-part
  `q14-1/2`, `q23-1/2`, `q24-1/2`, `q39-1/2`).

## Statistics

The statistics are what query-trigger analyze (`enable_query_trigger_analyze`) collects when the whole TPC-DS
query set runs, reproduced by the statements below (run in the database above):

- only the columns the queries reference;
- SAMPLE for tables with more than `statistic_sample_collect_partition_size` (300) partitions, FULL for the others
  (`ConnectorAnalyzeTask`);
- default sample caps (`connector_table_analyze_scan_rows_cap` etc.).

```sql
ANALYZE SAMPLE warehouse;
ANALYZE SAMPLE ship_mode;
ANALYZE SAMPLE income_band;
ANALYZE SAMPLE call_center;
ANALYZE SAMPLE web_site;
ANALYZE SAMPLE reason;
ANALYZE SAMPLE store;
ANALYZE SAMPLE promotion;
ANALYZE SAMPLE web_page;
ANALYZE SAMPLE household_demographics;
ANALYZE SAMPLE catalog_page;
ANALYZE SAMPLE date_dim;
ANALYZE SAMPLE time_dim;
ANALYZE SAMPLE item;
ANALYZE SAMPLE customer_demographics;
ANALYZE SAMPLE customer_address;
ANALYZE SAMPLE customer;
ANALYZE SAMPLE web_returns;
ANALYZE SAMPLE catalog_returns;
ANALYZE SAMPLE store_returns;
ANALYZE SAMPLE web_sales;
ANALYZE SAMPLE inventory;
ANALYZE SAMPLE catalog_sales;
ANALYZE SAMPLE store_sales;
```

## Capture

```bash
curl -u root: -X POST --data-binary @qNN.sql \
  "http://<fe>:<http_port>/api/query_dump?db=iceberg.zz_iceberg_tpcds_sf1000_iceberg_parquet_zstd&mock=false"
```

- Run every query twice and keep the second dump. Connector column statistics load asynchronously, so a cold
  cache plans the first query with UNKNOWN statistics.
- Check every dump before checking it in: no column statistic of type UNKNOWN, an empty `exception`, and
  `external_column_row_count` / `external_table_format_version` present.
- `gzip -9 -n`. Raw, the dumps are ~30 MB, mostly the per-partition names and row counts of the fact tables, which
  replay needs for partition pruning.

## Replay fidelity

Each dump carries the cluster's own `EXPLAIN COSTS` (`explain_info`). With `TPCDS1T_REPLAY_OUT=<dir>`,
`IcebergTPCDS1TReplayTest` writes `<q>.replay` and `<q>.cluster` there for diffing.

At capture time, all 103 replayed plans matched the cluster's node for node, including distribution, join operator
and every cardinality. 56 of them were line-for-line identical, ignoring scan metrics and snapshot ids.

The remaining line differences:
- `group by min-max stats`: the dump records column min/max for native tables only;
- the last digit of a few statistics;
- the order in which column statistics are listed.

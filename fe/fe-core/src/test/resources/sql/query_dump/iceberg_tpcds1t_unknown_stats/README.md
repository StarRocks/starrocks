# Iceberg TPC-DS 1T query dumps without column statistics

One query dump per TPC-DS query (103 files, `qNN.json.gz`), captured on a 1T iceberg cluster before any column
statistics were collected. Row counts are known; every column statistic is UNKNOWN. This is what a query sees when
it is planned early in a run with query-trigger analyze (`enable_query_trigger_analyze`), before the analyze it
triggers has finished. `IcebergTPCDS1TUnknownStatsReplayTest` replays every dump.

The same queries with analyzed statistics are in `../iceberg_tpcds1t`.

## Source

- Cluster: 1 FE + 4 BE (16 cores each), shared-nothing, captured 2026-10-08. FE built from `5943a79`
  (`tpcds-benchmark-candidate`), with its plan options at their defaults, so the `explain_info` of each dump is the
  plan the cluster ran. That build carried plan PRs not yet on main,
  so a replay on main can differ from `explain_info`: at capture, 101 of 103 replays had the cluster's join
  structure; q05 (one join type) and q67 (group key encoding, #64548) did not.
- Database: `iceberg.zz_iceberg_tpcds_sf1000_iceberg_parquet_zstd`, the same as `../iceberg_tpcds1t`.
- Queries: the same 103 TPC-DS query texts of the StarRocks sql-bench suite as `../iceberg_tpcds1t`.
- Statistics: dropped on the cluster before capture. Each dump was checked to have UNKNOWN for every column, known
  table row counts and `be_number` 4.

## Capture

```bash
curl -u root: -X POST --data-binary @q72.sql \
  "http://<fe>:8030/api/query_dump?db=iceberg.zz_iceberg_tpcds_sf1000_iceberg_parquet_zstd&mock=false" \
  | gzip -9 > q72.json.gz
```

## Replay

- The test sets `FeConstants.isReplayFromQueryDump`: with unknown column statistics, `ReorderJoinRule` only uses the
  left-deep reorder on a cluster, but runs DP and greedy in plain unit tests.
- To compare with the cluster plans, set `TPCDS1T_UNKNOWN_STATS_REPLAY_OUT=<dir>`. The test writes `<query>.cluster`
  (the dump's `explain_info`) and `<query>.replay` there.

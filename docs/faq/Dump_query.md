# QueryDump interface

This topic describes how to use the QueryDump interface of StarRocks.

If you encounter any of the following issues when executing SQL queries with StarRocks, you can use QueryDump to send the SQL and related information to StarRocks technical support, and StarRocks will assist in troubleshooting at the first time.

* `Unknown Error` is returned when you execute SQL or EXPLAIN.
* An error message or exception is returned when you execute SQL.
* Executing SQL isn't as efficient as expected, or the execution plan can be optimized (for example, partitions can be pruned or Join order can be adjusted).

## Feature

The QueryDump interface will return the information that FE relies on when executing SQL, including:

* Query statement
* Table creation statement
* Session variables
* Number of BEs
* Statistics information (Min, Max values)
* Exception information (exception stack)

## HTTP interface

HTTP Post

```shell
 fe_host:fe_http_port/api/query_dump?db=${database} post_data=${Query}
```

```shell
wget --user=${username} --password=${password} --post-file ${query_file} http://${fe_host}:${fe_http_port}/api/query_dump?db=${database} -O ${dump_file}
```

Parameter description:

* query_file: the file containing the query
* dump_file: the output file
* db: the database where the SQL is executed. The `db` parameter is optional if the query includes `use db`. Otherwise, it must be specified.

Example

```shell
wget --user=root --password=123 --post-file query_file http://127.0.0.1:8030/api/query_dump?db=tpch -O dump_file
```

## Return data

Data is returned in JSON format.

```json
{
  "statement": "select\n    l_returnflag,\n    l_linestatus,\n    sum(l_quantity) as sum_qty,\n    sum(l_extendedprice) as sum_base_price,\n    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n    avg(l_quantity) as avg_qty,\n    avg(l_extendedprice) as avg_price,\n    avg(l_discount) as avg_disc,\n    count(*) as count_order\nfrom\n    lineitem\nwhere\n    l_shipdate \u003c\u003d date \u00271998-12-01\u0027\ngroup by\n    l_returnflag,\n    l_linestatus\norder by\n    l_returnflag,\n    l_linestatus ;\n",
  "table_meta": {
    "test.lineitem": "CREATE TABLE `lineitem` (\n  `L_ORDERKEY` int(11) NOT NULL COMMENT \"\",\n  `L_PARTKEY` int(11) NOT NULL COMMENT \"\",\n  `L_SUPPKEY` int(11) NOT NULL COMMENT \"\",\n  `L_LINENUMBER` int(11) NOT NULL COMMENT \"\",\n  `L_QUANTITY` double NOT NULL COMMENT \"\",\n  `L_EXTENDEDPRICE` double NOT NULL COMMENT \"\",\n  `L_DISCOUNT` double NOT NULL COMMENT \"\",\n  `L_TAX` double NOT NULL COMMENT \"\",\n  `L_RETURNFLAG` char(1) NOT NULL COMMENT \"\",\n  `L_LINESTATUS` char(1) NOT NULL COMMENT \"\",\n  `L_SHIPDATE` date NOT NULL COMMENT \"\",\n  `L_COMMITDATE` date NOT NULL COMMENT \"\",\n  `L_RECEIPTDATE` date NOT NULL COMMENT \"\",\n  `L_SHIPINSTRUCT` char(25) NOT NULL COMMENT \"\",\n  `L_SHIPMODE` char(10) NOT NULL COMMENT \"\",\n  `L_COMMENT` varchar(44) NOT NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`L_ORDERKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`L_ORDERKEY`) BUCKETS 20 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"storage_format\" \u003d \"DEFAULT\"\n);"
  },
  "table_row_count": {
    "test.lineitem": {
      "lineitem": 600000000
    }
  },
  "session_variables": "{\"runtime_join_filter_push_down_limit\":1024000,\"codegen_level\":0,\"character_set_connection\":\"utf8\",\"enable_insert_strict\":true,\"div_precision_increment\":4,\"tx_isolation\":\"REPEATABLE-READ\",\"wait_timeout\":28800,\"auto_increment_increment\":1,\"foreign_key_checks\":true,\"character_set_client\":\"utf8\",\"autocommit\":true,\"character_set_results\":\"utf8\",\"parallel_fragment_exec_instance_num\":1,\"max_scan_key_num\":-1,\"enable_global_runtime_filter\":true,\"forward_to_master\":false,\"net_read_timeout\":60,\"streaming_preaggregation_mode\":\"auto\",\"storage_engine\":\"olap\",\"tx_visible_wait_timeout\":10,\"new_planner_optimize_timeout\":3000,\"force_schedule_local\":false,\"enable_query_dump\":false,\"prefer_join_method\":\"broadcast\",\"load_mem_limit\":0,\"sql_select_limit\":9223372036854775807,\"profiling\":false,\"sql_safe_updates\":0,\"enable_new_planner_mock_tpch_statistic\":true,\"query_cache_type\":0,\"use_v2_rollup\":false,\"disable_colocate_join\":false,\"max_pushdown_conditions_per_column\":-1,\"global_runtime_filter_max_size\":4096000,\"new_planner_tpch_scale\":100,\"enable_vectorized_engine\":true,\"net_write_timeout\":60,\"collation_database\":\"utf8_general_ci\",\"hash_join_push_down_right_table\":true,\"new_planner_agg_stage\":0,\"enable_runtime_filter_from_planner\":true,\"collation_connection\":\"utf8_general_ci\",\"resource_group\":\"normal\",\"enable_new_planner_push_down_join_to_agg\":false,\"broadcast_row_limit\":15000000,\"exec_mem_limit\":2147483648,\"disable_join_reorder\":false,\"enable_profile\":false,\"global_runtime_filter_rpc_timeout\":400,\"enable_groupby_use_output_alias\":false,\"global_runtime_filter_wait_timeout\":200,\"enable_vectorized_insert\":true,\"net_buffer_length\":16384,\"transmission_compression_type\":\"LZ4\",\"interactive_timeout\":3600,\"enable_spilling\":false,\"batch_size\":1024,\"max_allowed_packet\":1048576,\"query_timeout\":300,\"test_materialized_view\":false,\"enable_cbo\":false,\"collation_server\":\"utf8_general_ci\",\"new_planner_max_transform_reorder_joins\":8,\"time_zone\":\"Asia/Shanghai\",\"max_execution_time\":3000000,\"character_set_server\":\"utf8\",\"rewrite_count_distinct_to_bitmap_hll\":true,\"parallel_exchange_instance_num\":-1,\"sql_mode\":0,\"SQL_AUTO_IS_NULL\":false,\"event_scheduler\":\"OFF\",\"disable_streaming_preaggregations\":false}",
  "column_statistics": {
    "test.lineitem": {
      "L_TAX": "[0.0, 0.08, 0.0, 8.0, 9.0]",
      "L_SHIPDATE": "[6.942816E8, 9.124416E8, 0.0, 4.0, 2526.0]",
      "L_EXTENDEDPRICE": "[901.0, 104949.5, 0.0, 8.0, 932377.0]",
      "L_DISCOUNT": "[0.0, 0.1, 0.0, 8.0, 11.0]",
      "L_RETURNFLAG": "[-Infinity, Infinity, 0.0, 1.0, 3.0]",
      "L_LINESTATUS": "[-Infinity, Infinity, 0.0, 1.0, 2.0]",
      "L_QUANTITY": "[1.0, 50.0, 0.0, 8.0, 50.0]"
    }
  },
  "be_number": 3,
  "exception": []
}
```

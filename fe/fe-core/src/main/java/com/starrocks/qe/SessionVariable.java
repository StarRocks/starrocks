// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/SessionVariable.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.qe;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.CompressionUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.VariableMgr.VarAttr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.system.BackendCoreStat;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TOverflowMode;
import com.starrocks.thrift.TPipelineProfileLevel;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TSpillMode;
import com.starrocks.thrift.TTabletInternalParallelMode;
import org.apache.commons.lang3.EnumUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.TestOnly;
import org.json.JSONObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

// System variable
@SuppressWarnings("FieldMayBeFinal")
public class SessionVariable implements Serializable, Writable, Cloneable {
    private static final Logger LOG = LogManager.getLogger(SessionVariable.class);

    public static final SessionVariable DEFAULT_SESSION_VARIABLE = new SessionVariable();
    private static final Gson GSON = new GsonBuilder()
            .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE) // explicit default, may be omitted
            .create();

    public static final String USE_COMPUTE_NODES = "use_compute_nodes";
    public static final String PREFER_COMPUTE_NODE = "prefer_compute_node";
    public static final String EXEC_MEM_LIMIT = "exec_mem_limit";

    /**
     * configure the mem limit of load process on BE.
     * Previously users used exec_mem_limit to set memory limits.
     * To maintain compatibility, the default value of load_mem_limit is 0,
     * which means that the load memory limit is still using exec_mem_limit.
     * Users can set a value greater than zero to explicitly specify the load memory limit.
     * This variable is mainly for INSERT operation, because INSERT operation has both query and load part.
     * Using only the exec_mem_limit variable does not make a good distinction of memory limit between the two parts.
     */
    public static final String LOAD_MEM_LIMIT = "load_mem_limit";

    /**
     * The mem limit of query on BE. It takes effects only when enabling pipeline engine.
     * If `query_mem_limit` > 0, use it to limit the memory of a query.
     * Otherwise, no limitation
     */
    public static final String QUERY_MEM_LIMIT = "query_mem_limit";

    public static final String QUERY_TIMEOUT = "query_timeout";

    /*
     * When FE does not set the pagecache parameter, we expect a query to follow the pagecache policy of BE.
     * If pagecache is set by FE, a query whether to use pagecache follows the policy specified by FE.
     */
    public static final String USE_PAGE_CACHE = "use_page_cache";

    public static final String QUERY_DELIVERY_TIMEOUT = "query_delivery_timeout";
    public static final String MAX_EXECUTION_TIME = "max_execution_time";
    public static final String IS_REPORT_SUCCESS = "is_report_success";
    public static final String ENABLE_PROFILE = "enable_profile";

    public static final String ENABLE_LOAD_PROFILE = "enable_load_profile";
    public static final String PROFILING = "profiling";
    public static final String SQL_MODE = "sql_mode";
    /**
     * Because we modified the default value of sql_mode.
     * The default value in v1 version is 0, and in v2 we support sql mode not set only_full_group_by.
     * In order to ensure the consistency of logic,
     * the storage name of sql_mode is changed here, in order to achieve compatibility
     */
    public static final String SQL_MODE_STORAGE_NAME = "sql_mode_v2";
    public static final String RESOURCE_GROUP = "resource_group";
    public static final String AUTO_COMMIT = "autocommit";
    public static final String TX_ISOLATION = "tx_isolation";
    public static final String TX_READ_ONLY = "tx_read_only";
    public static final String TRANSACTION_ISOLATION = "transaction_isolation";
    public static final String TRANSACTION_READ_ONLY = "transaction_read_only";
    public static final String CHARACTER_SET_CLIENT = "character_set_client";
    public static final String CHARACTER_SET_CONNNECTION = "character_set_connection";
    public static final String CHARACTER_SET_RESULTS = "character_set_results";
    public static final String CHARACTER_SET_SERVER = "character_set_server";
    public static final String COLLATION_CONNECTION = "collation_connection";
    public static final String COLLATION_DATABASE = "collation_database";
    public static final String COLLATION_SERVER = "collation_server";
    public static final String SQL_AUTO_IS_NULL = "SQL_AUTO_IS_NULL";
    public static final String SQL_SELECT_LIMIT = "sql_select_limit";
    public static final String MAX_ALLOWED_PACKET = "max_allowed_packet";
    public static final String AUTO_INCREMENT_INCREMENT = "auto_increment_increment";
    public static final String QUERY_CACHE_TYPE = "query_cache_type";
    public static final String INTERACTIVE_TIMTOUT = "interactive_timeout";
    public static final String WAIT_TIMEOUT = "wait_timeout";
    public static final String WAREHOUSE = "warehouse";
    public static final String NET_WRITE_TIMEOUT = "net_write_timeout";
    public static final String NET_READ_TIMEOUT = "net_read_timeout";
    public static final String TIME_ZONE = "time_zone";
    public static final String INNODB_READ_ONLY = "innodb_read_only";
    public static final String SQL_SAFE_UPDATES = "sql_safe_updates";
    public static final String NET_BUFFER_LENGTH = "net_buffer_length";
    public static final String CODEGEN_LEVEL = "codegen_level";
    public static final String BATCH_SIZE = "batch_size";
    public static final String CHUNK_SIZE = "chunk_size";
    public static final String STREAMING_PREAGGREGATION_MODE = "streaming_preaggregation_mode";
    public static final String DISABLE_COLOCATE_JOIN = "disable_colocate_join";
    public static final String DISABLE_BUCKET_JOIN = "disable_bucket_join";
    public static final String PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM = "parallel_fragment_exec_instance_num";
    public static final String MAX_PARALLEL_SCAN_INSTANCE_NUM = "max_parallel_scan_instance_num";
    public static final String ENABLE_INSERT_STRICT = "enable_insert_strict";
    public static final String ENABLE_SPILL = "enable_spill";
    public static final String SPILLABLE_OPERATOR_MASK = "spillable_operator_mask";
    // spill mode: auto, force
    public static final String SPILL_MODE = "spill_mode";
    // enable table pruning(RBO) in cardinality-preserving joins
    public static final String ENABLE_RBO_TABLE_PRUNE = "enable_rbo_table_prune";

    // enable table pruning(CBO) in cardinality-preserving joins
    public static final String ENABLE_CBO_TABLE_PRUNE = "enable_cbo_table_prune";
    // if set to true, some of stmt will be forwarded to leader FE to get result

    // if set to true, some of stmt will be forwarded to leader FE to get result
    public static final String FORWARD_TO_LEADER = "forward_to_leader";
    public static final String FORWARD_TO_MASTER = "forward_to_master";
    // user can set instance num after exchange, no need to be equal to nums of before exchange
    public static final String PARALLEL_EXCHANGE_INSTANCE_NUM = "parallel_exchange_instance_num";
    public static final String EVENT_SCHEDULER = "event_scheduler";
    public static final String STORAGE_ENGINE = "storage_engine";
    public static final String DIV_PRECISION_INCREMENT = "div_precision_increment";

    // see comment of `starrocks_max_scan_key_num` and `max_pushdown_conditions_per_column` in BE config
    public static final String MAX_SCAN_KEY_NUM = "max_scan_key_num";
    public static final String MAX_PUSHDOWN_CONDITIONS_PER_COLUMN = "max_pushdown_conditions_per_column";

    // use new execution engine instead of the old one if enable_pipeline_engine is true,
    // the new execution engine split a fragment into pipelines, then create several drivers
    // from the pipeline for parallel executing, threads from global pool pick out the
    // ready-to-run drivers to execute and switch the blocked drivers off cores;
    // the old one put each fragment into a thread, then pull final result from the root fragment,
    // leaf fragments always waiting for disk IO's completion and intermediate fragments wait
    // for chunk arrival and blocked on receive queues of its exchange node, so among
    // threads allocated for a query in the old execution engine, only small number of
    // them do the real work on core.
    public static final String ENABLE_PIPELINE = "enable_pipeline";

    public static final String ENABLE_RUNTIME_ADAPTIVE_DOP = "enable_runtime_adaptive_dop";
    public static final String ADAPTIVE_DOP_MAX_BLOCK_ROWS_PER_DRIVER_SEQ =
            "runtime_adaptive_dop_max_block_rows_per_driver_seq";
    public static final String ADAPTIVE_DOP_MAX_OUTPUT_AMPLIFICATION_FACTOR =
            "runtime_adaptive_dop_max_output_amplification_factor";

    public static final String ENABLE_PIPELINE_ENGINE = "enable_pipeline_engine";

    public static final String MAX_BUCKETS_PER_BE_TO_USE_BALANCER_ASSIGNMENT = "max_buckets_per_be_to_use_balancer_assignment";

    public static final String ENABLE_MV_PLANNER = "enable_mv_planner";
    public static final String ENABLE_INCREMENTAL_REFRESH_MV = "enable_incremental_mv";

    public static final String LOG_REJECTED_RECORD_NUM = "log_rejected_record_num";

    /**
     * Whether to allow the generation of one-phase local aggregation with the local shuffle operator
     * (ScanNode->LocalShuffleNode->OnePhaseAggNode) regardless of the differences between grouping keys
     * and scan distribution keys, when there is only one BE.
     */
    public static final String ENABLE_LOCAL_SHUFFLE_AGG = "enable_local_shuffle_agg";

    public static final String ENABLE_QUERY_TABLET_AFFINITY = "enable_query_tablet_affinity";

    public static final String ENABLE_TABLET_INTERNAL_PARALLEL = "enable_tablet_internal_parallel";
    public static final String ENABLE_TABLET_INTERNAL_PARALLEL_V2 = "enable_tablet_internal_parallel_v2";

    public static final String TABLET_INTERNAL_PARALLEL_MODE = "tablet_internal_parallel_mode";
    public static final String ENABLE_SHARED_SCAN = "enable_shared_scan";
    public static final String PIPELINE_DOP = "pipeline_dop";
    public static final String MAX_PIPELINE_DOP = "max_pipeline_dop";

    public static final String PROFILE_TIMEOUT = "profile_timeout";
    public static final String RUNTIME_PROFILE_REPORT_INTERVAL = "runtime_profile_report_interval";
    public static final String PIPELINE_PROFILE_LEVEL = "pipeline_profile_level";
    public static final String ENABLE_ASYNC_PROFILE = "enable_async_profile";

    public static final String RESOURCE_GROUP_ID = "workgroup_id";
    public static final String RESOURCE_GROUP_ID_V2 = "resource_group_id";

    // hash join right table push down
    public static final String HASH_JOIN_PUSH_DOWN_RIGHT_TABLE = "hash_join_push_down_right_table";

    // disable join reorder
    public static final String DISABLE_JOIN_REORDER = "disable_join_reorder";

    // open predicate reorder
    public static final String ENABLE_PREDICATE_REORDER = "enable_predicate_reorder";

    public static final String ENABLE_FILTER_UNUSED_COLUMNS_IN_SCAN_STAGE =
            "enable_filter_unused_columns_in_scan_stage";

    // the maximum time, in seconds, waiting for an insert statement's transaction state
    // transfer from COMMITTED to VISIBLE.
    // If the time exceeded but the transaction state is not VISIBLE, the transaction will
    // still be considered as succeeded and an OK packet will be sent to the client, but
    // the affected records may not be visible to the subsequent queries, even if it's in
    // the same session.
    public static final String TRANSACTION_VISIBLE_WAIT_TIMEOUT = "tx_visible_wait_timeout";
    // only for Aliyun DTS, no actual use.
    public static final String FOREIGN_KEY_CHECKS = "foreign_key_checks";

    // force schedule local be for HybridBackendSelector
    // only for hive external table now
    public static final String FORCE_SCHEDULE_LOCAL = "force_schedule_local";

    // --------  New planner session variables start --------
    public static final String NEW_PLANER_AGG_STAGE = "new_planner_agg_stage";
    public static final String BROADCAST_ROW_LIMIT = "broadcast_row_limit";
    public static final String BROADCAST_RIGHT_TABLE_SCALE_FACTOR =
            "broadcast_right_table_scale_factor";
    public static final String NEW_PLANNER_OPTIMIZER_TIMEOUT = "new_planner_optimize_timeout";
    public static final String ENABLE_GROUPBY_USE_OUTPUT_ALIAS = "enable_groupby_use_output_alias";
    public static final String ENABLE_QUERY_DUMP = "enable_query_dump";
    public static final String QUERY_DEBUG_OPTIONS = "query_debug_options";

    // --------------------------- Limitations for Materialized View ------------------------------------ //
    public static final String OPTIMIZER_MATERIALIZED_VIEW_TIMELIMIT = "optimizer_materialized_view_timelimit";
    public static final String CBO_MATERIALIZED_VIEW_REWRITE_RULE_OUTPUT_LIMIT =
            "cbo_materialized_view_rewrite_rule_output_limit";
    public static final String CBO_MATERIALIZED_VIEW_REWRITE_CANDIDATE_LIMIT =
            "cbo_materialized_view_rewrite_candidate_limit";

    public static final String CBO_MAX_REORDER_NODE_USE_EXHAUSTIVE = "cbo_max_reorder_node_use_exhaustive";
    public static final String CBO_ENABLE_DP_JOIN_REORDER = "cbo_enable_dp_join_reorder";
    public static final String CBO_MAX_REORDER_NODE_USE_DP = "cbo_max_reorder_node_use_dp";
    public static final String CBO_ENABLE_GREEDY_JOIN_REORDER = "cbo_enable_greedy_join_reorder";
    public static final String CBO_ENABLE_REPLICATED_JOIN = "cbo_enable_replicated_join";
    public static final String CBO_USE_CORRELATED_JOIN_ESTIMATE = "cbo_use_correlated_join_estimate";
    public static final String CBO_ENABLE_LOW_CARDINALITY_OPTIMIZE = "cbo_enable_low_cardinality_optimize";
    public static final String CBO_USE_NTH_EXEC_PLAN = "cbo_use_nth_exec_plan";
    public static final String CBO_CTE_REUSE = "cbo_cte_reuse";
    public static final String CBO_CTE_REUSE_RATE = "cbo_cte_reuse_rate";
    public static final String CBO_CTE_MAX_LIMIT = "cbo_cte_max_limit";
    public static final String CBO_CTE_REUSE_RATE_V2 = "cbo_cte_reuse_rate_v2";
    public static final String PREFER_CTE_REWRITE = "prefer_cte_rewrite";
    public static final String ENABLE_SQL_DIGEST = "enable_sql_digest";
    public static final String CBO_MAX_REORDER_NODE = "cbo_max_reorder_node";
    public static final String CBO_PRUNE_SHUFFLE_COLUMN_RATE = "cbo_prune_shuffle_column_rate";
    public static final String CBO_PUSH_DOWN_AGGREGATE_MODE = "cbo_push_down_aggregate_mode";

    public static final String CBO_PUSH_DOWN_DISTINCT_BELOW_WINDOW = "cbo_push_down_distinct_below_window";
    public static final String CBO_PUSH_DOWN_AGGREGATE = "cbo_push_down_aggregate";
    public static final String CBO_DEBUG_ALIVE_BACKEND_NUMBER = "cbo_debug_alive_backend_number";
    public static final String CBO_PRUNE_SUBFIELD = "cbo_prune_subfield";
    public static final String ENABLE_OPTIMIZER_REWRITE_GROUPINGSETS_TO_UNION_ALL =
            "enable_rewrite_groupingsets_to_union_all";

    public static final String CBO_USE_DB_LOCK = "cbo_use_lock_db";
    public static final String CBO_PREDICATE_SUBFIELD_PATH = "cbo_enable_predicate_subfield_path";

    public static final String SKEW_JOIN_RAND_RANGE = "skew_join_rand_range";

    // --------  New planner session variables end --------

    // Type of compression of transmitted data
    // Different compression algorithms may be chosen in different hardware environments. For example,
    // in the case of insufficient network bandwidth, but excess CPU resources, an algorithm with a
    // higher compression ratio may be chosen to use more CPU and make the overall query time lower.
    public static final String TRANSMISSION_COMPRESSION_TYPE = "transmission_compression_type";
    public static final String LOAD_TRANSMISSION_COMPRESSION_TYPE = "load_transmission_compression_type";

    public static final String RUNTIME_JOIN_FILTER_PUSH_DOWN_LIMIT = "runtime_join_filter_push_down_limit";
    public static final String ENABLE_GLOBAL_RUNTIME_FILTER = "enable_global_runtime_filter";
    public static final String GLOBAL_RUNTIME_FILTER_BUILD_MAX_SIZE = "global_runtime_filter_build_max_size";

    public static final String GLOBAL_RUNTIME_FILTER_BUILD_MIN_SIZE = "global_runtime_filter_build_min_size";
    public static final String GLOBAL_RUNTIME_FILTER_PROBE_MIN_SIZE = "global_runtime_filter_probe_min_size";
    public static final String GLOBAL_RUNTIME_FILTER_PROBE_MIN_SELECTIVITY =
            "global_runtime_filter_probe_min_selectivity";
    public static final String GLOBAL_RUNTIME_FILTER_WAIT_TIMEOUT = "global_runtime_filter_wait_timeout";
    public static final String GLOBAL_RUNTIME_FILTER_RPC_TIMEOUT = "global_runtime_filter_rpc_timeout";
    public static final String RUNTIME_FILTER_EARLY_RETURN_SELECTIVITY = "runtime_filter_early_return_selectivity";
    public static final String ENABLE_TOPN_RUNTIME_FILTER = "enable_topn_runtime_filter";
    public static final String GLOBAL_RUNTIME_FILTER_RPC_HTTP_MIN_SIZE = "global_runtime_filter_rpc_http_min_size";

    public static final String ENABLE_COLUMN_EXPR_PREDICATE = "enable_column_expr_predicate";
    public static final String ENABLE_EXCHANGE_PASS_THROUGH = "enable_exchange_pass_through";
    public static final String ENABLE_EXCHANGE_PERF = "enable_exchange_perf";

    public static final String SINGLE_NODE_EXEC_PLAN = "single_node_exec_plan";

    public static final String ALLOW_DEFAULT_PARTITION = "allow_default_partition";

    public static final String ENABLE_PRUNE_ICEBERG_MANIFEST = "enable_prune_iceberg_manifest";

    public static final String ENABLE_READ_ICEBERG_PUFFIN_NDV = "enable_read_iceberg_puffin_ndv";

    public static final String ENABLE_ICEBERG_COLUMN_STATISTICS = "enable_iceberg_column_statistics";

    public static final String ENABLE_HIVE_COLUMN_STATS = "enable_hive_column_stats";

    public static final String ENABLE_WRITE_HIVE_EXTERNAL_TABLE = "enable_write_hive_external_table";

    public static final String ENABLE_HIVE_METADATA_CACHE_WITH_INSERT = "enable_hive_metadata_cache_with_insert";

    public static final String DEFAULT_TABLE_COMPRESSION = "default_table_compression";

    // In most cases, the partition statistics obtained from the hive metastore are empty.
    // Because we get partition statistics asynchronously for the first query of a table or partition,
    // if the gc of any service is caused, you can set the value to 100 for testing.
    public static final String HIVE_PARTITION_STATS_SAMPLE_SIZE = "hive_partition_stats_sample_size";

    public static final String PIPELINE_SINK_DOP = "pipeline_sink_dop";
    public static final String ENABLE_ADAPTIVE_SINK_DOP = "enable_adaptive_sink_dop";
    public static final String RUNTIME_FILTER_SCAN_WAIT_TIME = "runtime_filter_scan_wait_time";
    public static final String RUNTIME_FILTER_ON_EXCHANGE_NODE = "runtime_filter_on_exchange_node";
    public static final String ENABLE_MULTI_COLUMNS_ON_GLOBAL_RUNTIME_FILTER =
            "enable_multicolumn_global_runtime_filter";

    // command, file
    public static final String TRACE_LOG_MODE = "trace_log_mode";
    public static final String JOIN_IMPLEMENTATION_MODE = "join_implementation_mode";
    public static final String JOIN_IMPLEMENTATION_MODE_V2 = "join_implementation_mode_v2";

    public static final String STATISTIC_COLLECT_PARALLEL = "statistic_collect_parallel";

    public static final String ENABLE_SHOW_ALL_VARIABLES = "enable_show_all_variables";

    public static final String ENABLE_QUERY_DEBUG_TRACE = "enable_query_debug_trace";

    public static final String INTERPOLATE_PASSTHROUGH = "interpolate_passthrough";

    public static final String HASH_JOIN_INTERPOLATE_PASSTHROUGH = "hash_join_interpolate_passthrough";

    public static final String PARSE_TOKENS_LIMIT = "parse_tokens_limit";

    public static final String ENABLE_SORT_AGGREGATE = "enable_sort_aggregate";
    public static final String ENABLE_PER_BUCKET_OPTIMIZE = "enable_per_bucket_optimize";
    public static final String ENABLE_PARALLEL_MERGE = "enable_parallel_merge";
    public static final String ENABLE_QUERY_QUEUE = "enable_query_queue";

    public static final String WINDOW_PARTITION_MODE = "window_partition_mode";

    public static final String ENABLE_SCAN_DATACACHE = "enable_scan_datacache";
    public static final String ENABLE_POPULATE_DATACACHE = "enable_populate_datacache";
    // The following configurations will be deprecated, and we use the `datacache` suffix instead.
    // But it is temporarily necessary to keep them for a period of time to be compatible with
    // the old session variable names.
    public static final String ENABLE_SCAN_BLOCK_CACHE = "enable_scan_block_cache";
    public static final String ENABLE_POPULATE_BLOCK_CACHE = "enable_populate_block_cache";

    public static final String ENABLE_FILE_METACACHE = "enable_file_metacache";
    public static final String HUDI_MOR_FORCE_JNI_READER = "hudi_mor_force_jni_reader";
    public static final String PAIMON_FORCE_JNI_READER = "paimon_force_jni_reader";
    public static final String IO_TASKS_PER_SCAN_OPERATOR = "io_tasks_per_scan_operator";
    public static final String CONNECTOR_IO_TASKS_PER_SCAN_OPERATOR = "connector_io_tasks_per_scan_operator";
    public static final String ENABLE_CONNECTOR_ADAPTIVE_IO_TASKS = "enable_connector_adaptive_io_tasks";
    public static final String CONNECTOR_IO_TASKS_SLOW_IO_LATENCY_MS = "connector_io_tasks_slow_io_latency_ms";
    public static final String SCAN_USE_QUERY_MEM_RATIO = "scan_use_query_mem_ratio";
    public static final String CONNECTOR_SCAN_USE_QUERY_MEM_RATIO = "connector_scan_use_query_mem_ratio";
    public static final String CONNECTOR_SINK_COMPRESSION_CODEC = "connector_sink_compression_codec";
    public static final String ENABLE_QUERY_CACHE = "enable_query_cache";
    public static final String QUERY_CACHE_FORCE_POPULATE = "query_cache_force_populate";
    public static final String QUERY_CACHE_ENTRY_MAX_BYTES = "query_cache_entry_max_bytes";
    public static final String QUERY_CACHE_ENTRY_MAX_ROWS = "query_cache_entry_max_rows";

    // We assume that for PRIMARY_KEYS and UNIQUE_KEYS, the latest partitions are hot partitions that are updated
    // frequently, so it should not be cached in query cache since its disruptive cache invalidation.
    public static final String QUERY_CACHE_HOT_PARTITION_NUM = "query_cache_hot_partition_num";

    public static final String QUERY_CACHE_AGG_CARDINALITY_LIMIT = "query_cache_agg_cardinality_limit";
    public static final String TRANSMISSION_ENCODE_LEVEL = "transmission_encode_level";
    public static final String RPC_HTTP_MIN_SIZE = "rpc_http_min_size";

    public static final String NESTED_MV_REWRITE_MAX_LEVEL = "nested_mv_rewrite_max_level";
    public static final String ENABLE_MATERIALIZED_VIEW_REWRITE = "enable_materialized_view_rewrite";
    public static final String ENABLE_MATERIALIZED_VIEW_UNION_REWRITE = "enable_materialized_view_union_rewrite";
    public static final String ENABLE_MATERIALIZED_VIEW_REWRITE_PARTITION_COMPENSATE =
            "enable_materialized_view_rewrite_partition_compensate";

    public static final String LARGE_DECIMAL_UNDERLYING_TYPE = "large_decimal_underlying_type";

    public static final String ENABLE_ICEBERG_IDENTITY_COLUMN_OPTIMIZE = "enable_iceberg_identity_column_optimize";
    public static final String ENABLE_PIPELINE_LEVEL_SHUFFLE = "enable_pipeline_level_shuffle";

    public static final String ENABLE_PLAN_SERIALIZE_CONCURRENTLY = "enable_plan_serialize_concurrently";

    public static final String ENABLE_STRICT_ORDER_BY = "enable_strict_order_by";

    // Flag to control whether to proxy follower's query statement to leader/follower.
    public enum FollowerQueryForwardMode {
        DEFAULT,    // proxy queries by the follower's replay progress (default)
        FOLLOWER,   // proxy queries to follower no matter the follower's replay progress
        LEADER      // proxy queries to leader no matter the follower's replay progress
    }
    public static final String FOLLOWER_QUERY_FORWARD_MODE = "follower_query_forward_mode";

    public enum MaterializedViewRewriteMode {
        DISABLE,            // disable materialized view rewrite
        DEFAULT,            // default, choose the materialized view or not by cost optimizer
        DEFAULT_OR_ERROR,   // default, but throw exception if no materialized view is not chosen.
        FORCE,              // force to choose the materialized view if possible, otherwise use the original query
        FORCE_OR_ERROR;     // force to choose the materialized view if possible, throw exception if no materialized view is
        // not chosen.

        public static String MODE_DISABLE = DISABLE.toString();
        public static String MODE_DEFAULT = DEFAULT.toString();
        public static String MODE_DEFAULT_OR_ERROR = DEFAULT_OR_ERROR.toString();
        public static String MODE_FORCE = FORCE.toString();
        public static String MODE_FORCE_OR_ERROR = FORCE_OR_ERROR.toString();

        public static MaterializedViewRewriteMode parse(String str) {
            return EnumUtils.getEnumIgnoreCase(MaterializedViewRewriteMode.class, str);
        }
    }

    public static final String MATERIALIZED_VIEW_REWRITE_MODE = "materialized_view_rewrite_mode";

    public static final String ENABLE_MATERIALIZED_VIEW_REWRITE_FOR_INSERT = "enable_materialized_view_for_insert";

    public static final String ENABLE_SYNC_MATERIALIZED_VIEW_REWRITE = "enable_sync_materialized_view_rewrite";
    public static final String ENABLE_RULE_BASED_MATERIALIZED_VIEW_REWRITE =
            "enable_rule_based_materialized_view_rewrite";

    public static final String ENABLE_MATERIALIZED_VIEW_VIEW_DELTA_REWRITE =
            "enable_materialized_view_view_delta_rewrite";

    public static final String MATERIALIZED_VIEW_JOIN_SAME_TABLE_PERMUTATION_LIMIT =
            "materialized_view_join_same_table_permutation_limit";

    public static final String ENABLE_MATERIALIZED_VIEW_SINGLE_TABLE_VIEW_DELTA_REWRITE =
            "enable_materialized_view_single_table_view_delta_rewrite";
    public static final String ANALYZE_FOR_MV = "analyze_mv";
    public static final String QUERY_EXCLUDING_MV_NAMES = "query_excluding_mv_names";
    public static final String QUERY_INCLUDING_MV_NAMES = "query_including_mv_names";
    public static final String ENABLE_MATERIALIZED_VIEW_REWRITE_GREEDY_MODE =
            "enable_materialized_view_rewrite_greedy_mode";

    public static final String ENABLE_MATERIALIZED_VIEW_PLAN_CACHE = "enable_materialized_view_plan_cache";

    public static final String ENABLE_VIEW_BASED_MV_REWRITE = "enable_view_based_mv_rewrite";

    public static final String ENABLE_BIG_QUERY_LOG = "enable_big_query_log";
    public static final String BIG_QUERY_LOG_CPU_SECOND_THRESHOLD = "big_query_log_cpu_second_threshold";
    public static final String BIG_QUERY_LOG_SCAN_BYTES_THRESHOLD = "big_query_log_scan_bytes_threshold";
    public static final String BIG_QUERY_LOG_SCAN_ROWS_THRESHOLD = "big_query_log_scan_rows_threshold";
    public static final String BIG_QUERY_PROFILE_SECOND_THRESHOLD = "big_query_profile_second_threshold";

    public static final String SQL_DIALECT = "sql_dialect";

    public static final String ENABLE_OUTER_JOIN_REORDER = "enable_outer_join_reorder";

    public static final String CBO_REORDER_THRESHOLD_USE_EXHAUSTIVE = "cbo_reorder_threshold_use_exhaustive";
    public static final String ENABLE_REWRITE_SUM_BY_ASSOCIATIVE_RULE = "enable_rewrite_sum_by_associative_rule";
    public static final String ENABLE_REWRITE_SIMPLE_AGG_TO_META_SCAN = "enable_rewrite_simple_agg_to_meta_scan";

    public static final String ENABLE_PRUNE_COMPLEX_TYPES = "enable_prune_complex_types";
    public static final String ENABLE_PRUNE_COMPLEX_TYPES_IN_UNNEST = "enable_prune_complex_types_in_unnest";
    public static final String RANGE_PRUNER_PREDICATES_MAX_LEN = "range_pruner_max_predicate";

    public static final String GROUP_CONCAT_MAX_LEN = "group_concat_max_len";

    // These parameters are experimental. They may be removed in the future
    public static final String SPILL_MEM_TABLE_SIZE = "spill_mem_table_size";
    public static final String SPILL_MEM_TABLE_NUM = "spill_mem_table_num";
    public static final String SPILL_MEM_LIMIT_THRESHOLD = "spill_mem_limit_threshold";
    public static final String SPILL_OPERATOR_MIN_BYTES = "spill_operator_min_bytes";
    public static final String SPILL_OPERATOR_MAX_BYTES = "spill_operator_max_bytes";
    public static final String SPILL_REVOCABLE_MAX_BYTES = "spill_revocable_max_bytes";
    public static final String SPILL_ENCODE_LEVEL = "spill_encode_level";

    // full_sort_max_buffered_{rows,bytes} are thresholds that limits input size of partial_sort
    // in full sort.
    public static final String FULL_SORT_MAX_BUFFERED_ROWS = "full_sort_max_buffered_rows";

    public static final String FULL_SORT_MAX_BUFFERED_BYTES = "full_sort_max_buffered_bytes";

    // Used by full sort inorder to permute only order-by columns in cascading merging phase, after
    // that, non-order-by output columns are permuted according to the ordinal column.
    public static final String FULL_SORT_LATE_MATERIALIZATION_V2 = "full_sort_late_materialization_v2";
    public static final String FULL_SORT_LATE_MATERIALIZATION = "full_sort_late_materialization";

    // For group-by-count-distinct query like select a, count(distinct b) from t group by a, if group-by column a
    // is low-cardinality while count-distinct column b is high-cardinality, there exists a performance bottleneck
    // if column a is a0 for the majority rows, since the data is shuffle by only column a, so one PipelineDriver will
    // tackle with the majority portion of data that can not scale to multi-machines or multi-cores. so we add a
    // bucket column produced from evaluation of expression hash(b)%num_buckets to the partition-by column of shuffle
    // ExchangeNode to make the computation scale-out to multi-machines/multi-cores.
    // Here: count_distinct_column_buckets means the num_buckets and enable_distinct_column_bucketization is switch to
    // control on/off of this bucketization optimization.
    public static final String DISTINCT_COLUMN_BUCKETS = "count_distinct_column_buckets";
    public static final String ENABLE_DISTINCT_COLUMN_BUCKETIZATION = "enable_distinct_column_bucketization";
    public static final String HDFS_BACKEND_SELECTOR_SCAN_RANGE_SHUFFLE = "hdfs_backend_selector_scan_range_shuffle";

    public static final String SQL_QUOTE_SHOW_CREATE = "sql_quote_show_create";

    public static final String ENABLE_PLAN_VALIDATION = "enable_plan_validation";

    public static final String ENABLE_STRICT_TYPE = "enable_strict_type";

    public static final String PARTIAL_UPDATE_MODE = "partial_update_mode";

    public static final String SCAN_OR_TO_UNION_LIMIT = "scan_or_to_union_limit";

    public static final String SCAN_OR_TO_UNION_THRESHOLD = "scan_or_to_union_threshold";

    public static final String SELECT_RATIO_THRESHOLD = "select_ratio_threshold";

    public static final String DISABLE_FUNCTION_FOLD_CONSTANTS = "disable_function_fold_constants";

    public static final String ENABLE_SIMPLIFY_CASE_WHEN = "enable_simplify_case_when";

    public static final String ENABLE_COUNT_STAR_OPTIMIZATION = "enable_count_star_optimization";

    public static final String ENABLE_PARTITION_COLUMN_VALUE_ONLY_OPTIMIZATION =
            "enable_partition_column_value_only_optimization";

    public static final String HDFS_BACKEND_SELECTOR_HASH_ALGORITHM = "hdfs_backend_selector_hash_algorithm";

    public static final String CONSISTENT_HASH_VIRTUAL_NUMBER = "consistent_hash_virtual_number";

    public static final String ENABLE_COLLECT_TABLE_LEVEL_SCAN_STATS = "enable_collect_table_level_scan_stats";

    public static final String HIVE_TEMP_STAGING_DIR = "hive_temp_staging_dir";

    // binary, json, compact
    public static final String THRIFT_PLAN_PROTOCOL = "thrift_plan_protocol";

    // 0 means disable interleaving, positive value sets the group size, but adaptively enable interleaving,
    // negative value means force interleaving under the group size of abs(interleaving_group_size)
    public static final String INTERLEAVING_GROUP_SIZE = "interleaving_group_size";

    public static final String CBO_PUSHDOWN_TOPN_LIMIT = "cbo_push_down_topn_limit";

    public static final String ENABLE_EXPR_PRUNE_PARTITION = "enable_expr_prune_partition";

    public static final String AUDIT_EXECUTE_STMT = "audit_execute_stmt";

    public static final String CROSS_JOIN_COST_PENALTY = "cross_join_cost_penalty";

    public static final String CBO_DERIVE_RANGE_JOIN_PREDICATE = "cbo_derive_range_join_predicate";

    public static final String CBO_DERIVE_JOIN_IS_NULL_PREDICATE = "cbo_derive_join_is_null_predicate";

    public static final String CBO_DECIMAL_CAST_STRING_STRICT = "cbo_decimal_cast_string_strict";

    public static final String CBO_EQ_BASE_TYPE = "cbo_eq_base_type";

    public static final String ENABLE_SHORT_CIRCUIT = "enable_short_circuit";

    // whether rewrite bitmap_union(to_bitmap(x)) to bitmap_agg(x) directly.
    public static final String ENABLE_REWRITE_BITMAP_UNION_TO_BITMAP_AGG = "enable_rewrite_bitmap_union_to_bitamp_agg";

    public static final List<String> DEPRECATED_VARIABLES = ImmutableList.<String>builder()
            .add(CODEGEN_LEVEL)
            .add(MAX_EXECUTION_TIME)
            .add(PROFILING)
            .add(BATCH_SIZE)
            .add(DISABLE_BUCKET_JOIN)
            .add(CBO_ENABLE_REPLICATED_JOIN)
            .add(FOREIGN_KEY_CHECKS)
            .add("enable_cbo")
            .add("enable_vectorized_engine")
            .add("vectorized_engine_enable")
            .add("enable_vectorized_insert")
            .add("vectorized_insert_enable")
            .add("prefer_join_method")
            .add("rewrite_count_distinct_to_bitmap_hll")
            .build();

    // Limitations
    // mem limit can't smaller than bufferpool's default page size
    public static final long MIN_EXEC_MEM_LIMIT = 2097152;
    // query timeout cannot greater than one month
    public static final int MAX_QUERY_TIMEOUT = 259200;

    @VariableMgr.VarAttr(name = ENABLE_PIPELINE, alias = ENABLE_PIPELINE_ENGINE, show = ENABLE_PIPELINE_ENGINE)
    private boolean enablePipelineEngine = true;

    /**
     * The threshold for determining whether to use a more evenly assignment bucket sequences to backend algorithm for query
     * execution.
     *
     * <p> This algorithm is only used when {@code numBucketsPerBe} is smaller than this threshold, because the time complexity
     * of it is {@code numBucketsPerBe} times than the previous algorithm.
     *
     * @see ColocatedBackendSelector
     */
    @VariableMgr.VarAttr(name = MAX_BUCKETS_PER_BE_TO_USE_BALANCER_ASSIGNMENT, flag = VariableMgr.INVISIBLE)
    private int maxBucketsPerBeToUseBalancerAssignment = 6;

    @VariableMgr.VarAttr(name = ENABLE_RUNTIME_ADAPTIVE_DOP)
    private boolean enableRuntimeAdaptiveDop = false;

    @VariableMgr.VarAttr(name = ADAPTIVE_DOP_MAX_BLOCK_ROWS_PER_DRIVER_SEQ, flag = VariableMgr.INVISIBLE)
    private long adaptiveDopMaxBlockRowsPerDriverSeq = 4096L * 4;

    // Effective when it is positive.
    @VariableMgr.VarAttr(name = ADAPTIVE_DOP_MAX_OUTPUT_AMPLIFICATION_FACTOR, flag = VariableMgr.INVISIBLE)
    private long adaptiveDopMaxOutputAmplificationFactor = 0;

    @VarAttr(name = ENABLE_MV_PLANNER)
    private boolean enableMVPlanner = false;
    @VarAttr(name = ENABLE_INCREMENTAL_REFRESH_MV)
    private boolean enableIncrementalRefreshMV = false;

    @VariableMgr.VarAttr(name = ENABLE_LOCAL_SHUFFLE_AGG)
    private boolean enableLocalShuffleAgg = true;

    @VariableMgr.VarAttr(name = USE_COMPUTE_NODES)
    private int useComputeNodes = -1;

    @VariableMgr.VarAttr(name = PREFER_COMPUTE_NODE)
    private boolean preferComputeNode = false;

    @VariableMgr.VarAttr(name = LOG_REJECTED_RECORD_NUM)
    private long logRejectedRecordNum = 0;

    /**
     * Determines whether to enable query tablet affinity. When enabled, attempts to schedule
     * fragments that access the same tablet to run on the same node to improve cache hit.
     */
    @VariableMgr.VarAttr(name = ENABLE_QUERY_TABLET_AFFINITY)
    private boolean enableQueryTabletAffinity = false;

    @VariableMgr.VarAttr(name = RUNTIME_FILTER_SCAN_WAIT_TIME, flag = VariableMgr.INVISIBLE)
    private long runtimeFilterScanWaitTime = 20L;

    @VariableMgr.VarAttr(name = RUNTIME_FILTER_ON_EXCHANGE_NODE)
    private boolean runtimeFilterOnExchangeNode = false;

    @VariableMgr.VarAttr(name = ENABLE_MULTI_COLUMNS_ON_GLOBAL_RUNTIME_FILTER)
    private boolean enableMultiColumnsOnGlobalRuntimeFilter = false;

    @VariableMgr.VarAttr(name = ENABLE_TABLET_INTERNAL_PARALLEL_V2,
            alias = ENABLE_TABLET_INTERNAL_PARALLEL, show = ENABLE_TABLET_INTERNAL_PARALLEL)
    private boolean enableTabletInternalParallel = true;

    // The strategy mode of TabletInternalParallel, which is effective only when enableTabletInternalParallel is true.
    // The optional values are "auto" and "force_split".
    @VariableMgr.VarAttr(name = TABLET_INTERNAL_PARALLEL_MODE, flag = VariableMgr.INVISIBLE)
    private String tabletInternalParallelMode = "auto";

    @VariableMgr.VarAttr(name = ENABLE_SHARED_SCAN)
    private boolean enableSharedScan = false;

    // max memory used on each fragment instance
    // NOTE: only used for non-pipeline engine and stream_load
    // The pipeline engine uses the query_mem_limit
    public static final long DEFAULT_EXEC_MEM_LIMIT = 2147483648L;
    @VariableMgr.VarAttr(name = EXEC_MEM_LIMIT, flag = VariableMgr.INVISIBLE)
    public long maxExecMemByte = DEFAULT_EXEC_MEM_LIMIT;

    @VariableMgr.VarAttr(name = LOAD_MEM_LIMIT)
    private long loadMemLimit = 0L;

    @VariableMgr.VarAttr(name = QUERY_MEM_LIMIT)
    private long queryMemLimit = 0L;

    // query timeout in second.
    @VariableMgr.VarAttr(name = QUERY_TIMEOUT)
    private int queryTimeoutS = 300;

    @VariableMgr.VarAttr(name = USE_PAGE_CACHE)
    private boolean usePageCache = true;

    // Execution of a query contains two phase.
    // 1. Deliver all the fragment instances to BEs.
    // 2. Pull data from BEs, after all the fragments are prepared and ready to execute in BEs.
    // queryDeliveryTimeoutS is the timeout of the first phase.
    @VariableMgr.VarAttr(name = QUERY_DELIVERY_TIMEOUT)
    private int queryDeliveryTimeoutS = 300;

    // if true, need report to coordinator when plan fragment execute successfully.
    @VariableMgr.VarAttr(name = ENABLE_PROFILE, alias = IS_REPORT_SUCCESS)
    private boolean enableProfile = false;

    // if true, will generate profile when load finished
    @VariableMgr.VarAttr(name = ENABLE_LOAD_PROFILE)
    private boolean enableLoadProfile = false;

    // Default sqlMode is ONLY_FULL_GROUP_BY
    @VariableMgr.VarAttr(name = SQL_MODE_STORAGE_NAME, alias = SQL_MODE, show = SQL_MODE)
    private long sqlMode = 32L;

    // The specified resource group of this session
    @VariableMgr.VarAttr(name = RESOURCE_GROUP, flag = VariableMgr.SESSION_ONLY)
    private String resourceGroup = "";

    // this is used to make mysql client happy
    @VariableMgr.VarAttr(name = AUTO_COMMIT)
    private boolean autoCommit = true;

    // this is used to make c3p0 library happy
    @VariableMgr.VarAttr(name = TX_ISOLATION)
    private String txIsolation = "REPEATABLE-READ";

    // this is used to compatible mysql 5.8
    @VariableMgr.VarAttr(name = TRANSACTION_ISOLATION)
    private String transactionIsolation = "REPEATABLE-READ";
    @VariableMgr.VarAttr(name = TRANSACTION_READ_ONLY, alias = TX_READ_ONLY)
    private String transactionReadOnly = "OFF";
    // this is used to make c3p0 library happy
    @VariableMgr.VarAttr(name = CHARACTER_SET_CLIENT)
    private String charsetClient = "utf8";
    @VariableMgr.VarAttr(name = CHARACTER_SET_CONNNECTION)
    private String charsetConnection = "utf8";
    @VariableMgr.VarAttr(name = CHARACTER_SET_RESULTS)
    private String charsetResults = "utf8";
    @VariableMgr.VarAttr(name = CHARACTER_SET_SERVER)
    private String charsetServer = "utf8";
    @VariableMgr.VarAttr(name = COLLATION_CONNECTION)
    private String collationConnection = "utf8_general_ci";
    @VariableMgr.VarAttr(name = COLLATION_DATABASE)
    private String collationDatabase = "utf8_general_ci";
    @VariableMgr.VarAttr(name = COLLATION_SERVER)
    private String collationServer = "utf8_general_ci";

    // this is used to make c3p0 library happy
    @VariableMgr.VarAttr(name = SQL_AUTO_IS_NULL)
    private boolean sqlAutoIsNull = false;

    public static final long DEFAULT_SELECT_LIMIT = 9223372036854775807L;
    @VariableMgr.VarAttr(name = SQL_SELECT_LIMIT)
    private long sqlSelectLimit = DEFAULT_SELECT_LIMIT;

    // this is used to make c3p0 library happy
    // Max packet length to send to or receive from the server,
    // try to set it to a higher value if `PacketTooBigException` is thrown at client
    @VariableMgr.VarAttr(name = MAX_ALLOWED_PACKET)
    private int maxAllowedPacket = 33554432; // 32MB
    @VariableMgr.VarAttr(name = AUTO_INCREMENT_INCREMENT)
    private int autoIncrementIncrement = 1;

    // this is used to make c3p0 library happy
    @VariableMgr.VarAttr(name = QUERY_CACHE_TYPE)
    private int queryCacheType = 0;

    // The number of seconds the server waits for activity on an interactive connection before closing it
    @VariableMgr.VarAttr(name = INTERACTIVE_TIMTOUT)
    private int interactiveTimeout = 3600;

    // The number of seconds the server waits for activity on a noninteractive connection before closing it.
    @VariableMgr.VarAttr(name = WAIT_TIMEOUT)
    private int waitTimeout = 28800;

    // The number of seconds to wait for a block to be written to a connection before aborting the write
    @VariableMgr.VarAttr(name = NET_WRITE_TIMEOUT)
    private int netWriteTimeout = 60;

    // The number of seconds to wait for a block to be written to a connection before aborting the write
    @VariableMgr.VarAttr(name = NET_READ_TIMEOUT)
    private int netReadTimeout = 60;

    // The current time zone
    @VariableMgr.VarAttr(name = TIME_ZONE)
    private String timeZone = TimeUtils.getSystemTimeZone().getID();

    @VariableMgr.VarAttr(name = INNODB_READ_ONLY)
    private boolean innodbReadOnly = true;

    @VariableMgr.VarAttr(name = PARALLEL_EXCHANGE_INSTANCE_NUM)
    private int exchangeInstanceParallel = -1;

    @VariableMgr.VarAttr(name = SQL_SAFE_UPDATES)
    private int sqlSafeUpdates = 0;

    // only
    @VariableMgr.VarAttr(name = NET_BUFFER_LENGTH, flag = VariableMgr.READ_ONLY)
    private int netBufferLength = 16384;

    @VariableMgr.VarAttr(name = CHUNK_SIZE, flag = VariableMgr.INVISIBLE)
    private int chunkSize = 4096;

    public static final int PIPELINE_BATCH_SIZE = 4096;

    // auto, force_streaming, force_preaggregation
    @VariableMgr.VarAttr(name = STREAMING_PREAGGREGATION_MODE)
    private String streamingPreaggregationMode = SessionVariableConstants.AUTO;

    @VariableMgr.VarAttr(name = DISABLE_COLOCATE_JOIN)
    private boolean disableColocateJoin = false;

    @VariableMgr.VarAttr(name = CBO_USE_CORRELATED_JOIN_ESTIMATE, flag = VariableMgr.INVISIBLE)
    private boolean useCorrelatedJoinEstimate = true;

    @VariableMgr.VarAttr(name = CBO_USE_NTH_EXEC_PLAN, flag = VariableMgr.INVISIBLE)
    private int useNthExecPlan = 0;

    @VarAttr(name = CBO_CTE_REUSE)
    private boolean cboCteReuse = true;

    @VarAttr(name = CBO_CTE_REUSE_RATE_V2, flag = VariableMgr.INVISIBLE, alias = CBO_CTE_REUSE_RATE,
            show = CBO_CTE_REUSE_RATE)
    private double cboCTERuseRatio = 1.15;

    @VarAttr(name = CBO_CTE_MAX_LIMIT, flag = VariableMgr.INVISIBLE)
    private int cboCTEMaxLimit = 10;

    @VarAttr(name = PREFER_CTE_REWRITE, flag = VariableMgr.INVISIBLE)
    private boolean preferCTERewrite = false;

    @VarAttr(name = CBO_PRUNE_SUBFIELD, flag = VariableMgr.INVISIBLE)
    private boolean cboPruneSubfield = true;

    @VarAttr(name = ENABLE_SQL_DIGEST, flag = VariableMgr.INVISIBLE)
    private boolean enableSQLDigest = false;

    @VarAttr(name = CBO_USE_DB_LOCK, flag = VariableMgr.INVISIBLE)
    private boolean cboUseDBLock = false;

    /*
     * the parallel exec instance num for one Fragment in one BE
     * 1 means disable this feature
     */
    @VariableMgr.VarAttr(name = PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM)
    private int parallelExecInstanceNum = 1;

    @VariableMgr.VarAttr(name = MAX_PARALLEL_SCAN_INSTANCE_NUM)
    private int maxParallelScanInstanceNum = -1;

    @VariableMgr.VarAttr(name = PIPELINE_DOP)
    private int pipelineDop = 0;

    @VariableMgr.VarAttr(name = PIPELINE_SINK_DOP)
    private int pipelineSinkDop = 0;

    /*
     * The maximum pipeline dop limit which only takes effect when pipeline_dop=0.
     * This limitation is to avoid the negative overhead caused by scheduling on super multi-core scenarios.
     */
    @VariableMgr.VarAttr(name = MAX_PIPELINE_DOP)
    private int maxPipelineDop = 64;

    @VariableMgr.VarAttr(name = PROFILE_TIMEOUT, flag = VariableMgr.INVISIBLE)
    private int profileTimeout = 2;

    @VariableMgr.VarAttr(name = RUNTIME_PROFILE_REPORT_INTERVAL)
    private int runtimeProfileReportInterval = 10;

    @VariableMgr.VarAttr(name = PIPELINE_PROFILE_LEVEL)
    private int pipelineProfileLevel = 1;

    @VariableMgr.VarAttr(name = ENABLE_ASYNC_PROFILE, flag = VariableMgr.INVISIBLE)
    private boolean enableAsyncProfile = true;

    @VariableMgr.VarAttr(name = BIG_QUERY_PROFILE_SECOND_THRESHOLD)
    private int bigQueryProfileSecondThreshold = 0;

    @VariableMgr.VarAttr(name = RESOURCE_GROUP_ID, alias = RESOURCE_GROUP_ID_V2,
            show = RESOURCE_GROUP_ID_V2, flag = VariableMgr.INVISIBLE)
    private int resourceGroupId = 0;

    @VariableMgr.VarAttr(name = ENABLE_INSERT_STRICT)
    private boolean enableInsertStrict = true;

    @VariableMgr.VarAttr(name = ENABLE_SPILL)
    private boolean enableSpill = false;

    // this is used to control which operators can spill, only meaningful when enable_spill=true
    // it uses a bit to identify whether the spill of each operator is in effect, 0 means no, 1 means yes
    // at present, only the lowest 4 bits are meaningful, corresponding to the four operators
    // HASH_JOIN, AGG, AGG_DISTINCT and SORT respectively (see TSpillableOperatorType in InternalService.thrift)
    // e.g.
    // if spillable_operator_mask & 1 != 0, hash join operator can spill
    // if spillable_operator_mask & 2 != 0, agg operator can spill
    // if spillable_operator_mask & 4 != 0, agg distinct operator can spill
    // if spillable_operator_mask & 8 != 0, sort operator can spill
    // if spillable_operator_mask & 16 != 0, nest loop join operator can spill
    // ...
    // default value is -1, means all operators can spill
    @VariableMgr.VarAttr(name = SPILLABLE_OPERATOR_MASK, flag = VariableMgr.INVISIBLE)
    private long spillableOperatorMask = -1;

    @VariableMgr.VarAttr(name = SPILL_MODE)
    private String spillMode = "auto";

    // These parameters are experimental. They may be removed in the future
    @VarAttr(name = SPILL_MEM_TABLE_SIZE, flag = VariableMgr.INVISIBLE)
    private int spillMemTableSize = 1024 * 1024 * 100;
    @VarAttr(name = SPILL_MEM_TABLE_NUM, flag = VariableMgr.INVISIBLE)
    private int spillMemTableNum = 2;
    @VarAttr(name = SPILL_MEM_LIMIT_THRESHOLD, flag = VariableMgr.INVISIBLE)
    private double spillMemLimitThreshold = 0.8;
    @VarAttr(name = SPILL_OPERATOR_MIN_BYTES, flag = VariableMgr.INVISIBLE)
    private long spillOperatorMinBytes = 1024L * 1024 * 50;
    @VarAttr(name = SPILL_OPERATOR_MAX_BYTES, flag = VariableMgr.INVISIBLE)
    private long spillOperatorMaxBytes = 1024L * 1024 * 1000;
    // If the operator memory revocable memory exceeds this value, the operator will perform a spill as soon as possible
    @VarAttr(name = SPILL_REVOCABLE_MAX_BYTES)
    private long spillRevocableMaxBytes = 0;
    // the encoding level of spilled data, the meaning of values is similar to transmission_encode_level,
    // see more details in the comment above transmissionEncodeLevel
    @VarAttr(name = SPILL_ENCODE_LEVEL)
    private int spillEncodeLevel = 7;

    @VarAttr(name = ENABLE_RBO_TABLE_PRUNE)
    private boolean enableRboTablePrune = false;

    @VarAttr(name = ENABLE_CBO_TABLE_PRUNE)
    private boolean enableCboTablePrune = false;
    @VariableMgr.VarAttr(name = FORWARD_TO_LEADER, alias = FORWARD_TO_MASTER)
    private boolean forwardToLeader = false;

    // compatible with some mysql client connect, say DataGrip of JetBrains
    @VariableMgr.VarAttr(name = EVENT_SCHEDULER)
    private String eventScheduler = "OFF";
    @VariableMgr.VarAttr(name = STORAGE_ENGINE)
    private String storageEngine = "olap";
    @VariableMgr.VarAttr(name = DIV_PRECISION_INCREMENT)
    private int divPrecisionIncrement = 4;

    // -1 means unset, BE will use its config value
    @VariableMgr.VarAttr(name = MAX_SCAN_KEY_NUM)
    private int maxScanKeyNum = -1;
    @VariableMgr.VarAttr(name = MAX_PUSHDOWN_CONDITIONS_PER_COLUMN)
    private int maxPushdownConditionsPerColumn = -1;

    @VariableMgr.VarAttr(name = HASH_JOIN_PUSH_DOWN_RIGHT_TABLE)
    private boolean hashJoinPushDownRightTable = true;

    @VariableMgr.VarAttr(name = DISABLE_JOIN_REORDER)
    private boolean disableJoinReorder = false;

    @VariableMgr.VarAttr(name = ENABLE_PREDICATE_REORDER)
    private boolean enablePredicateReorder = false;

    @VariableMgr.VarAttr(name = ENABLE_FILTER_UNUSED_COLUMNS_IN_SCAN_STAGE)
    private boolean enableFilterUnusedColumnsInScanStage = true;

    @VariableMgr.VarAttr(name = CBO_MAX_REORDER_NODE_USE_EXHAUSTIVE)
    private int cboMaxReorderNodeUseExhaustive = 4;

    @VariableMgr.VarAttr(name = CBO_MAX_REORDER_NODE, flag = VariableMgr.INVISIBLE)
    private int cboMaxReorderNode = 50;

    @VariableMgr.VarAttr(name = CBO_ENABLE_DP_JOIN_REORDER, flag = VariableMgr.INVISIBLE)
    private boolean cboEnableDPJoinReorder = true;

    @VariableMgr.VarAttr(name = CBO_MAX_REORDER_NODE_USE_DP)
    private long cboMaxReorderNodeUseDP = 10;

    @VariableMgr.VarAttr(name = CBO_ENABLE_GREEDY_JOIN_REORDER, flag = VariableMgr.INVISIBLE)
    private boolean cboEnableGreedyJoinReorder = true;

    @VariableMgr.VarAttr(name = CBO_DEBUG_ALIVE_BACKEND_NUMBER, flag = VariableMgr.INVISIBLE)
    private int cboDebugAliveBackendNumber = 0;

    @VariableMgr.VarAttr(name = TRANSACTION_VISIBLE_WAIT_TIMEOUT)
    private long transactionVisibleWaitTimeout = 10;

    @VariableMgr.VarAttr(name = FORCE_SCHEDULE_LOCAL)
    private boolean forceScheduleLocal = false;

    @VariableMgr.VarAttr(name = BROADCAST_ROW_LIMIT)
    private long broadcastRowCountLimit = 15000000;

    @VariableMgr.VarAttr(name = BROADCAST_RIGHT_TABLE_SCALE_FACTOR, flag = VariableMgr.INVISIBLE)
    private double broadcastRightTableScaleFactor = 10.0;

    @VariableMgr.VarAttr(name = NEW_PLANNER_OPTIMIZER_TIMEOUT)
    private long optimizerExecuteTimeout = 3000;

    @VariableMgr.VarAttr(name = QUERY_DEBUG_OPTIONS, flag = VariableMgr.INVISIBLE)
    private String queryDebugOptions = "";

    @VariableMgr.VarAttr(name = OPTIMIZER_MATERIALIZED_VIEW_TIMELIMIT)
    private long optimizerMaterializedViewTimeLimitMillis = 1000;

    @VariableMgr.VarAttr(name = ENABLE_QUERY_DUMP)
    private boolean enableQueryDump = false;

    @VariableMgr.VarAttr(name = CBO_ENABLE_LOW_CARDINALITY_OPTIMIZE)
    private boolean enableLowCardinalityOptimize = true;

    @VariableMgr.VarAttr(name = ENABLE_OPTIMIZER_REWRITE_GROUPINGSETS_TO_UNION_ALL)
    private boolean enableRewriteGroupingSetsToUnionAll = false;

    // value should be 0~4
    // 0 represents automatic selection, and 1, 2, 3, and 4 represent forced selection of AGG of
    // corresponding stages respectively. However, stages 3 and 4 can only be generated in
    // single-column distinct scenarios
    @VariableMgr.VarAttr(name = NEW_PLANER_AGG_STAGE)
    private int newPlannerAggStage = 0;

    @VariableMgr.VarAttr(name = TRANSMISSION_COMPRESSION_TYPE)
    private String transmissionCompressionType = "NO_COMPRESSION";

    // if a packet's size is larger than RPC_HTTP_MIN_SIZE, it will use RPC via http, as the std rpc has 2GB size limit.
    // the setting size is a bit smaller than 2GB, as the pre-computed serialization size of packets may not accurate.
    // no need to change it in general.
    @VariableMgr.VarAttr(name = RPC_HTTP_MIN_SIZE, flag = VariableMgr.INVISIBLE)
    private long rpcHttpMinSize = ((1L << 31) - (1L << 10));

    // encode integers/binary per column for exchange, controlled by transmission_encode_level
    // if transmission_encode_level & 2, intergers are encode by streamvbyte, in order or not;
    // if transmission_encode_level & 4, binary columns are compressed by lz4
    // if transmission_encode_level & 1, enable adaptive encoding.
    // e.g.
    // if transmission_encode_level = 7, SR will adaptively encode numbers and string columns according to the proper encoding
    // ratio(< 0.9);
    // if transmission_encode_level = 6, SR will force encoding numbers and string columns.
    // in short,
    // for transmission_encode_level,
    // 2 for encoding integers or types supported by integers,
    // 4 for encoding string,
    // json and object columns are left to be supported later.
    @VariableMgr.VarAttr(name = TRANSMISSION_ENCODE_LEVEL)
    private int transmissionEncodeLevel = 7;

    @VariableMgr.VarAttr(name = LOAD_TRANSMISSION_COMPRESSION_TYPE)
    private String loadTransmissionCompressionType = "NO_COMPRESSION";

    @VariableMgr.VarAttr(name = RUNTIME_JOIN_FILTER_PUSH_DOWN_LIMIT)
    private long runtimeJoinFilterPushDownLimit = 1024000;

    @VariableMgr.VarAttr(name = ENABLE_GLOBAL_RUNTIME_FILTER)
    private boolean enableGlobalRuntimeFilter = true;

    @VariableMgr.VarAttr(name = ENABLE_TOPN_RUNTIME_FILTER)
    private boolean enableTopNRuntimeFilter = true;

    // Parameters to determine the usage of runtime filter
    // Either the build_max or probe_min equal to 0 would force use the filter,
    // otherwise would decide based on the cardinality
    @VariableMgr.VarAttr(name = GLOBAL_RUNTIME_FILTER_BUILD_MAX_SIZE, flag = VariableMgr.INVISIBLE)
    private long globalRuntimeFilterBuildMaxSize = 64L * 1024L * 1024L;

    @VariableMgr.VarAttr(name = GLOBAL_RUNTIME_FILTER_BUILD_MIN_SIZE, flag = VariableMgr.INVISIBLE)
    private long globalRuntimeFilterBuildMinSize = 128L * 1024L;
    @VariableMgr.VarAttr(name = GLOBAL_RUNTIME_FILTER_PROBE_MIN_SIZE, flag = VariableMgr.INVISIBLE)
    private long globalRuntimeFilterProbeMinSize = 100L * 1024L;
    @VariableMgr.VarAttr(name = GLOBAL_RUNTIME_FILTER_PROBE_MIN_SELECTIVITY, flag = VariableMgr.INVISIBLE)
    private float globalRuntimeFilterProbeMinSelectivity = 0.5f;
    @VariableMgr.VarAttr(name = GLOBAL_RUNTIME_FILTER_WAIT_TIMEOUT, flag = VariableMgr.INVISIBLE)
    private int globalRuntimeFilterWaitTimeout = 20;
    @VariableMgr.VarAttr(name = GLOBAL_RUNTIME_FILTER_RPC_TIMEOUT, flag = VariableMgr.INVISIBLE)
    private int globalRuntimeFilterRpcTimeout = 400;
    @VariableMgr.VarAttr(name = RUNTIME_FILTER_EARLY_RETURN_SELECTIVITY, flag = VariableMgr.INVISIBLE)
    private float runtimeFilterEarlyReturnSelectivity = 0.05f;
    @VariableMgr.VarAttr(name = GLOBAL_RUNTIME_FILTER_RPC_HTTP_MIN_SIZE, flag = VariableMgr.INVISIBLE)
    private long globalRuntimeFilterRpcHttpMinSize = 64L * 1024 * 1024;

    //In order to be compatible with the logic of the old planner,
    //When the column name is the same as the alias name,
    //the alias will be used as the groupby column if set to true.
    @VariableMgr.VarAttr(name = ENABLE_GROUPBY_USE_OUTPUT_ALIAS)
    private boolean enableGroupbyUseOutputAlias = false;

    @VariableMgr.VarAttr(name = ENABLE_COLUMN_EXPR_PREDICATE, flag = VariableMgr.INVISIBLE)
    private boolean enableColumnExprPredicate = true;

    @VariableMgr.VarAttr(name = ENABLE_EXCHANGE_PASS_THROUGH, flag = VariableMgr.INVISIBLE)
    private boolean enableExchangePassThrough = true;

    @VariableMgr.VarAttr(name = ENABLE_EXCHANGE_PERF, flag = VariableMgr.INVISIBLE)
    private boolean enableExchangePerf = false;

    @VariableMgr.VarAttr(name = ALLOW_DEFAULT_PARTITION, flag = VariableMgr.INVISIBLE)
    private boolean allowDefaultPartition = false;

    @VariableMgr.VarAttr(name = SINGLE_NODE_EXEC_PLAN, flag = VariableMgr.INVISIBLE)
    private boolean singleNodeExecPlan = false;

    @VariableMgr.VarAttr(name = ENABLE_HIVE_COLUMN_STATS)
    private boolean enableHiveColumnStats = true;

    @VariableMgr.VarAttr(name = ENABLE_WRITE_HIVE_EXTERNAL_TABLE)
    private boolean enableWriteHiveExternalTable = false;

    @VariableMgr.VarAttr(name = ENABLE_HIVE_METADATA_CACHE_WITH_INSERT)
    private boolean enableHiveMetadataCacheWithInsert = false;

    @VariableMgr.VarAttr(name = HIVE_PARTITION_STATS_SAMPLE_SIZE)
    private int hivePartitionStatsSampleSize = 3000;

    @VarAttr(name = DEFAULT_TABLE_COMPRESSION)
    private String defaultTableCompressionAlgorithm = "lz4_frame";

    @VariableMgr.VarAttr(name = ENABLE_ADAPTIVE_SINK_DOP)
    private boolean enableAdaptiveSinkDop = false;

    @VariableMgr.VarAttr(name = JOIN_IMPLEMENTATION_MODE_V2, alias = JOIN_IMPLEMENTATION_MODE)
    private String joinImplementationMode = "auto"; // auto, merge, hash, nestloop

    @VariableMgr.VarAttr(name = ENABLE_QUERY_DEBUG_TRACE, flag = VariableMgr.INVISIBLE)
    private boolean enableQueryDebugTrace = false;

    // command, file
    @VarAttr(name = TRACE_LOG_MODE, flag = VariableMgr.INVISIBLE)
    private String traceLogMode = "command";

    @VariableMgr.VarAttr(name = INTERPOLATE_PASSTHROUGH, flag = VariableMgr.INVISIBLE)
    private boolean interpolatePassthrough = true;

    @VariableMgr.VarAttr(name = HASH_JOIN_INTERPOLATE_PASSTHROUGH, flag = VariableMgr.INVISIBLE)
    private boolean hashJoinInterpolatePassthrough = false;

    @VarAttr(name = STATISTIC_COLLECT_PARALLEL)
    private int statisticCollectParallelism = 1;

    @VarAttr(name = ENABLE_SHOW_ALL_VARIABLES, flag = VariableMgr.INVISIBLE)
    private boolean enableShowAllVariables = false;

    @VarAttr(name = CBO_PRUNE_SHUFFLE_COLUMN_RATE, flag = VariableMgr.INVISIBLE)
    private double cboPruneShuffleColumnRate = 0.1;

    // 0: auto, 1: force push down, -1: don't push down, 2: push down medium, 3: push down high
    @VarAttr(name = "cboPushDownAggregateMode_v1", alias = CBO_PUSH_DOWN_AGGREGATE_MODE,
            show = CBO_PUSH_DOWN_AGGREGATE_MODE, flag = VariableMgr.INVISIBLE)
    private int cboPushDownAggregateMode = -1;

    // auto, global, local
    @VarAttr(name = CBO_PUSH_DOWN_AGGREGATE, flag = VariableMgr.INVISIBLE)
    private String cboPushDownAggregate = "global";

    @VariableMgr.VarAttr(name = PARSE_TOKENS_LIMIT)
    private int parseTokensLimit = 3500000;

    @VarAttr(name = ENABLE_SORT_AGGREGATE)
    private boolean enableSortAggregate = false;

    @VarAttr(name = ENABLE_PER_BUCKET_OPTIMIZE)
    private boolean enablePerBucketComputeOptimize = true;

    @VarAttr(name = ENABLE_PARALLEL_MERGE)
    private boolean enableParallelMerge = true;

    @VarAttr(name = ENABLE_QUERY_QUEUE, flag = VariableMgr.INVISIBLE)
    private boolean enableQueryQueue = true;

    // 1: sort based, 2: hash based
    @VarAttr(name = WINDOW_PARTITION_MODE, flag = VariableMgr.INVISIBLE)
    private int windowPartitionMode = 1;

    @VarAttr(name = ENABLE_REWRITE_SUM_BY_ASSOCIATIVE_RULE)
    private boolean enableRewriteSumByAssociativeRule = true;

    @VarAttr(name = ENABLE_REWRITE_SIMPLE_AGG_TO_META_SCAN)
    private boolean enableRewriteSimpleAggToMetaScan = false;

    @VariableMgr.VarAttr(name = INTERLEAVING_GROUP_SIZE)
    private int interleavingGroupSize = 10;

    // support auto|row|column
    @VariableMgr.VarAttr(name = PARTIAL_UPDATE_MODE)
    private String partialUpdateMode = "auto";

    @VariableMgr.VarAttr(name = HDFS_BACKEND_SELECTOR_HASH_ALGORITHM, flag = VariableMgr.INVISIBLE)
    private String hdfsBackendSelectorHashAlgorithm = "consistent";

    @VariableMgr.VarAttr(name = CONSISTENT_HASH_VIRTUAL_NUMBER, flag = VariableMgr.INVISIBLE)
    private int consistentHashVirtualNodeNum = 128;

    // binary, json, compact,
    @VarAttr(name = THRIFT_PLAN_PROTOCOL)
    private String thriftPlanProtocol = "binary";

    @VarAttr(name = CBO_PUSHDOWN_TOPN_LIMIT)
    private long cboPushDownTopNLimit = 1000;

    @VarAttr(name = ENABLE_REWRITE_BITMAP_UNION_TO_BITMAP_AGG)
    private boolean enableRewriteBitmapUnionToBitmapAgg = true;

    public boolean isEnableRewriteBitmapUnionToBitmapAgg() {
        return enableRewriteBitmapUnionToBitmapAgg;
    }

    public void setEnableRewriteBitmapUnionToBitmapAgg(boolean enableRewriteBitmapUnionToBitmapAgg) {
        this.enableRewriteBitmapUnionToBitmapAgg = enableRewriteBitmapUnionToBitmapAgg;
    }
    public long getCboPushDownTopNLimit() {
        return cboPushDownTopNLimit;
    }

    public void setCboPushDownTopNLimit(long cboPushDownTopNLimit) {
        this.cboPushDownTopNLimit = cboPushDownTopNLimit;
    }

    public String getThriftPlanProtocol() {
        return thriftPlanProtocol;
    }

    public void setTraceLogMode(String traceLogMode) {
        this.traceLogMode = traceLogMode;
    }

    public String getTraceLogMode() {
        return traceLogMode;
    }

    public void setPartialUpdateMode(String mode) {
        this.partialUpdateMode = mode;
    }

    public String getPartialUpdateMode() {
        return this.partialUpdateMode;
    }

    public boolean isEnableSortAggregate() {
        return enableSortAggregate;
    }

    public boolean isEnablePerBucketComputeOptimize() {
        return enablePerBucketComputeOptimize;
    }

    public int getWindowPartitionMode() {
        return windowPartitionMode;
    }

    public void setEnableSortAggregate(boolean enableSortAggregate) {
        this.enableSortAggregate = enableSortAggregate;
    }

    public boolean isEnableParallelMerge() {
        return enableParallelMerge;
    }

    public void setEnableParallelMerge(boolean enableParallelMerge) {
        this.enableParallelMerge = enableParallelMerge;
    }

    public boolean isEnableQueryQueue() {
        return enableQueryQueue;
    }

    @VariableMgr.VarAttr(name = ENABLE_SCAN_DATACACHE, alias = ENABLE_SCAN_BLOCK_CACHE)
    private boolean enableScanDataCache = false;

    @VariableMgr.VarAttr(name = ENABLE_POPULATE_DATACACHE, alias = ENABLE_POPULATE_BLOCK_CACHE)
    private boolean enablePopulateDataCache = true;

    @VariableMgr.VarAttr(name = IO_TASKS_PER_SCAN_OPERATOR)
    private int ioTasksPerScanOperator = 4;

    @VariableMgr.VarAttr(name = CONNECTOR_IO_TASKS_PER_SCAN_OPERATOR)
    private int connectorIoTasksPerScanOperator = 16;

    @VariableMgr.VarAttr(name = ENABLE_CONNECTOR_ADAPTIVE_IO_TASKS)
    private boolean enableConnectorAdaptiveIoTasks = true;

    @VariableMgr.VarAttr(name = CONNECTOR_IO_TASKS_SLOW_IO_LATENCY_MS, flag = VariableMgr.INVISIBLE)
    private int connectorIoTasksSlowIoLatency = 50;

    @VariableMgr.VarAttr(name = SCAN_USE_QUERY_MEM_RATIO)
    private double scanUseQueryMemRatio = 0.3;

    @VariableMgr.VarAttr(name = CONNECTOR_SCAN_USE_QUERY_MEM_RATIO)
    private double connectorScanUseQueryMemRatio = 0.3;

    @VariableMgr.VarAttr(name = CONNECTOR_SINK_COMPRESSION_CODEC)
    private String connectorSinkCompressionCodec = "uncompressed";

    public String getConnectorSinkCompressionCodec() {
        return connectorSinkCompressionCodec;
    }

    @VariableMgr.VarAttr(name = ENABLE_FILE_METACACHE)
    private boolean enableFileMetaCache = false;

    @VariableMgr.VarAttr(name = HUDI_MOR_FORCE_JNI_READER)
    private boolean hudiMORForceJNIReader = false;

    @VariableMgr.VarAttr(name = PAIMON_FORCE_JNI_READER)
    private boolean paimonForceJNIReader = false;

    @VarAttr(name = ENABLE_QUERY_CACHE)
    private boolean enableQueryCache = false;

    @VarAttr(name = QUERY_CACHE_FORCE_POPULATE)
    private boolean queryCacheForcePopulate = false;

    @VarAttr(name = QUERY_CACHE_ENTRY_MAX_BYTES)
    private long queryCacheEntryMaxBytes = 4194304;

    @VarAttr(name = QUERY_CACHE_ENTRY_MAX_ROWS)
    private long queryCacheEntryMaxRows = 409600;

    @VarAttr(name = QUERY_CACHE_HOT_PARTITION_NUM)
    private int queryCacheHotPartitionNum = 3;

    @VarAttr(name = QUERY_CACHE_AGG_CARDINALITY_LIMIT)
    private long queryCacheAggCardinalityLimit = 5000000;

    @VarAttr(name = NESTED_MV_REWRITE_MAX_LEVEL)
    private int nestedMvRewriteMaxLevel = 3;

    @VarAttr(name = ENABLE_MATERIALIZED_VIEW_REWRITE)
    private boolean enableMaterializedViewRewrite = true;

    /**
     * Whether enable materialized-view rewrite for INSERT statement
     */
    @VarAttr(name = ENABLE_MATERIALIZED_VIEW_REWRITE_FOR_INSERT)
    private boolean enableMaterializedViewRewriteForInsert = false;

    @VarAttr(name = ENABLE_SYNC_MATERIALIZED_VIEW_REWRITE)
    private boolean enableSyncMaterializedViewRewrite = true;

    @VarAttr(name = ENABLE_MATERIALIZED_VIEW_UNION_REWRITE)
    private boolean enableMaterializedViewUnionRewrite = true;

    /**
     * Whether to compensate partition predicates in mv rewrite, see
     * <code>Materialization#isCompensatePartitionPredicate</code> for more details.
     * NOTE: if set it false, it will be rewritten by the mv defined sql with user's query and will not add
     * extra compensated predicates which can rewrite more cases but may lose consistency check.
     */
    @VarAttr(name = ENABLE_MATERIALIZED_VIEW_REWRITE_PARTITION_COMPENSATE, flag = VariableMgr.INVISIBLE)
    private boolean enableMaterializedViewRewritePartitionCompensate = true;

    @VarAttr(name = ENABLE_RULE_BASED_MATERIALIZED_VIEW_REWRITE)
    private boolean enableRuleBasedMaterializedViewRewrite = true;

    @VarAttr(name = ENABLE_MATERIALIZED_VIEW_VIEW_DELTA_REWRITE)
    private boolean enableMaterializedViewViewDeltaRewrite = true;

    @VarAttr(name = MATERIALIZED_VIEW_JOIN_SAME_TABLE_PERMUTATION_LIMIT, flag = VariableMgr.INVISIBLE)
    private int materializedViewJoinSameTablePermutationLimit = 5;

    @VarAttr(name = MATERIALIZED_VIEW_REWRITE_MODE)
    private String materializedViewRewriteMode = MaterializedViewRewriteMode.MODE_DEFAULT;

    //  Whether to enable view delta compensation for single table,
    //  - try to rewrite single table query into candidate view-delta mvs if enabled which will choose
    //      plan by cost.
    //  - otherwise not try to write single table query by using candidate view-delta mvs which only
    //      try to rewrite by single table mvs and is determined by rule rather than by cost.
    @VarAttr(name = ENABLE_MATERIALIZED_VIEW_SINGLE_TABLE_VIEW_DELTA_REWRITE, flag = VariableMgr.INVISIBLE)
    private boolean enableMaterializedViewSingleTableViewDeltaRewrite = false;

    // Enable greedy mode in mv rewrite to cut down optimizer time for mv rewrite:
    // - Use plan cache if possible to avoid regenerating plan tree.
    // - Use the max plan tree to rewrite in view-delta mode to avoid too many rewrites.
    @VarAttr(name = ENABLE_MATERIALIZED_VIEW_REWRITE_GREEDY_MODE)
    private boolean enableMaterializedViewRewriteGreedyMode = false;

    // whether to use materialized view plan context cache to reduce mv rewrite time cost
    @VarAttr(name = ENABLE_MATERIALIZED_VIEW_PLAN_CACHE, flag = VariableMgr.INVISIBLE)
    private boolean enableMaterializedViewPlanCache = true;

    @VarAttr(name = ENABLE_VIEW_BASED_MV_REWRITE)
    private boolean enableViewBasedMvRewrite = false;

    /**
     * Materialized view rewrite rule output limit: how many MVs would be chosen in a Rule for an OptExpr ?
     */
    @VarAttr(name = CBO_MATERIALIZED_VIEW_REWRITE_RULE_OUTPUT_LIMIT, flag = VariableMgr.INVISIBLE)
    private int cboMaterializedViewRewriteRuleOutputLimit = 3;

    /**
     * Materialized view rewrite candidate limit: how many MVs would be considered in a Rule for an OptExpr ?
     */
    @VarAttr(name = CBO_MATERIALIZED_VIEW_REWRITE_CANDIDATE_LIMIT, flag = VariableMgr.INVISIBLE)
    private int cboMaterializedViewRewriteCandidateLimit = 12;

    @VarAttr(name = QUERY_EXCLUDING_MV_NAMES, flag = VariableMgr.INVISIBLE)
    private String queryExcludingMVNames = "";

    @VarAttr(name = QUERY_INCLUDING_MV_NAMES, flag = VariableMgr.INVISIBLE)
    private String queryIncludingMVNames = "";

    @VarAttr(name = ANALYZE_FOR_MV)
    private String analyzeTypeForMV = "sample";

    // if enable_big_query_log = true and cpu/io cost of a query exceeds the related threshold,
    // the information will be written to the big query log
    @VarAttr(name = ENABLE_BIG_QUERY_LOG)
    private boolean enableBigQueryLog = true;
    // the value is set for testing,
    // if a query needs to perform 10s for computing tasks at full load on three 16-core machines,
    // we treat it as a big query, so set this value to 480(10 * 16 * 3).
    // Users need to set up according to their own scenario.
    @VarAttr(name = BIG_QUERY_LOG_CPU_SECOND_THRESHOLD)
    private long bigQueryLogCPUSecondThreshold = 480;
    // the value is set for testing, if a query needs to scan more than 10GB of data, we treat it as a big query.
    // Users need to set up according to their own scenario.
    @VarAttr(name = BIG_QUERY_LOG_SCAN_BYTES_THRESHOLD)
    private long bigQueryLogScanBytesThreshold = 1024L * 1024 * 1024 * 10;
    // the value is set for testing, if a query need to scan more than 1 billion rows of data,
    // we treat it as a big query.
    // Users need to set up according to their own scenario.
    @VarAttr(name = BIG_QUERY_LOG_SCAN_ROWS_THRESHOLD)
    private long bigQueryLogScanRowsThreshold = 1000000000L;

    @VarAttr(name = SQL_DIALECT)
    private String sqlDialect = "StarRocks";

    @VarAttr(name = ENABLE_OUTER_JOIN_REORDER)
    private boolean enableOuterJoinReorder = true;

    // This value is different from cboMaxReorderNodeUseExhaustive which only counts innerOrCross join node, while it
    // counts all types of join node including outer/semi/anti join.
    @VarAttr(name = CBO_REORDER_THRESHOLD_USE_EXHAUSTIVE)
    private int cboReorderThresholdUseExhaustive = 6;

    @VarAttr(name = ENABLE_PRUNE_COMPLEX_TYPES)
    private boolean enablePruneComplexTypes = true;

    @VarAttr(name = ENABLE_PRUNE_COMPLEX_TYPES_IN_UNNEST)
    private boolean enablePruneComplexTypesInUnnest = true;

    @VarAttr(name = RANGE_PRUNER_PREDICATES_MAX_LEN)
    public int rangePrunerPredicateMaxLen = 100;

    @VarAttr(name = SQL_QUOTE_SHOW_CREATE)
    private boolean quoteShowCreate = true; // Defined but unused now, for compatibility with MySQL

    @VariableMgr.VarAttr(name = GROUP_CONCAT_MAX_LEN)
    private long groupConcatMaxLen = 1024;

    @VariableMgr.VarAttr(name = FULL_SORT_MAX_BUFFERED_ROWS, flag = VariableMgr.INVISIBLE)
    private long fullSortMaxBufferedRows = 1024000;

    @VariableMgr.VarAttr(name = FULL_SORT_MAX_BUFFERED_BYTES, flag = VariableMgr.INVISIBLE)
    private long fullSortMaxBufferedBytes = 16L * 1024 * 1024;

    @VariableMgr.VarAttr(name = FULL_SORT_LATE_MATERIALIZATION_V2, alias = FULL_SORT_LATE_MATERIALIZATION,
            show = FULL_SORT_LATE_MATERIALIZATION)
    private boolean fullSortLateMaterialization = true;

    @VariableMgr.VarAttr(name = DISTINCT_COLUMN_BUCKETS)
    private int distinctColumnBuckets = 1024;

    @VariableMgr.VarAttr(name = ENABLE_DISTINCT_COLUMN_BUCKETIZATION)
    private boolean enableDistinctColumnBucketization = false;

    @VariableMgr.VarAttr(name = HDFS_BACKEND_SELECTOR_SCAN_RANGE_SHUFFLE, flag = VariableMgr.INVISIBLE)
    private boolean hdfsBackendSelectorScanRangeShuffle = false;

    @VariableMgr.VarAttr(name = CBO_PUSH_DOWN_DISTINCT_BELOW_WINDOW)
    private boolean cboPushDownDistinctBelowWindow = true;

    @VarAttr(name = ENABLE_PLAN_VALIDATION, flag = VariableMgr.INVISIBLE)
    private boolean enablePlanValidation = true;

    @VarAttr(name = SCAN_OR_TO_UNION_LIMIT, flag = VariableMgr.INVISIBLE)
    private int scanOrToUnionLimit = 4;

    @VarAttr(name = SCAN_OR_TO_UNION_THRESHOLD, flag = VariableMgr.INVISIBLE)
    private long scanOrToUnionThreshold = 50000000;

    @VarAttr(name = SELECT_RATIO_THRESHOLD, flag = VariableMgr.INVISIBLE)
    private double selectRatioThreshold = 0.15;

    @VarAttr(name = DISABLE_FUNCTION_FOLD_CONSTANTS, flag = VariableMgr.INVISIBLE)
    private boolean disableFunctionFoldConstants = false;

    @VarAttr(name = ENABLE_SIMPLIFY_CASE_WHEN, flag = VariableMgr.INVISIBLE)
    private boolean enableSimplifyCaseWhen = true;

    @VarAttr(name = ENABLE_COUNT_STAR_OPTIMIZATION, flag = VariableMgr.INVISIBLE)
    private boolean enableCountStarOptimization = true;

    @VarAttr(name = ENABLE_PARTITION_COLUMN_VALUE_ONLY_OPTIMIZATION, flag = VariableMgr.INVISIBLE)
    private boolean enablePartitionColumnValueOnlyOptimization = true;

    // This variable is introduced to solve compatibility issues/
    // see more details: https://github.com/StarRocks/starrocks/pull/29678
    @VarAttr(name = ENABLE_COLLECT_TABLE_LEVEL_SCAN_STATS)
    private boolean enableCollectTableLevelScanStats = true;

    @VarAttr(name = HIVE_TEMP_STAGING_DIR)
    private String hiveTempStagingDir = "/tmp/starrocks";

    @VarAttr(name = ENABLE_EXPR_PRUNE_PARTITION, flag = VariableMgr.INVISIBLE)
    private boolean enableExprPrunePartition = true;

    @VariableMgr.VarAttr(name = AUDIT_EXECUTE_STMT)
    private boolean auditExecuteStmt = false;

    @VariableMgr.VarAttr(name = ENABLE_SHORT_CIRCUIT)
    private boolean enableShortCircuit = false;

    private int exprChildrenLimit = -1;

    @VarAttr(name = CBO_PREDICATE_SUBFIELD_PATH, flag = VariableMgr.INVISIBLE)
    private boolean cboPredicateSubfieldPath = true;

    @VarAttr(name = CROSS_JOIN_COST_PENALTY, flag = VariableMgr.INVISIBLE)
    private long crossJoinCostPenalty = 1000000;

    public String getHiveTempStagingDir() {
        return hiveTempStagingDir;
    }

    public boolean enableWriteHiveExternalTable() {
        return enableWriteHiveExternalTable;
    }

    public SessionVariable setHiveTempStagingDir(String hiveTempStagingDir) {
        this.hiveTempStagingDir = hiveTempStagingDir;
        return this;
    }

    @VarAttr(name = ENABLE_PRUNE_ICEBERG_MANIFEST)
    private boolean enablePruneIcebergManifest = true;

    @VarAttr(name = ENABLE_READ_ICEBERG_PUFFIN_NDV)
    private boolean enableReadIcebergPuffinNdv = true;

    @VarAttr(name = ENABLE_ICEBERG_COLUMN_STATISTICS)
    private boolean enableIcebergColumnStatistics = false;

    @VarAttr(name = SKEW_JOIN_RAND_RANGE, flag = VariableMgr.INVISIBLE)
    private int skewJoinRandRange = 1000;

    @VarAttr(name = LARGE_DECIMAL_UNDERLYING_TYPE)
    private String largeDecimalUnderlyingType = SessionVariableConstants.PANIC;

    @VarAttr(name = CBO_DERIVE_RANGE_JOIN_PREDICATE)
    private boolean cboDeriveRangeJoinPredicate = false;

    @VarAttr(name = CBO_DERIVE_JOIN_IS_NULL_PREDICATE)
    private boolean cboDeriveJoinIsNullPredicate = true;

    @VarAttr(name = CBO_DECIMAL_CAST_STRING_STRICT, flag = VariableMgr.INVISIBLE)
    private boolean cboDecimalCastStringStrict = true;

    @VarAttr(name = CBO_EQ_BASE_TYPE, flag = VariableMgr.INVISIBLE)
    private String cboEqBaseType = SessionVariableConstants.VARCHAR;

    public boolean isCboDecimalCastStringStrict() {
        return cboDecimalCastStringStrict;
    }

    public String getCboEqBaseType() {
        return cboEqBaseType;
    }

    public boolean isEnablePruneIcebergManifest() {
        return enablePruneIcebergManifest;
    }

    public void setEnablePruneIcebergManifest(boolean enablePruneIcebergManifest) {
        this.enablePruneIcebergManifest = enablePruneIcebergManifest;
    }

    public boolean enableReadIcebergPuffinNdv() {
        return enableReadIcebergPuffinNdv;
    }

    public void setEnableReadIcebergPuffinNdv(boolean enableReadIcebergPuffinNdv) {
        this.enableReadIcebergPuffinNdv = enableReadIcebergPuffinNdv;
    }

    public boolean enableIcebergColumnStatistics() {
        return enableIcebergColumnStatistics;
    }

    public void setEnableIcebergColumnStatistics(boolean enableIcebergColumnStatistics) {
        this.enableIcebergColumnStatistics = enableIcebergColumnStatistics;
    }

    public boolean isCboPredicateSubfieldPath() {
        return cboPredicateSubfieldPath;
    }
    @VarAttr(name = ENABLE_ICEBERG_IDENTITY_COLUMN_OPTIMIZE)
    private boolean enableIcebergIdentityColumnOptimize = true;

    @VarAttr(name = ENABLE_PLAN_SERIALIZE_CONCURRENTLY)
    private boolean enablePlanSerializeConcurrently = true;


    @VarAttr(name = ENABLE_PIPELINE_LEVEL_SHUFFLE, flag = VariableMgr.INVISIBLE)
    private boolean enablePipelineLevelShuffle = true;

    @VarAttr(name = FOLLOWER_QUERY_FORWARD_MODE, flag = VariableMgr.INVISIBLE | VariableMgr.DISABLE_FORWARD_TO_LEADER)
    private String followerForwardMode = "";

    @VarAttr(name = ENABLE_STRICT_ORDER_BY)
    private boolean enableStrictOrderBy = true;

    public void setFollowerQueryForwardMode(String mode) {
        this.followerForwardMode = mode;
    }

    public Optional<Boolean> isFollowerForwardToLeaderOpt() {
        if (Strings.isNullOrEmpty(this.followerForwardMode) ||
                followerForwardMode.equalsIgnoreCase(FollowerQueryForwardMode.DEFAULT.toString())) {
            return Optional.empty();
        }
        return Optional.of(followerForwardMode.equalsIgnoreCase(FollowerQueryForwardMode.LEADER.toString()));
    }

    public int getExprChildrenLimit() {
        return exprChildrenLimit;
    }

    public void setExprChildrenLimit(int exprChildrenLimit) {
        this.exprChildrenLimit = exprChildrenLimit;
    }

    public void setFullSortMaxBufferedRows(long v) {
        fullSortMaxBufferedRows = v;
    }

    public void setFullSortMaxBufferedBytes(long v) {
        fullSortMaxBufferedBytes = v;
    }

    public long getFullSortMaxBufferedRows() {
        return fullSortMaxBufferedRows;
    }

    public long getFullSortMaxBufferedBytes() {
        return fullSortMaxBufferedBytes;
    }

    public void setFullSortLateMaterialization(boolean v) {
        fullSortLateMaterialization = v;
    }

    public boolean isFullSortLateMaterialization() {
        return fullSortLateMaterialization;
    }

    public void setDistinctColumnBuckets(int buckets) {
        distinctColumnBuckets = buckets;
    }

    public int getDistinctColumnBuckets() {
        return distinctColumnBuckets;
    }

    public void setEnableDistinctColumnBucketization(boolean flag) {
        enableDistinctColumnBucketization = flag;
    }

    public boolean isEnableDistinctColumnBucketization() {
        return enableDistinctColumnBucketization;
    }

    public boolean getHudiMORForceJNIReader() {
        return hudiMORForceJNIReader;
    }

    public boolean getPaimonForceJNIReader() {
        return paimonForceJNIReader;
    }

    public void setCboCTEMaxLimit(int cboCTEMaxLimit) {
        this.cboCTEMaxLimit = cboCTEMaxLimit;
    }

    public int getCboCTEMaxLimit() {
        return cboCTEMaxLimit;
    }

    public double getCboPruneShuffleColumnRate() {
        return cboPruneShuffleColumnRate;
    }

    public void setCboPruneShuffleColumnRate(double cboPruneShuffleColumnRate) {
        this.cboPruneShuffleColumnRate = cboPruneShuffleColumnRate;
    }

    public boolean isEnableShowAllVariables() {
        return enableShowAllVariables;
    }

    public void setEnableShowAllVariables(boolean enableShowAllVariables) {
        this.enableShowAllVariables = enableShowAllVariables;
    }

    @VarAttr(name = ENABLE_STRICT_TYPE, flag = VariableMgr.INVISIBLE)
    private boolean enableStrictType = false;

    public boolean isCboUseDBLock() {
        return cboUseDBLock;
    }

    @TestOnly
    public void setCboUseDBLock(boolean cboUseDBLock) {
        this.cboUseDBLock = cboUseDBLock;
    }

    public boolean isEnableQueryTabletAffinity() {
        return enableQueryTabletAffinity;
    }

    public int getStatisticCollectParallelism() {
        return statisticCollectParallelism;
    }

    public void setStatisticCollectParallelism(int parallelism) {
        this.statisticCollectParallelism = parallelism;
    }

    public int getUseComputeNodes() {
        return useComputeNodes;
    }

    public void setUseComputeNodes(int useComputeNodes) {
        this.useComputeNodes = useComputeNodes;
    }

    public boolean isPreferComputeNode() {
        return preferComputeNode;
    }

    public void setPreferComputeNode(boolean preferComputeNode) {
        this.preferComputeNode = preferComputeNode;
    }

    public boolean enableHiveColumnStats() {
        return enableHiveColumnStats;
    }

    public void setEnableHiveColumnStats(boolean enableHiveColumnStats) {
        this.enableHiveColumnStats = enableHiveColumnStats;
    }

    public boolean isEnableHiveMetadataCacheWithInsert() {
        return enableHiveMetadataCacheWithInsert;
    }

    public void setEnableHiveMetadataCacheWithInsert(boolean enableHiveMetadataCacheWithInsert) {
        this.enableHiveMetadataCacheWithInsert = enableHiveMetadataCacheWithInsert;
    }

    public int getHivePartitionStatsSampleSize() {
        return hivePartitionStatsSampleSize;
    }

    public boolean getEnableAdaptiveSinkDop() {
        return enableAdaptiveSinkDop;
    }

    public void setEnableAdaptiveSinkDop(boolean e) {
        this.enableAdaptiveSinkDop = e;
    }

    public long getMaxExecMemByte() {
        return maxExecMemByte;
    }

    public long getLoadMemLimit() {
        return loadMemLimit;
    }

    public int getQueryTimeoutS() {
        return queryTimeoutS;
    }

    public void setQueryDeliveryTimeoutS(int queryDeliveryTimeoutS) {
        this.queryDeliveryTimeoutS = queryDeliveryTimeoutS;
    }

    public int getQueryDeliveryTimeoutS() {
        return queryDeliveryTimeoutS;
    }

    public boolean isEnableProfile() {
        return enableProfile;
    }

    public void setEnableProfile(boolean enableProfile) {
        this.enableProfile = enableProfile;
    }

    public boolean isEnableLoadProfile() {
        return enableLoadProfile;
    }

    public void setEnableLoadProfile(boolean enableLoadProfile) {
        this.enableLoadProfile = enableLoadProfile;
    }

    public boolean isEnableBigQueryProfile() {
        return bigQueryProfileSecondThreshold > 0;
    }

    public int getBigQueryProfileSecondThreshold() {
        return bigQueryProfileSecondThreshold;
    }

    public int getWaitTimeoutS() {
        return waitTimeout;
    }

    public long getSqlMode() {
        return sqlMode;
    }

    public void setSqlMode(long sqlMode) {
        this.sqlMode = sqlMode;
    }

    public long getSqlSelectLimit() {
        return sqlSelectLimit;
    }

    public void setSqlSelectLimit(long limit) {
        this.sqlSelectLimit = limit;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public boolean isInnodbReadOnly() {
        return innodbReadOnly;
    }

    public void setInnodbReadOnly(boolean innodbReadOnly) {
        this.innodbReadOnly = innodbReadOnly;
    }

    public void setMaxExecMemByte(long maxExecMemByte) {
        this.maxExecMemByte = maxExecMemByte;
    }

    public void setLoadMemLimit(long loadMemLimit) {
        this.loadMemLimit = loadMemLimit;
    }

    public void setUsePageCache(boolean usePageCache) {
        this.usePageCache = usePageCache;
    }

    public void setQueryTimeoutS(int queryTimeoutS) {
        this.queryTimeoutS = queryTimeoutS;
    }

    public String getResourceGroup() {
        return resourceGroup;
    }

    public void setResourceGroup(String resourceGroup) {
        this.resourceGroup = resourceGroup;
    }

    public boolean isDisableColocateJoin() {
        return disableColocateJoin;
    }

    public int getParallelExecInstanceNum() {
        return parallelExecInstanceNum;
    }

    public int getMaxParallelScanInstanceNum() {
        return maxParallelScanInstanceNum;
    }

    public String getHdfsBackendSelectorHashAlgorithm() {
        return hdfsBackendSelectorHashAlgorithm;
    }

    public void setHdfsBackendSelectorHashAlgorithm(String hdfsBackendSelectorHashAlgorithm) {
        this.hdfsBackendSelectorHashAlgorithm = hdfsBackendSelectorHashAlgorithm;
    }

    public int getConsistentHashVirtualNodeNum() {
        return consistentHashVirtualNodeNum;
    }

    public void setConsistentHashVirtualNodeNum(int consistentHashVirtualNodeNum) {
        this.consistentHashVirtualNodeNum = consistentHashVirtualNodeNum;
    }

    // when pipeline engine is enabled
    // in case of pipeline_dop > 0: return pipeline_dop * parallelExecInstanceNum;
    // in case of pipeline_dop <= 0 and avgNumCores < 2: return 1;
    // in case of pipeline_dop <= 0 and avgNumCores >=2; return avgNumCores;
    public int getDegreeOfParallelism() {
        if (enablePipelineEngine) {
            if (pipelineDop > 0) {
                return pipelineDop;
            }
            if (maxPipelineDop <= 0) {
                return BackendCoreStat.getDefaultDOP();
            }
            return Math.min(maxPipelineDop, BackendCoreStat.getDefaultDOP());
        } else {
            return parallelExecInstanceNum;
        }
    }

    public int getSinkDegreeOfParallelism() {
        if (enablePipelineEngine) {
            if (pipelineDop > 0) {
                return pipelineDop;
            }
            if (maxPipelineDop <= 0) {
                return BackendCoreStat.getSinkDefaultDOP();
            }
            return Math.min(maxPipelineDop, BackendCoreStat.getSinkDefaultDOP());
        } else {
            return parallelExecInstanceNum;
        }
    }

    public void setParallelExecInstanceNum(int parallelExecInstanceNum) {
        this.parallelExecInstanceNum = parallelExecInstanceNum;
    }

    public void setMaxParallelScanInstanceNum(int maxParallelScanInstanceNum) {
        this.maxParallelScanInstanceNum = maxParallelScanInstanceNum;
    }

    public int getExchangeInstanceParallel() {
        return exchangeInstanceParallel;
    }

    public boolean getEnableInsertStrict() {
        return enableInsertStrict;
    }

    public void setEnableInsertStrict(boolean enableInsertStrict) {
        this.enableInsertStrict = enableInsertStrict;
    }

    public boolean getEnableSpill() {
        return enableSpill;
    }

    public void setEnableSpill(boolean enableSpill) {
        this.enableSpill = enableSpill;
    }

    public void setSpillMode(String spillMode) {
        this.spillMode = spillMode;
    }

    public void setEnableRboTablePrune(boolean enableRboTablePrune) {
        this.enableRboTablePrune = enableRboTablePrune;
    }

    public void setEnableCboTablePrune(boolean enableCboTablePrune) {
        this.enableCboTablePrune = enableCboTablePrune;
    }

    public boolean isEnableRboTablePrune() {
        return enableRboTablePrune;
    }

    public boolean isEnableCboTablePrune() {
        return enableCboTablePrune;
    }

    public int getSpillMemTableSize() {
        return this.spillMemTableSize;
    }

    public int getSpillMemTableNum() {
        return this.spillMemTableNum;
    }

    public double getSpillMemLimitThreshold() {
        return this.spillMemLimitThreshold;
    }

    public long getSpillOperatorMinBytes() {
        return this.spillOperatorMinBytes;
    }

    public long getSpillOperatorMaxBytes() {
        return this.spillOperatorMaxBytes;
    }

    public int getSpillEncodeLevel() {
        return this.spillEncodeLevel;
    }

    public boolean getForwardToLeader() {
        return forwardToLeader;
    }

    public void setMaxScanKeyNum(int maxScanKeyNum) {
        this.maxScanKeyNum = maxScanKeyNum;
    }

    public void setMaxPushdownConditionsPerColumn(int maxPushdownConditionsPerColumn) {
        this.maxPushdownConditionsPerColumn = maxPushdownConditionsPerColumn;
    }

    public boolean isHashJoinPushDownRightTable() {
        return this.hashJoinPushDownRightTable;
    }

    public String getStreamingPreaggregationMode() {
        return streamingPreaggregationMode;
    }

    public boolean isDisableJoinReorder() {
        return disableJoinReorder;
    }

    public void disableJoinReorder() {
        this.disableJoinReorder = true;
    }

    public void enableJoinReorder() {
        this.disableJoinReorder = false;
    }

    public boolean isEnablePredicateReorder() {
        return enablePredicateReorder;
    }

    public void disablePredicateReorder() {
        this.enablePredicateReorder = false;
    }

    public void enablePredicateReorder() {
        this.enablePredicateReorder = true;
    }

    public boolean isEnableFilterUnusedColumnsInScanStage() {
        return enableFilterUnusedColumnsInScanStage;
    }

    public void disableTrimOnlyFilteredColumnsInScanStage() {
        this.enableFilterUnusedColumnsInScanStage = false;
    }

    public void enableTrimOnlyFilteredColumnsInScanStage() {
        this.enableFilterUnusedColumnsInScanStage = true;
    }

    public boolean isCboEnableDPJoinReorder() {
        return cboEnableDPJoinReorder;
    }

    public void disableDPJoinReorder() {
        this.cboEnableDPJoinReorder = false;
    }

    public void enableDPJoinReorder() {
        this.cboEnableDPJoinReorder = true;
    }

    public long getCboMaxReorderNodeUseDP() {
        return cboMaxReorderNodeUseDP;
    }

    public boolean isCboEnableGreedyJoinReorder() {
        return cboEnableGreedyJoinReorder;
    }

    public void disableGreedyJoinReorder() {
        this.cboEnableGreedyJoinReorder = false;
    }

    public void enableGreedyJoinReorder() {
        this.cboEnableGreedyJoinReorder = true;
    }

    public int getCboMaxReorderNode() {
        return cboMaxReorderNode;
    }

    public int getCboDebugAliveBackendNumber() {
        return cboDebugAliveBackendNumber;
    }

    public long getTransactionVisibleWaitTimeout() {
        return transactionVisibleWaitTimeout;
    }

    public void setTransactionVisibleWaitTimeout(long transactionVisibleWaitTimeout) {
        this.transactionVisibleWaitTimeout = transactionVisibleWaitTimeout;
    }

    public boolean getForceScheduleLocal() {
        return forceScheduleLocal;
    }

    public int getCboMaxReorderNodeUseExhaustive() {
        return cboMaxReorderNodeUseExhaustive;
    }

    public int getNewPlannerAggStage() {
        return newPlannerAggStage;
    }

    public void setNewPlanerAggStage(int stage) {
        this.newPlannerAggStage = stage;
    }

    public void setMaxTransformReorderJoins(int maxReorderNodeUseExhaustive) {
        this.cboMaxReorderNodeUseExhaustive = maxReorderNodeUseExhaustive;
    }

    public int getMaxTransformReorderJoins() {
        return this.cboMaxReorderNodeUseExhaustive;
    }

    public long getBroadcastRowCountLimit() {
        return broadcastRowCountLimit;
    }

    public double getBroadcastRightTableScaleFactor() {
        return broadcastRightTableScaleFactor;
    }

    public long getOptimizerExecuteTimeout() {
        return optimizerExecuteTimeout;
    }

    public void setOptimizerExecuteTimeout(long optimizerExecuteTimeout) {
        this.optimizerExecuteTimeout = optimizerExecuteTimeout;
    }

    public QueryDebugOptions getQueryDebugOptions() {
        if (Strings.isNullOrEmpty(queryDebugOptions)) {
            return QueryDebugOptions.getInstance();
        }

        return QueryDebugOptions.read(queryDebugOptions);
    }

    public void setQueryDebugOptions(String queryDebugOptions) throws SemanticException {
        if (!Strings.isNullOrEmpty(queryDebugOptions)) {
            try {
                QueryDebugOptions.read(queryDebugOptions);
            } catch (Exception e) {
                throw new SemanticException("Invalid planner options: %s", queryDebugOptions);
            }
        }
        this.queryDebugOptions = queryDebugOptions;
    }

    public long getOptimizerMaterializedViewTimeLimitMillis() {
        return optimizerMaterializedViewTimeLimitMillis;
    }

    public void setOptimizerMaterializedViewTimeLimitMillis(long millis) {
        this.optimizerMaterializedViewTimeLimitMillis = millis;
    }

    public boolean getEnableGroupbyUseOutputAlias() {
        return enableGroupbyUseOutputAlias;
    }

    public void setEnableGroupbyUseOutputAlias(boolean enableGroupbyUseOutputAlias) {
        this.enableGroupbyUseOutputAlias = enableGroupbyUseOutputAlias;
    }

    public boolean getEnableQueryDump() {
        return enableQueryDump;
    }

    public void setEnableQueryDump(boolean enable) {
        this.enableQueryDump = enable;
    }

    public boolean getEnableGlobalRuntimeFilter() {
        return enableGlobalRuntimeFilter;
    }

    public void setEnableGlobalRuntimeFilter(boolean value) {
        enableGlobalRuntimeFilter = value;
    }

    public boolean getEnableTopNRuntimeFilter() {
        return enableTopNRuntimeFilter;
    }

    public void setGlobalRuntimeFilterBuildMaxSize(long globalRuntimeFilterBuildMaxSize) {
        this.globalRuntimeFilterBuildMaxSize = globalRuntimeFilterBuildMaxSize;
    }

    public long getGlobalRuntimeFilterBuildMaxSize() {
        return globalRuntimeFilterBuildMaxSize;
    }

    public void setGlobalRuntimeFilterBuildMinSize(long value) {
        this.globalRuntimeFilterBuildMinSize = value;
    }

    public long getGlobalRuntimeFilterBuildMinSize() {
        return globalRuntimeFilterBuildMinSize;
    }

    public long getGlobalRuntimeFilterProbeMinSize() {
        return globalRuntimeFilterProbeMinSize;
    }

    public void setGlobalRuntimeFilterProbeMinSize(long globalRuntimeFilterProbeMinSize) {
        this.globalRuntimeFilterProbeMinSize = globalRuntimeFilterProbeMinSize;
    }

    public float getGlobalRuntimeFilterProbeMinSelectivity() {
        return globalRuntimeFilterProbeMinSelectivity;
    }

    public boolean isMVPlanner() {
        return enableMVPlanner;
    }

    public void setMVPlanner(boolean enable) {
        this.enableMVPlanner = enable;
    }

    public boolean isEnableIncrementalRefreshMV() {
        return enableIncrementalRefreshMV;
    }

    public void setEnableIncrementalRefreshMv(boolean enable) {
        this.enableIncrementalRefreshMV = enable;
    }

    public long getLogRejectedRecordNum() {
        return logRejectedRecordNum;
    }

    public void setLogRejectedRecordNum(long logRejectedRecordNum) {
        this.logRejectedRecordNum = logRejectedRecordNum;
    }

    public boolean isEnablePipelineEngine() {
        return enablePipelineEngine;
    }

    public boolean isEnablePipelineAdaptiveDop() {
        return enablePipelineEngine && pipelineDop <= 0;
    }

    public boolean isEnableRuntimeAdaptiveDop() {
        return enablePipelineEngine && enableRuntimeAdaptiveDop;
    }

    public long getAdaptiveDopMaxBlockRowsPerDriverSeq() {
        return adaptiveDopMaxBlockRowsPerDriverSeq;
    }

    public long getAdaptiveDopMaxOutputAmplificationFactor() {
        return adaptiveDopMaxOutputAmplificationFactor;
    }

    public void setEnablePipelineEngine(boolean enablePipelineEngine) {
        this.enablePipelineEngine = enablePipelineEngine;
    }

    public void setEnableLocalShuffleAgg(boolean enableLocalShuffleAgg) {
        this.enableLocalShuffleAgg = enableLocalShuffleAgg;
    }

    public boolean isEnableLocalShuffleAgg() {
        return enableLocalShuffleAgg;
    }

    public boolean isEnableTabletInternalParallel() {
        return enableTabletInternalParallel;
    }

    public boolean isEnableResourceGroup() {
        return true;
    }

    public int getMaxBucketsPerBeToUseBalancerAssignment() {
        return maxBucketsPerBeToUseBalancerAssignment;
    }

    public void setMaxBucketsPerBeToUseBalancerAssignment(int maxBucketsPerBeToUseBalancerAssignment) {
        this.maxBucketsPerBeToUseBalancerAssignment = maxBucketsPerBeToUseBalancerAssignment;
    }

    public void setPipelineDop(int pipelineDop) {
        this.pipelineDop = pipelineDop;
    }

    public int getPipelineDop() {
        return this.pipelineDop;
    }

    public int getPipelineSinkDop() {
        return pipelineSinkDop;
    }

    public void setPipelineSinkDop(int pipelineSinkDop) {
        this.pipelineSinkDop = pipelineSinkDop;
    }

    public void setMaxPipelineDop(int maxPipelineDop) {
        this.maxPipelineDop = maxPipelineDop;
    }

    public int getMaxPipelineDop() {
        return this.maxPipelineDop;
    }

    public boolean isEnableSharedScan() {
        return enableSharedScan;
    }

    public int getResourceGroupId() {
        return resourceGroupId;
    }

    public int getProfileTimeout() {
        return profileTimeout;
    }

    public int getRuntimeProfileReportInterval() {
        return runtimeProfileReportInterval;
    }

    public void setPipelineProfileLevel(int pipelineProfileLevel) {
        this.pipelineProfileLevel = pipelineProfileLevel;
    }

    public int getPipelineProfileLevel() {
        return pipelineProfileLevel;
    }

    public boolean isEnableAsyncProfile() {
        return enableAsyncProfile;
    }

    public void setEnableAsyncProfile(boolean enableAsyncProfile) {
        this.enableAsyncProfile = enableAsyncProfile;
    }

    public boolean isEnableReplicationJoin() {
        return false;
    }

    public String getMaterializedViewRewriteMode() {
        return materializedViewRewriteMode;
    }

    public void setMaterializedViewRewriteMode(String materializedViewRewriteMode) {
        this.materializedViewRewriteMode = materializedViewRewriteMode;
    }

    public boolean isDisableMaterializedViewRewrite() {
        return materializedViewRewriteMode.equalsIgnoreCase(MaterializedViewRewriteMode.MODE_DISABLE);
    }

    public boolean isEnableMaterializedViewForceRewrite() {
        return materializedViewRewriteMode.equalsIgnoreCase(MaterializedViewRewriteMode.MODE_FORCE) ||
                materializedViewRewriteMode.equalsIgnoreCase(MaterializedViewRewriteMode.MODE_FORCE_OR_ERROR);
    }

    public boolean isEnableMaterializedViewRewriteOrError() {
        return materializedViewRewriteMode.equalsIgnoreCase(MaterializedViewRewriteMode.MODE_FORCE_OR_ERROR) ||
                materializedViewRewriteMode.equalsIgnoreCase(MaterializedViewRewriteMode.MODE_DEFAULT_OR_ERROR);
    }

    public boolean isSetUseNthExecPlan() {
        return useNthExecPlan > 0;
    }

    public int getUseNthExecPlan() {
        return useNthExecPlan;
    }

    public void setUseNthExecPlan(int nthExecPlan) {
        this.useNthExecPlan = nthExecPlan;
    }

    public void setEnableReplicationJoin(boolean enableReplicationJoin) {
    }

    public boolean isUseCorrelatedJoinEstimate() {
        return useCorrelatedJoinEstimate;
    }

    public void setUseCorrelatedJoinEstimate(boolean useCorrelatedJoinEstimate) {
        this.useCorrelatedJoinEstimate = useCorrelatedJoinEstimate;
    }

    public boolean isEnableLowCardinalityOptimize() {
        return enableLowCardinalityOptimize;
    }

    public boolean isEnableRewriteGroupingsetsToUnionAll() {
        return enableRewriteGroupingSetsToUnionAll;
    }

    public void setEnableRewriteGroupingSetsToUnionAll(boolean enableRewriteGroupingSetsToUnionAll) {
        this.enableRewriteGroupingSetsToUnionAll = enableRewriteGroupingSetsToUnionAll;
    }

    public void setEnableLowCardinalityOptimize(boolean enableLowCardinalityOptimize) {
        this.enableLowCardinalityOptimize = enableLowCardinalityOptimize;
    }

    public boolean isEnableColumnExprPredicate() {
        return enableColumnExprPredicate;
    }

    public boolean isEnableExchangePassThrough() {
        return enableExchangePassThrough;
    }

    public boolean isEnableExchangePerf() {
        return enableExchangePerf;
    }

    public boolean isAllowDefaultPartition() {
        return allowDefaultPartition;
    }

    public void setAllowDefaultPartition(boolean allowDefaultPartition) {
        this.allowDefaultPartition = allowDefaultPartition;
    }

    /**
     * check cbo_cte_reuse && enable_pipeline
     */
    public boolean isCboCteReuse() {
        return cboCteReuse && enablePipelineEngine;
    }

    public void setCboCteReuse(boolean cboCteReuse) {
        this.cboCteReuse = cboCteReuse;
    }

    public boolean isPreferCTERewrite() {
        return preferCTERewrite;
    }

    public void setSingleNodeExecPlan(boolean singleNodeExecPlan) {
        this.singleNodeExecPlan = singleNodeExecPlan;
    }

    public boolean isSingleNodeExecPlan() {
        return singleNodeExecPlan;
    }

    public double getCboCTERuseRatio() {
        return cboCTERuseRatio;
    }

    public void setCboCTERuseRatio(double cboCTERuseRatio) {
        this.cboCTERuseRatio = cboCTERuseRatio;
    }

    public int getCboPushDownAggregateMode() {
        return cboPushDownAggregateMode;
    }

    public void setCboPushDownAggregateMode(int cboPushDownAggregateMode) {
        this.cboPushDownAggregateMode = cboPushDownAggregateMode;
    }

    public String getCboPushDownAggregate() {
        return cboPushDownAggregate;
    }

    public void setCboPushDownAggregate(String cboPushDownAggregate) {
        this.cboPushDownAggregate = cboPushDownAggregate;
    }

    public void setCboPushDownDistinctBelowWindow(boolean flag) {
        this.cboPushDownDistinctBelowWindow = flag;
    }

    public boolean isCboPushDownDistinctBelowWindow() {
        return this.cboPushDownDistinctBelowWindow;
    }

    public boolean isEnableSQLDigest() {
        return enableSQLDigest;
    }

    public void enableJoinReorder(boolean value) {
        this.disableJoinReorder = !value;
    }

    public String getJoinImplementationMode() {
        return joinImplementationMode;
    }

    public void setJoinImplementationMode(String joinImplementationMode) {
        this.joinImplementationMode = joinImplementationMode;
    }

    public boolean isRuntimeFilterOnExchangeNode() {
        return runtimeFilterOnExchangeNode;
    }

    public void setEnableRuntimeFilterOnExchangeNode(boolean value) {
        this.runtimeFilterOnExchangeNode = value;
    }

    public boolean isEnableMultiColumnsOnGlobbalRuntimeFilter() {
        return enableMultiColumnsOnGlobalRuntimeFilter;
    }

    public void setEnableMultiColumnsOnGlobbalRuntimeFilter(boolean value) {
        this.enableMultiColumnsOnGlobalRuntimeFilter = value;
    }

    public boolean isEnableQueryDebugTrace() {
        return enableQueryDebugTrace;
    }

    public void setEnableQueryDebugTrace(boolean val) {
        this.enableQueryDebugTrace = val;
    }

    public String getloadTransmissionCompressionType() {
        return loadTransmissionCompressionType;
    }

    public boolean isInterpolatePassthrough() {
        return interpolatePassthrough;
    }

    public void setInterpolatePassthrough(boolean value) {
        this.interpolatePassthrough = value;
    }
    public boolean isHashJoinInterpolatePassthrough() {
        return hashJoinInterpolatePassthrough;
    }

    public int getParseTokensLimit() {
        return parseTokensLimit;
    }

    public void setParseTokensLimit(int parseTokensLimit) {
        this.parseTokensLimit = parseTokensLimit;
    }

    public boolean isEnableQueryCache() {
        return isEnablePipelineEngine() && enableQueryCache;
    }

    public long getQueryCacheEntryMaxBytes() {
        return queryCacheEntryMaxBytes;
    }

    public long getQueryCacheEntryMaxRows() {
        return queryCacheEntryMaxRows;
    }

    public void setQueryCacheHotPartitionNum(int n) {
        queryCacheHotPartitionNum = n;
    }

    public int getQueryCacheHotPartitionNum() {
        return queryCacheHotPartitionNum;
    }

    public void setQueryCacheAggCardinalityLimit(long limit) {
        this.queryCacheAggCardinalityLimit = limit;
    }

    public long getQueryCacheAggCardinalityLimit() {
        return queryCacheAggCardinalityLimit;
    }

    public void setEnableQueryCache(boolean on) {
        enableQueryCache = on;
    }

    public boolean isQueryCacheForcePopulate() {
        return queryCacheForcePopulate;
    }

    public int getNestedMvRewriteMaxLevel() {
        return nestedMvRewriteMaxLevel;
    }

    public boolean isEnableMaterializedViewRewrite() {
        return enableMaterializedViewRewrite;
    }

    public void setEnableMaterializedViewRewrite(boolean enableMaterializedViewRewrite) {
        this.enableMaterializedViewRewrite = enableMaterializedViewRewrite;
    }

    public boolean isEnableMaterializedViewRewriteForInsert() {
        return enableMaterializedViewRewriteForInsert;
    }

    public void setEnableMaterializedViewRewriteForInsert(boolean value) {
        this.enableMaterializedViewRewriteForInsert = value;
    }

    public boolean isEnableMaterializedViewUnionRewrite() {
        return enableMaterializedViewUnionRewrite;
    }

    public void setEnableMaterializedViewUnionRewrite(boolean enableMaterializedViewUnionRewrite) {
        this.enableMaterializedViewUnionRewrite = enableMaterializedViewUnionRewrite;
    }

    public boolean isEnableSyncMaterializedViewRewrite() {
        return enableSyncMaterializedViewRewrite;
    }

    public void setEnableSyncMaterializedViewRewrite(boolean enableSyncMaterializedViewRewrite) {
        this.enableSyncMaterializedViewRewrite = enableSyncMaterializedViewRewrite;
    }

    // 1 means the mvs directly based on base table
    public void setNestedMvRewriteMaxLevel(int nestedMvRewriteMaxLevel) {
        if (nestedMvRewriteMaxLevel <= 0) {
            nestedMvRewriteMaxLevel = 1;
        }
        this.nestedMvRewriteMaxLevel = nestedMvRewriteMaxLevel;
    }

    public boolean isEnableRuleBasedMaterializedViewRewrite() {
        return enableRuleBasedMaterializedViewRewrite;
    }

    public void setEnableRuleBasedMaterializedViewRewrite(boolean enableRuleBasedMaterializedViewRewrite) {
        this.enableRuleBasedMaterializedViewRewrite = enableRuleBasedMaterializedViewRewrite;
    }

    public boolean isEnableMaterializedViewRewritePartitionCompensate() {
        return enableMaterializedViewRewritePartitionCompensate;
    }

    public void setEnableMaterializedViewRewritePartitionCompensate(boolean enableMaterializedViewRewritePartitionCompensate) {
        this.enableMaterializedViewRewritePartitionCompensate = enableMaterializedViewRewritePartitionCompensate;
    }

    public boolean isEnableMaterializedViewViewDeltaRewrite() {
        return enableMaterializedViewViewDeltaRewrite;
    }

    public void setEnableMaterializedViewViewDeltaRewrite(boolean enableMaterializedViewViewDeltaRewrite) {
        this.enableMaterializedViewViewDeltaRewrite = enableMaterializedViewViewDeltaRewrite;
    }

    public int getMaterializedViewJoinSameTablePermutationLimit() {
        return materializedViewJoinSameTablePermutationLimit;
    }

    public boolean isEnableMaterializedViewSingleTableViewDeltaRewrite() {
        return enableMaterializedViewSingleTableViewDeltaRewrite;
    }

    public void setEnableMaterializedViewSingleTableViewDeltaRewrite(
            boolean enableMaterializedViewSingleTableViewDeltaRewrite) {
        this.enableMaterializedViewSingleTableViewDeltaRewrite = enableMaterializedViewSingleTableViewDeltaRewrite;
    }

    public void setEnableMaterializedViewRewriteGreedyMode(boolean enableMaterializedViewRewriteGreedyMode) {
        this.enableMaterializedViewRewriteGreedyMode = enableMaterializedViewRewriteGreedyMode;
    }

    public boolean isEnableMaterializedViewRewriteGreedyMode() {
        return this.enableMaterializedViewRewriteGreedyMode;
    }

    public void setEnableMaterializedViewPlanCache(boolean enableMaterializedViewPlanCache) {
        this.enableMaterializedViewPlanCache = enableMaterializedViewPlanCache;
    }

    public boolean isEnableMaterializedViewPlanCache() {
        return this.enableMaterializedViewPlanCache;
    }

    public void setEnableViewBasedMvRewrite(boolean enableViewBasedMvRewrite) {
        this.enableViewBasedMvRewrite = enableViewBasedMvRewrite;
    }

    public boolean isEnableViewBasedMvRewrite() {
        return this.enableViewBasedMvRewrite;
    }

    public int getCboMaterializedViewRewriteRuleOutputLimit() {
        return cboMaterializedViewRewriteRuleOutputLimit;
    }

    public void setCboMaterializedViewRewriteRuleOutputLimit(int limit) {
        this.cboMaterializedViewRewriteRuleOutputLimit = limit;
    }

    public int getCboMaterializedViewRewriteCandidateLimit() {
        return cboMaterializedViewRewriteCandidateLimit;
    }

    public void setCboMaterializedViewRewriteCandidateLimit(int limit) {
        this.cboMaterializedViewRewriteCandidateLimit = limit;
    }

    public String getQueryExcludingMVNames() {
        return queryExcludingMVNames;
    }

    public void setQueryExcludingMVNames(String queryExcludingMVNames) {
        this.queryExcludingMVNames = queryExcludingMVNames;
    }

    public String getQueryIncludingMVNames() {
        return queryIncludingMVNames;
    }

    public void setQueryIncludingMVNames(String queryIncludingMVNames) {
        this.queryIncludingMVNames = queryIncludingMVNames;
    }

    public String getAnalyzeForMV() {
        return analyzeTypeForMV;
    }

    public boolean isEnableBigQueryLog() {
        return enableBigQueryLog;
    }

    public void setEnableBigQueryLog(boolean enableBigQueryLog) {
        this.enableBigQueryLog = enableBigQueryLog;
    }

    public long getBigQueryLogCPUSecondThreshold() {
        return this.bigQueryLogCPUSecondThreshold;
    }

    public void setBigQueryLogCpuSecondThreshold(long bigQueryLogCPUSecondThreshold) {
        this.bigQueryLogCPUSecondThreshold = bigQueryLogCPUSecondThreshold;
    }

    public long getBigQueryLogScanBytesThreshold() {
        return bigQueryLogScanBytesThreshold;
    }

    public void setBigQueryLogScanBytesThreshold(long bigQueryLogScanBytesThreshold) {
        this.bigQueryLogScanBytesThreshold = bigQueryLogScanBytesThreshold;
    }

    public long getBigQueryLogScanRowsThreshold() {
        return bigQueryLogScanRowsThreshold;
    }

    public void setBigQueryLogScanRowsThreshold(long bigQueryLogScanRowsThreshold) {
        this.bigQueryLogScanRowsThreshold = bigQueryLogScanRowsThreshold;
    }

    public String getSqlDialect() {
        return this.sqlDialect;
    }

    public void setSqlDialect(String dialect) {
        this.sqlDialect = dialect;
    }

    public boolean isEnableOuterJoinReorder() {
        return enableOuterJoinReorder;
    }

    public void setEnableOuterJoinReorder(boolean enableOuterJoinReorder) {
        this.enableOuterJoinReorder = enableOuterJoinReorder;
    }

    public int getCboReorderThresholdUseExhaustive() {
        return cboReorderThresholdUseExhaustive;
    }

    public void setCboReorderThresholdUseExhaustive(int cboReorderThresholdUseExhaustive) {
        this.cboReorderThresholdUseExhaustive = cboReorderThresholdUseExhaustive;
    }

    public void setEnableRewriteSumByAssociativeRule(boolean enableRewriteSumByAssociativeRule) {
        this.enableRewriteSumByAssociativeRule = enableRewriteSumByAssociativeRule;
    }

    public boolean isEnableRewriteSumByAssociativeRule() {
        return this.enableRewriteSumByAssociativeRule;
    }

    public void setEnableRewriteSimpleAggToMetaScan(boolean enableRewriteSimpleAggToMetaScan) {
        this.enableRewriteSimpleAggToMetaScan = enableRewriteSimpleAggToMetaScan;
    }

    public boolean isEnableRewriteSimpleAggToMetaScan() {
        return this.enableRewriteSimpleAggToMetaScan;
    }

    public boolean getEnablePruneComplexTypes() {
        return this.enablePruneComplexTypes;
    }

    public void setEnablePruneComplexTypes(boolean enablePruneComplexTypes) {
        this.enablePruneComplexTypes = enablePruneComplexTypes;
    }

    public boolean getEnablePruneComplexTypesInUnnest() {
        return this.enablePruneComplexTypesInUnnest;
    }

    public void setEnablePruneComplexTypesInUnnest(boolean enablePruneComplexTypesInUnnest) {
        this.enablePruneComplexTypesInUnnest = enablePruneComplexTypesInUnnest;
    }

    public int getRangePrunerPredicateMaxLen() {
        return rangePrunerPredicateMaxLen;
    }

    public void setRangePrunerPredicateMaxLen(int rangePrunerPredicateMaxLen) {
        this.rangePrunerPredicateMaxLen = rangePrunerPredicateMaxLen;
    }

    public String getDefaultTableCompression() {
        return defaultTableCompressionAlgorithm;
    }

    public void setDefaultTableCompression(String compression) {
        this.defaultTableCompressionAlgorithm = compression;
    }

    public boolean getHDFSBackendSelectorScanRangeShuffle() {
        return hdfsBackendSelectorScanRangeShuffle;
    }

    public boolean isEnableStrictType() {
        return enableStrictType;
    }

    public void setEnableStrictType(boolean val) {
        this.enableStrictType = val;
    }

    public boolean getEnablePlanValidation() {
        return this.enablePlanValidation;
    }

    public void setEnablePlanValidation(boolean val) {
        this.enablePlanValidation = val;
    }

    public boolean isCboPruneSubfield() {
        return cboPruneSubfield;
    }

    public void setCboPruneSubfield(boolean cboPruneSubfield) {
        this.cboPruneSubfield = cboPruneSubfield;
    }

    public int getScanOrToUnionLimit() {
        return scanOrToUnionLimit;
    }

    public void setScanOrToUnionLimit(int scanOrToUnionLimit) {
        this.scanOrToUnionLimit = scanOrToUnionLimit;
    }

    public long getScanOrToUnionThreshold() {
        return scanOrToUnionThreshold;
    }

    public void setScanOrToUnionThreshold(long scanOrToUnionThreshold) {
        this.scanOrToUnionThreshold = scanOrToUnionThreshold;
    }

    public double getSelectRatioThreshold() {
        return selectRatioThreshold;
    }

    public void setSelectRatioThreshold(double selectRatioThreshold) {
        this.selectRatioThreshold = selectRatioThreshold;
    }

    public boolean isDisableFunctionFoldConstants() {
        return disableFunctionFoldConstants;
    }

    public void setDisableFunctionFoldConstants(boolean disableFunctionFoldConstants) {
        this.disableFunctionFoldConstants = disableFunctionFoldConstants;
    }

    public boolean isEnableSimplifyCaseWhen() {
        return enableSimplifyCaseWhen;
    }

    public void setEnableSimplifyCaseWhen(boolean enableSimplifyCaseWhen) {
        this.enableSimplifyCaseWhen = enableSimplifyCaseWhen;
    }

    public boolean isEnableCountStarOptimization() {
        return enableCountStarOptimization;
    }

    public void setEnableCountStarOptimization(boolean v) {
        enableCountStarOptimization = v;
    }

    public boolean isEnablePartitionColumnValueOnlyOptimization() {
        return enablePartitionColumnValueOnlyOptimization;
    }

    public void setEnablePartitionColumnValueOnlyOptimization(boolean v) {
        enablePartitionColumnValueOnlyOptimization = v;
    }

    public boolean isEnableExprPrunePartition() {
        return enableExprPrunePartition;
    }

    public void setEnableExprPrunePartition(boolean enableExprPrunePartition) {
        this.enableExprPrunePartition = enableExprPrunePartition;
    }

    public boolean enableCboDeriveRangeJoinPredicate() {
        return cboDeriveRangeJoinPredicate;
    }

    public void setCboDeriveRangeJoinPredicate(boolean cboDeriveRangeJoinPredicate) {
        this.cboDeriveRangeJoinPredicate = cboDeriveRangeJoinPredicate;
    }

    public boolean isCboDeriveJoinIsNullPredicate() {
        return cboDeriveJoinIsNullPredicate;
    }

    public boolean isAuditExecuteStmt() {
        return auditExecuteStmt;
    }

    public void setEnableShortCircuit(boolean enableShortCircuit) {
        this.enableShortCircuit = enableShortCircuit;
    }

    public boolean isEnableShortCircuit() {
        return enableShortCircuit;
    }

    public void setLargeDecimalUnderlyingType(String type) {
        if (type.equalsIgnoreCase(SessionVariableConstants.PANIC) ||
                type.equalsIgnoreCase(SessionVariableConstants.DECIMAL) ||
                type.equalsIgnoreCase(SessionVariableConstants.DOUBLE)) {
            largeDecimalUnderlyingType = type.toLowerCase();
        } else {
            throw new IllegalArgumentException(
                    "Legal values of large_decimal_underlying_type are panic|decimal|double");
        }
    }

    public String getLargeDecimalUnderlyingType() {
        return largeDecimalUnderlyingType;
    }

    public long getCrossJoinCostPenalty() {
        return crossJoinCostPenalty;
    }

    public void setCrossJoinCostPenalty(long crossJoinCostPenalty) {
        this.crossJoinCostPenalty = crossJoinCostPenalty;
    }

    public boolean getEnableIcebergIdentityColumnOptimize() {
        return enableIcebergIdentityColumnOptimize;
    }

    public boolean getEnablePlanSerializeConcurrently() {
        return enablePlanSerializeConcurrently;
    }

    public int getSkewJoinRandRange() {
        return skewJoinRandRange;
    }

    public void setSkewJoinRandRange(int skewJoinRandRange) {
        this.skewJoinRandRange = skewJoinRandRange;
    }

    public boolean isEnableStrictOrderBy() {
        return enableStrictOrderBy;
    }

    public void setEnableStrictOrderBy(boolean enableStrictOrderBy) {
        this.enableStrictOrderBy = enableStrictOrderBy;
    }

    // Serialize to thrift object
    // used for rest api
    public TQueryOptions toThrift() {
        TQueryOptions tResult = new TQueryOptions();
        tResult.setMem_limit(maxExecMemByte);
        tResult.setQuery_mem_limit(queryMemLimit);

        // Avoid integer overflow
        tResult.setQuery_timeout(Math.min(Integer.MAX_VALUE / 1000, queryTimeoutS));
        tResult.setQuery_delivery_timeout(Math.min(Integer.MAX_VALUE / 1000, queryDeliveryTimeoutS));
        tResult.setEnable_profile(enableProfile);
        tResult.setBig_query_profile_second_threshold(bigQueryProfileSecondThreshold);
        tResult.setRuntime_profile_report_interval(runtimeProfileReportInterval);
        tResult.setBatch_size(chunkSize);
        tResult.setLoad_mem_limit(loadMemLimit);

        if (maxScanKeyNum > -1) {
            tResult.setMax_scan_key_num(maxScanKeyNum);
        }
        if (maxPushdownConditionsPerColumn > -1) {
            tResult.setMax_pushdown_conditions_per_column(maxPushdownConditionsPerColumn);
        }

        if (SqlModeHelper.check(sqlMode, SqlModeHelper.MODE_ERROR_IF_OVERFLOW)) {
            tResult.setOverflow_mode(TOverflowMode.REPORT_ERROR);
        }

        tResult.setEnable_spill(enableSpill);
        if (enableSpill) {
            tResult.setSpill_mem_table_size(spillMemTableSize);
            tResult.setSpill_mem_table_num(spillMemTableNum);
            tResult.setSpill_mem_limit_threshold(spillMemLimitThreshold);
            tResult.setSpill_operator_min_bytes(spillOperatorMinBytes);
            tResult.setSpill_operator_max_bytes(spillOperatorMaxBytes);
            tResult.setSpill_revocable_max_bytes(spillRevocableMaxBytes);
            tResult.setSpill_encode_level(spillEncodeLevel);
            tResult.setSpillable_operator_mask(spillableOperatorMask);
        }

        // Compression Type
        TCompressionType compressionType = CompressionUtils.findTCompressionByName(transmissionCompressionType);
        if (compressionType != null) {
            tResult.setTransmission_compression_type(compressionType);
        }

        tResult.setTransmission_encode_level(transmissionEncodeLevel);
        tResult.setGroup_concat_max_len(groupConcatMaxLen);
        tResult.setRpc_http_min_size(rpcHttpMinSize);
        tResult.setInterleaving_group_size(interleavingGroupSize);

        TCompressionType loadCompressionType =
                CompressionUtils.findTCompressionByName(loadTransmissionCompressionType);
        if (loadCompressionType != null) {
            tResult.setLoad_transmission_compression_type(loadCompressionType);
        }

        tResult.setRuntime_join_filter_pushdown_limit(runtimeJoinFilterPushDownLimit);
        tResult.setGlobal_runtime_filter_build_max_size(globalRuntimeFilterBuildMaxSize);
        tResult.setRuntime_filter_wait_timeout_ms(globalRuntimeFilterWaitTimeout);
        tResult.setRuntime_filter_send_timeout_ms(globalRuntimeFilterRpcTimeout);
        tResult.setRuntime_filter_scan_wait_time_ms(runtimeFilterScanWaitTime);
        tResult.setRuntime_filter_rpc_http_min_size(globalRuntimeFilterRpcHttpMinSize);
        tResult.setPipeline_dop(pipelineDop);
        if (pipelineProfileLevel == 2) {
            tResult.setPipeline_profile_level(TPipelineProfileLevel.DETAIL);
        } else {
            tResult.setPipeline_profile_level(TPipelineProfileLevel.MERGE);
        }

        tResult.setEnable_tablet_internal_parallel(enableTabletInternalParallel);
        tResult.setTablet_internal_parallel_mode(
                TTabletInternalParallelMode.valueOf(tabletInternalParallelMode.toUpperCase()));
        tResult.setSpill_mode(TSpillMode.valueOf(spillMode.toUpperCase()));
        tResult.setEnable_query_debug_trace(enableQueryDebugTrace);
        tResult.setEnable_pipeline_query_statistic(true);
        tResult.setRuntime_filter_early_return_selectivity(runtimeFilterEarlyReturnSelectivity);

        tResult.setAllow_throw_exception((sqlMode & SqlModeHelper.MODE_ALLOW_THROW_EXCEPTION) != 0);

        tResult.setEnable_scan_datacache(enableScanDataCache);
        tResult.setEnable_populate_datacache(enablePopulateDataCache);
        tResult.setEnable_file_metacache(enableFileMetaCache);
        tResult.setHudi_mor_force_jni_reader(hudiMORForceJNIReader);
        tResult.setIo_tasks_per_scan_operator(ioTasksPerScanOperator);
        tResult.setConnector_io_tasks_per_scan_operator(connectorIoTasksPerScanOperator);
        tResult.setUse_page_cache(usePageCache);

        tResult.setEnable_connector_adaptive_io_tasks(enableConnectorAdaptiveIoTasks);
        tResult.setConnector_io_tasks_slow_io_latency_ms(connectorIoTasksSlowIoLatency);
        tResult.setConnector_scan_use_query_mem_ratio(connectorScanUseQueryMemRatio);
        tResult.setScan_use_query_mem_ratio(scanUseQueryMemRatio);
        tResult.setEnable_collect_table_level_scan_stats(enableCollectTableLevelScanStats);
        tResult.setEnable_pipeline_level_shuffle(enablePipelineLevelShuffle);
        return tResult;
    }

    public String getJsonString() throws IOException {
        JSONObject root = new JSONObject();
        try {
            for (Field field : SessionVariable.class.getDeclaredFields()) {
                VarAttr attr = field.getAnnotation(VarAttr.class);
                if (attr == null) {
                    continue;
                }
                switch (field.getType().getSimpleName()) {
                    case "boolean":
                    case "int":
                    case "long":
                    case "float":
                    case "double":
                    case "String":
                        root.put(attr.name(), field.get(this));
                        break;
                    default:
                        // Unsupported type variable.
                        throw new IOException("invalid type: " + field.getType().getSimpleName());
                }
            }
        } catch (Exception e) {
            throw new IOException("failed to write session variable: " + e.getMessage());
        }
        return root.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, getJsonString());
    }

    public void readFields(DataInput in) throws IOException {
        readFromJson(in);
    }

    private void readFromJson(DataInput in) throws IOException {
        String json = Text.readString(in);
        replayFromJson(json);
    }

    public void replayFromJson(String json) throws IOException {
        JSONObject root = new JSONObject(json);
        try {
            for (Field field : SessionVariable.class.getDeclaredFields()) {
                VarAttr attr = field.getAnnotation(VarAttr.class);
                if (attr == null) {
                    continue;
                }

                if (!root.has(attr.name())) {
                    continue;
                }
                // Do not restore the session_only variable
                if ((attr.flag() & VariableMgr.SESSION_ONLY) != 0) {
                    continue;
                }

                switch (field.getType().getSimpleName()) {
                    case "boolean":
                        field.set(this, root.getBoolean(attr.name()));
                        break;
                    case "int":
                        field.set(this, root.getInt(attr.name()));
                        break;
                    case "long":
                        field.set(this, root.getLong(attr.name()));
                        break;
                    case "float":
                        field.set(this, root.getFloat(attr.name()));
                        break;
                    case "double":
                        field.set(this, root.getDouble(attr.name()));
                        break;
                    case "String":
                        field.set(this, root.getString(attr.name()));
                        break;
                    default:
                        // Unsupported type variable.
                        throw new IOException("invalid type: " + field.getType().getSimpleName());
                }
            }
        } catch (Exception e) {
            LOG.warn("failed to read session variable: {}", e.getMessage());
        }
    }

    public Map<String, NonDefaultValue> getNonDefaultVariables() {
        Map<String, NonDefaultValue> nonDefaultVariables = Maps.newHashMap();
        Class<SessionVariable> clazz = SessionVariable.class;
        Field[] fields = clazz.getDeclaredFields();
        try {
            for (Field field : fields) {
                VarAttr varAttr = field.getAnnotation(VarAttr.class);
                if (varAttr == null) {
                    continue;
                }
                field.setAccessible(true);

                Object defaultValue = field.get(DEFAULT_SESSION_VARIABLE);
                Object actualValue = field.get(this);
                if (!Objects.equals(defaultValue, actualValue)) {
                    nonDefaultVariables.put(varAttr.name(), new NonDefaultValue(defaultValue, actualValue));
                }
            }
        } catch (IllegalAccessException e) {
            LOG.warn("failed to get non default variables", e);
        }
        return nonDefaultVariables;
    }

    public String getNonDefaultVariablesJson() {
        return GSON.toJson(getNonDefaultVariables());
    }

    public static final class NonDefaultValue {
        public final Object defaultValue;
        public final Object actualValue;

        public NonDefaultValue(Object defaultValue, Object actualValue) {
            this.defaultValue = defaultValue;
            this.actualValue = actualValue;
        }

        public static Map<String, NonDefaultValue> parseFrom(String content) {
            return GSON.fromJson(content, new TypeToken<Map<String, NonDefaultValue>>() {
            }.getType());
        }
    }

    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
}

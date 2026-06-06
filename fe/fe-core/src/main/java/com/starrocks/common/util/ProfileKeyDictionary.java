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

package com.starrocks.common.util;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Global static dictionary for profile string keys (counter names and info-string keys).
 *
 * <p>Pre-assigns a fixed set of universally-common names to integer IDs
 * 0..{@link #STATIC_SIZE}-1.  {@link ProfileSerializer} resolves these names
 * with a single lock-free lookup and never writes them into any per-tree binary
 * header, shrinking every stored {@code ProfileElement.profileContent}.
 *
 * <p>Profile data is kept purely in memory and never persisted; IDs therefore
 * carry no cross-process or cross-version meaning.
 */
public final class ProfileKeyDictionary {
    // --- top-level profile node names ---
    public static final String SUMMARY = "Summary";
    public static final String PLANNER = "Planner";

    // --- Summary info-string keys ---
    public static final String QUERY_ID = "Query ID";
    public static final String START_TIME = "Start Time";
    public static final String END_TIME = "End Time";
    public static final String TOTAL_TIME = "Total";
    public static final String RETRY_TIMES = "Retry Times";
    public static final String QUERY_TYPE = "Query Type";
    public static final String QUERY_STATE = "Query State";
    public static final String SQL_STATEMENT = "Sql Statement";
    public static final String SQL_DIALECT = "Sql Dialect";
    public static final String USER = "User";
    public static final String DEFAULT_DB = "Default Db";
    public static final String VARIABLES = "Variables";
    public static final String PROFILE_COLLECT_TIME = "Collect Profile Time";
    public static final String LOAD_TYPE = "Load Type";
    public static final String WAREHOUSE_CNGROUP = "Warehouse";
    public static final String STARROCKS_VERSION = "StarRocks Version";
    public static final String NON_DEFAULT_SESSION_VARIABLES = "NonDefaultSessionVariables";
    public static final String IS_PROFILE_ASYNC = "IsProfileAsync";
    public static final String HIT_MATERIALIZED_VIEWS = "HitMaterializedViews";
    public static final String EXPLAIN_PLAN = "ExplainPlan";

    // --- Execution info-string keys ---
    public static final String TOPOLOGY = "Topology";

    // --- FE-added query-level counters ---
    public static final String BACKEND_NUM = "BackendNum";
    public static final String INSTANCE_NUM = "InstanceNum";
    public static final String OPERATOR_TOTAL_TIME = "OperatorTotalTime";
    public static final String SCAN_TIME = "ScanTime";
    public static final String NETWORK_TIME = "NetworkTime";
    public static final String RESULT_DELIVER_TIME = "ResultDeliverTime";
    public static final String QUERY_ALLOCATED_MEMORY_USAGE = "QueryAllocatedMemoryUsage";
    public static final String QUERY_DEALLOCATED_MEMORY_USAGE = "QueryDeallocatedMemoryUsage";
    public static final String QUERY_CUMULATIVE_OPERATOR_TIME = "QueryCumulativeOperatorTime";
    public static final String QUERY_CUMULATIVE_SCAN_TIME = "QueryCumulativeScanTime";
    public static final String QUERY_CUMULATIVE_NETWORK_TIME = "QueryCumulativeNetworkTime";
    public static final String QUERY_PEAK_SCHEDULE_TIME = "QueryPeakScheduleTime";
    public static final String QUERY_CUMULATIVE_CPU_TIME = "QueryCumulativeCpuTime";
    public static final String QUERY_PEAK_MEMORY_USAGE_PER_NODE = "QueryPeakMemoryUsagePerNode";
    public static final String QUERY_SUM_MEMORY_USAGE = "QuerySumMemoryUsage";
    public static final String QUERY_EXECUTION_WALL_TIME = "QueryExecutionWallTime";
    public static final String QUERY_SPILL_BYTES = "QuerySpillBytes";
    public static final String FRONTEND_PROFILE_MERGE_TIME = "FrontendProfileMergeTime";

    // --- common pipeline / scheduler counters (from BE Thrift) ---
    public static final String SCHEDULE_TIME = "ScheduleTime";
    public static final String OUTPUT_FULL_TIME = "OutputFullTime";
    public static final String PENDING_FINISH_TIME = "PendingFinishTime";
    public static final String CHANNEL_NUM = "ChannelNum";

    // --- CommonMetrics (present in every pipeline operator) ---
    public static final String OUTPUT_CHUNK_BYTES = "OutputChunkBytes";
    public static final String PULL_CHUNK_NUM = "PullChunkNum";
    public static final String PULL_ROW_NUM = "PullRowNum";
    public static final String PULL_TOTAL_TIME = "PullTotalTime";
    public static final String PUSH_CHUNK_NUM = "PushChunkNum";
    public static final String PUSH_ROW_NUM = "PushRowNum";
    public static final String PUSH_TOTAL_TIME = "PushTotalTime";
    public static final String RUNTIME_FILTER_NUM = "RuntimeFilterNum";
    public static final String RUNTIME_IN_FILTER_NUM = "RuntimeInFilterNum";
    public static final String IS_SUBORDINATE = "IsSubordinate";
    public static final String IS_FINAL_SINK = "IsFinalSink";
    public static final String RUNTIME_FILTER_DESC = "RuntimeFilterDesc";
    public static final String ACTIVE_TIME = "ActiveTime";
    public static final String BLOCK_BY_INPUT_EMPTY = "BlockByInputEmpty";
    public static final String BLOCK_BY_OUTPUT_FULL = "BlockByOutputFull";
    public static final String BLOCK_BY_PRECONDITION = "BlockByPrecondition";
    public static final String DEGREE_OF_PARALLELISM = "DegreeOfParallelism";
    public static final String TOTAL_DEGREE_OF_PARALLELISM = "TotalDegreeOfParallelism";
    public static final String DRIVER_TOTAL_TIME = "DriverTotalTime";
    public static final String IS_GROUP_EXECUTION = "IsGroupExecution";
    public static final String PEAK_DRIVER_QUEUE_SIZE = "PeakDriverQueueSize";
    public static final String SCHEDULE_COUNT = "ScheduleCount";
    public static final String YIELD_BY_LOCAL_WAIT = "YieldByLocalWait";
    public static final String YIELD_BY_PREEMPT = "YieldByPreempt";
    public static final String YIELD_BY_TIME_LIMIT = "YieldByTimeLimit";
    public static final String PENDING_TIME = "PendingTime";
    public static final String INPUT_EMPTY_TIME = "InputEmptyTime";
    public static final String FIRST_INPUT_EMPTY_TIME = "FirstInputEmptyTime";
    public static final String FOLLOWUP_INPUT_EMPTY_TIME = "FollowupInputEmptyTime";
    public static final String PRECONDITION_BLOCK_TIME = "PreconditionBlockTime";
    public static final String WAIT_TIME = "WaitTime";

    // --- join operator metrics ---
    public static final String JOIN_RUNTIME_FILTER_TIME = "JoinRuntimeFilterTime";
    public static final String JOIN_RUNTIME_FILTER_OUTPUT_ROWS = "JoinRuntimeFilterOutputRows";
    public static final String JOIN_RUNTIME_FILTER_INPUT_ROWS = "JoinRuntimeFilterInputRows";
    public static final String JOIN_RUNTIME_FILTER_HASH_TIME = "JoinRuntimeFilterHashTime";
    public static final String JOIN_RUNTIME_FILTER_EVALUATE = "JoinRuntimeFilterEvaluate";
    public static final String RUNTIME_FILTER_EVAL_TIME = "RuntimeFilterEvalTime";
    public static final String RUNTIME_FILTER_INPUT_ROWS = "RuntimeFilterInputRows";
    public static final String RUNTIME_FILTER_OUTPUT_ROWS = "RuntimeFilterOutputRows";
    public static final String JOIN_TYPE = "JoinType";
    public static final String DISTRIBUTION_MODE = "DistributionMode";

    // --- exchange operator metrics ---
    public static final String BYTES_PASS_THROUGH = "BytesPassThrough";
    public static final String SERIALIZED_BYTES = "SerializedBytes";
    public static final String SERIALIZE_CHUNK_TIME = "SerializeChunkTime";
    public static final String DESERIALIZE_CHUNK_TIME = "DeserializeChunkTime";
    public static final String NETWORK_BANDWIDTH = "NetworkBandwidth";
    public static final String RPC_COUNT = "RpcCount";
    public static final String RPC_AVG_TIME = "RpcAvgTime";
    public static final String REQUEST_SENT = "RequestSent";
    public static final String REQUEST_RECEIVED = "RequestReceived";
    public static final String REQUEST_UNSENT = "RequestUnsent";
    public static final String RECEIVER_PROCESS_TOTAL_TIME = "ReceiverProcessTotalTime";
    public static final String SHUFFLE_NUM = "ShuffleNum";
    public static final String SHUFFLE_HASH_TIME = "ShuffleHashTime";
    public static final String SHUFFLE_CHUNK_APPEND_TIME = "ShuffleChunkAppendTime";
    public static final String SHUFFLE_CHUNK_APPEND_COUNTER = "ShuffleChunkAppendCounter";
    public static final String OVERALL_TIME = "OverallTime";
    public static final String OVERALL_THROUGHPUT = "OverallThroughput";
    public static final String WAIT_LOCK_TIME = "WaitLockTime";
    public static final String PART_TYPE = "PartType";
    public static final String DEST_ID = "DestID";
    public static final String DEST_FRAGMENTS = "DestFragments";
    public static final String PEAK_BUFFER_MEMORY_BYTES = "PeakBufferMemoryBytes";
    public static final String PASS_THROUGH_BUFFER_PEAK_MEMORY_USAGE = "PassThroughBufferPeakMemoryUsage";
    public static final String LOCAL_EXCHANGE_PEAK_MEMORY_USAGE = "LocalExchangePeakMemoryUsage";

    // --- scan operator metrics ---
    public static final String RAW_ROWS_READ = "RawRowsRead";
    public static final String ROWS_READ = "RowsRead";
    public static final String READ_PAGES_NUM = "ReadPagesNum";
    public static final String UNCOMPRESSED_BYTES_READ = "UncompressedBytesRead";
    public static final String BLOCK_FETCH = "BlockFetch";
    public static final String BLOCK_FETCH_COUNT = "BlockFetchCount";
    public static final String BLOCK_SEEK = "BlockSeek";
    public static final String BLOCK_SEEK_COUNT = "BlockSeekCount";
    public static final String SEGMENT_READ = "SegmentRead";
    public static final String DECOMPRESS_T = "DecompressT";
    public static final String CHUNK_COPY = "ChunkCopy";
    public static final String PRED_FILTER = "PredFilter";
    public static final String PRED_FILTER_ROWS = "PredFilterRows";
    public static final String ZONE_MAP_INDEX_FILTER_ROWS = "ZoneMapIndexFilterRows";
    public static final String ZONE_MAP_INDEX_FITER = "ZoneMapIndexFiter";
    public static final String SHORT_KEY_FILTER_ROWS = "ShortKeyFilterRows";
    public static final String SHORT_KEY_RANGE_NUMBER = "ShortKeyRangeNumber";
    public static final String BITMAP_INDEX_FILTER_ROWS = "BitmapIndexFilterRows";
    public static final String BLOOM_FILTER_ROWS = "BloomFilterRows";
    public static final String VECTOR_INDEX_FILTER_ROWS = "VectorIndexFilterRows";
    public static final String VECTOR_SEARCH_TIME = "VectorSearchTime";
    public static final String ROWSETS_READ_COUNT = "RowsetsReadCount";
    public static final String SEGMENTS_READ_COUNT = "SegmentsReadCount";
    public static final String TOTAL_COLUMNS_DATA_PAGE_COUNT = "TotalColumnsDataPageCount";
    public static final String TABLET_COUNT = "TabletCount";
    public static final String MORSELS_COUNT = "MorselsCount";
    public static final String PUSHDOWN_PREDICATES = "PushdownPredicates";
    public static final String NON_PUSHDOWN_PREDICATES = "NonPushdownPredicates";
    public static final String PUSHDOWN_ACCESS_PATHS = "PushdownAccessPaths";
    public static final String PREPARE_CHUNK_SOURCE_TIME = "PrepareChunkSourceTime";
    public static final String SUBMIT_TASK_COUNT = "SubmitTaskCount";
    public static final String SUBMIT_TASK_TIME = "SubmitTaskTime";
    public static final String IO_TASK_WAIT_TIME = "IOTaskWaitTime";
    public static final String PEAK_IO_TASKS = "PeakIOTasks";
    public static final String PEAK_SCAN_TASK_QUEUE_SIZE = "PeakScanTaskQueueSize";
    public static final String PEAK_CHUNK_BUFFER_MEMORY_USAGE = "PeakChunkBufferMemoryUsage";
    public static final String PEAK_CHUNK_BUFFER_SIZE = "PeakChunkBufferSize";
    public static final String DEL_VEC_FILTER_ROWS = "DelVecFilterRows";
    public static final String CAPTURE_TABLET_ROWSETS_TIME = "CaptureTabletRowsetsTime";

    // --- aggregation / computation metrics ---
    public static final String HASH_TABLE_MEMORY_USAGE = "HashTableMemoryUsage";
    public static final String EXPR_COMPUTE_TIME = "ExprComputeTime";
    public static final String COMMON_SUB_EXPR_COMPUTE_TIME = "CommonSubExprComputeTime";

    // --- operator type info strings ---
    public static final String TYPE = "Type";

    // --- fragment-level metrics ---
    public static final String BACKEND_ADDRESSES = "BackendAddresses";
    public static final String BACKEND_PROFILE_MERGE_TIME = "BackendProfileMergeTime";
    public static final String INITIAL_PROCESS_DRIVER_COUNT = "InitialProcessDriverCount";
    public static final String INITIAL_PROCESS_MEM = "InitialProcessMem";
    public static final String INSTANCE_ALLOCATED_MEMORY_USAGE = "InstanceAllocatedMemoryUsage";
    public static final String INSTANCE_DEALLOCATED_MEMORY_USAGE = "InstanceDeallocatedMemoryUsage";
    public static final String INSTANCE_IDS = "InstanceIds";
    public static final String INSTANCE_PEAK_MEMORY_USAGE = "InstancePeakMemoryUsage";
    public static final String JIT_COUNTER = "JITCounter";
    public static final String JIT_TOTAL_COST_TIME = "JITTotalCostTime";
    public static final String QUERY_MEMORY_LIMIT = "QueryMemoryLimit";

    /**
     * All globally-known string keys. Index in this array == static ID used by {@link ProfileSerializer}.
     */
    static final String[] STATIC_NAMES = {
            // --- universally present in every profile node ---
            RuntimeProfile.ROOT_COUNTER,
            RuntimeProfile.TOTAL_TIME_COUNTER,

            // --- FE-added query-level counters ---
            BACKEND_NUM,
            INSTANCE_NUM,
            OPERATOR_TOTAL_TIME,
            SCAN_TIME,
            NETWORK_TIME,
            RESULT_DELIVER_TIME,
            QUERY_ALLOCATED_MEMORY_USAGE,
            QUERY_DEALLOCATED_MEMORY_USAGE,
            QUERY_CUMULATIVE_OPERATOR_TIME,
            QUERY_CUMULATIVE_SCAN_TIME,
            QUERY_CUMULATIVE_NETWORK_TIME,
            QUERY_PEAK_SCHEDULE_TIME,
            QUERY_CUMULATIVE_CPU_TIME,
            QUERY_PEAK_MEMORY_USAGE_PER_NODE,
            QUERY_SUM_MEMORY_USAGE,
            QUERY_EXECUTION_WALL_TIME,
            QUERY_SPILL_BYTES,
            FRONTEND_PROFILE_MERGE_TIME,

            // --- common pipeline / scheduler counters ---
            SCHEDULE_TIME,
            OUTPUT_FULL_TIME,
            PENDING_FINISH_TIME,
            CHANNEL_NUM,

            // --- Summary info-string keys ---
            QUERY_ID,
            START_TIME,
            END_TIME,
            TOTAL_TIME,
            RETRY_TIMES,
            QUERY_TYPE,
            QUERY_STATE,
            SQL_STATEMENT,
            SQL_DIALECT,
            USER,
            DEFAULT_DB,
            VARIABLES,
            PROFILE_COLLECT_TIME,
            LOAD_TYPE,
            WAREHOUSE_CNGROUP,
            STARROCKS_VERSION,
            NON_DEFAULT_SESSION_VARIABLES,
            IS_PROFILE_ASYNC,
            HIT_MATERIALIZED_VIEWS,
            EXPLAIN_PLAN,

            // --- Execution info-string keys ---
            TOPOLOGY,

            // --- CommonMetrics (pipeline operator) ---
            OUTPUT_CHUNK_BYTES,
            PULL_CHUNK_NUM,
            PULL_ROW_NUM,
            PULL_TOTAL_TIME,
            PUSH_CHUNK_NUM,
            PUSH_ROW_NUM,
            PUSH_TOTAL_TIME,
            RUNTIME_FILTER_NUM,
            RUNTIME_IN_FILTER_NUM,
            IS_SUBORDINATE,
            IS_FINAL_SINK,
            RUNTIME_FILTER_DESC,
            ACTIVE_TIME,
            BLOCK_BY_INPUT_EMPTY,
            BLOCK_BY_OUTPUT_FULL,
            BLOCK_BY_PRECONDITION,
            DEGREE_OF_PARALLELISM,
            TOTAL_DEGREE_OF_PARALLELISM,
            DRIVER_TOTAL_TIME,
            IS_GROUP_EXECUTION,
            PEAK_DRIVER_QUEUE_SIZE,
            SCHEDULE_COUNT,
            YIELD_BY_LOCAL_WAIT,
            YIELD_BY_PREEMPT,
            YIELD_BY_TIME_LIMIT,
            PENDING_TIME,
            INPUT_EMPTY_TIME,
            FIRST_INPUT_EMPTY_TIME,
            FOLLOWUP_INPUT_EMPTY_TIME,
            PRECONDITION_BLOCK_TIME,
            WAIT_TIME,

            // --- join operator ---
            JOIN_RUNTIME_FILTER_TIME,
            JOIN_RUNTIME_FILTER_OUTPUT_ROWS,
            JOIN_RUNTIME_FILTER_INPUT_ROWS,
            JOIN_RUNTIME_FILTER_HASH_TIME,
            JOIN_RUNTIME_FILTER_EVALUATE,
            RUNTIME_FILTER_EVAL_TIME,
            RUNTIME_FILTER_INPUT_ROWS,
            RUNTIME_FILTER_OUTPUT_ROWS,
            JOIN_TYPE,
            DISTRIBUTION_MODE,

            // --- exchange operator ---
            BYTES_PASS_THROUGH,
            SERIALIZED_BYTES,
            SERIALIZE_CHUNK_TIME,
            DESERIALIZE_CHUNK_TIME,
            NETWORK_BANDWIDTH,
            RPC_COUNT,
            RPC_AVG_TIME,
            REQUEST_SENT,
            REQUEST_RECEIVED,
            REQUEST_UNSENT,
            RECEIVER_PROCESS_TOTAL_TIME,
            SHUFFLE_NUM,
            SHUFFLE_HASH_TIME,
            SHUFFLE_CHUNK_APPEND_TIME,
            SHUFFLE_CHUNK_APPEND_COUNTER,
            OVERALL_TIME,
            OVERALL_THROUGHPUT,
            WAIT_LOCK_TIME,
            PART_TYPE,
            DEST_ID,
            DEST_FRAGMENTS,
            PEAK_BUFFER_MEMORY_BYTES,
            PASS_THROUGH_BUFFER_PEAK_MEMORY_USAGE,
            LOCAL_EXCHANGE_PEAK_MEMORY_USAGE,

            // --- scan operator ---
            RAW_ROWS_READ,
            ROWS_READ,
            READ_PAGES_NUM,
            UNCOMPRESSED_BYTES_READ,
            BLOCK_FETCH,
            BLOCK_FETCH_COUNT,
            BLOCK_SEEK,
            BLOCK_SEEK_COUNT,
            SEGMENT_READ,
            DECOMPRESS_T,
            CHUNK_COPY,
            PRED_FILTER,
            PRED_FILTER_ROWS,
            ZONE_MAP_INDEX_FILTER_ROWS,
            ZONE_MAP_INDEX_FITER,
            SHORT_KEY_FILTER_ROWS,
            SHORT_KEY_RANGE_NUMBER,
            BITMAP_INDEX_FILTER_ROWS,
            BLOOM_FILTER_ROWS,
            VECTOR_INDEX_FILTER_ROWS,
            VECTOR_SEARCH_TIME,
            ROWSETS_READ_COUNT,
            SEGMENTS_READ_COUNT,
            TOTAL_COLUMNS_DATA_PAGE_COUNT,
            TABLET_COUNT,
            MORSELS_COUNT,
            PUSHDOWN_PREDICATES,
            NON_PUSHDOWN_PREDICATES,
            PUSHDOWN_ACCESS_PATHS,
            PREPARE_CHUNK_SOURCE_TIME,
            SUBMIT_TASK_COUNT,
            SUBMIT_TASK_TIME,
            IO_TASK_WAIT_TIME,
            PEAK_IO_TASKS,
            PEAK_SCAN_TASK_QUEUE_SIZE,
            PEAK_CHUNK_BUFFER_MEMORY_USAGE,
            PEAK_CHUNK_BUFFER_SIZE,
            DEL_VEC_FILTER_ROWS,
            CAPTURE_TABLET_ROWSETS_TIME,

            // --- aggregation / computation ---
            HASH_TABLE_MEMORY_USAGE,
            EXPR_COMPUTE_TIME,
            COMMON_SUB_EXPR_COMPUTE_TIME,

            // --- operator type info string ---
            TYPE,

            // --- fragment-level ---
            BACKEND_ADDRESSES,
            BACKEND_PROFILE_MERGE_TIME,
            INITIAL_PROCESS_DRIVER_COUNT,
            INITIAL_PROCESS_MEM,
            INSTANCE_ALLOCATED_MEMORY_USAGE,
            INSTANCE_DEALLOCATED_MEMORY_USAGE,
            INSTANCE_IDS,
            INSTANCE_PEAK_MEMORY_USAGE,
            JIT_COUNTER,
            JIT_TOTAL_COST_TIME,
            QUERY_MEMORY_LIMIT,
    };

    /**
     * Number of entries in the static dictionary.
     */
    static final int STATIC_SIZE = STATIC_NAMES.length;

    /**
     * Forward lookup for all three static zones (size = 3 × {@link #STATIC_SIZE}):
     * <pre>
     *   [0 .. S-1]   base names   (zone 0)
     *   [S .. 2S-1]  __MIN_OF_... (zone 1)
     *   [2S .. 3S-1] __MAX_OF_... (zone 2)
     * </pre>
     * Use {@code STATIC_ALL.get(id)} to resolve any static-zone nameId to its string.
     */
    static final List<String> STATIC_ALL;

    /**
     * Sentinel returned when a name is not found.
     * Matches the {@link Object2IntOpenHashMap} default return value.
     */
    static final int ABSENT = -1;

    /**
     * Lock-free reverse map: name → static ID (covers all three zones).
     * Initialised once at class-load; never mutated afterwards.
     */
    private static final Object2IntOpenHashMap<String> STATIC_MAP;

    static {
        ArrayList<String> all = new ArrayList<>(STATIC_SIZE * 3);
        Object2IntOpenHashMap<String> map = new Object2IntOpenHashMap<>(STATIC_SIZE * 6);
        map.defaultReturnValue(ABSENT);
        // Zone 0: base names
        for (int i = 0; i < STATIC_SIZE; i++) {
            all.add(STATIC_NAMES[i]);
            map.put(STATIC_NAMES[i], i);
        }
        // Zone 1: __MIN_OF_ prefixed
        for (int i = 0; i < STATIC_SIZE; i++) {
            String minName = RuntimeProfile.MERGED_INFO_PREFIX_MIN + STATIC_NAMES[i];
            all.add(minName);
            map.put(minName, STATIC_SIZE + i);
        }
        // Zone 2: __MAX_OF_ prefixed
        for (int i = 0; i < STATIC_SIZE; i++) {
            String maxName = RuntimeProfile.MERGED_INFO_PREFIX_MAX + STATIC_NAMES[i];
            all.add(maxName);
            map.put(maxName, 2 * STATIC_SIZE + i);
        }
        STATIC_ALL = Collections.unmodifiableList(all);
        STATIC_MAP = map;
    }

    // -- public API -----------------------------------------------------------

    /**
     * Returns the static ID for {@code name}, or {@link #ABSENT} if the name
     * is not in the dictionary.
     *
     * <p>Lock-free; safe on any hot path.</p>
     */
    static int staticId(String name) {
        return STATIC_MAP.getInt(name);
    }

    /**
     * Returns the name for static {@code id} (0..{@link #STATIC_SIZE}-1),
     * or {@code null} if {@code id} is out of range.
     */
    static String nameOf(int id) {
        if (id >= 0 && id < STATIC_SIZE) {
            return STATIC_NAMES[id];
        }
        return null;
    }

    private ProfileKeyDictionary() {
    }
}

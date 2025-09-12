// Copyright 2025-present StarRocks, Inc. All rights reserved.
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

import java.util.HashMap;
import java.util.Map;

/**
 * Maps internal metric names to user-friendly names for better readability.
 * Implements requirements from issue #60952 to eliminate confusing internal terminology.
 */
public class MetricNameMapper {
    
    private static final Map<String, String> FRIENDLY_NAME_MAP = new HashMap<>();
    
    static {
        initializeFriendlyNames();
    }
    
    /**
     * Get user-friendly name for a metric, or return original name if no mapping exists
     */
    public static String getFriendlyName(String metricName) {
        return FRIENDLY_NAME_MAP.getOrDefault(metricName, metricName);
    }
    
    /**
     * Check if a metric has a user-friendly name mapping
     */
    public static boolean hasFriendlyName(String metricName) {
        return FRIENDLY_NAME_MAP.containsKey(metricName);
    }
    
    private static void initializeFriendlyNames() {
        // Data processing metrics
        FRIENDLY_NAME_MAP.put("PullChunkNum", "Data Chunks Processed");
        FRIENDLY_NAME_MAP.put("PushChunkNum", "Data Chunks Sent");
        FRIENDLY_NAME_MAP.put("QuerySpillBytes", "Data Spilled to Disk");
        FRIENDLY_NAME_MAP.put("QueryPeakMemoryUsagePerNode", "Peak Memory per Node");
        FRIENDLY_NAME_MAP.put("QueryCumulativeCpuTime", "Total CPU Time");
        FRIENDLY_NAME_MAP.put("QueryExecutionWallTime", "Query Duration");
        
        // Row processing metrics
        FRIENDLY_NAME_MAP.put("RawRowsRead", "Rows Scanned");
        FRIENDLY_NAME_MAP.put("RowsRead", "Rows Returned (after filters)");
        FRIENDLY_NAME_MAP.put("ExprFilterRows", "Rows Filtered by Expression");
        FRIENDLY_NAME_MAP.put("probeCount", "Hash Table Probes");
        
        // Hash table metrics
        FRIENDLY_NAME_MAP.put("HashTableMemoryUsage", "Join Memory Usage");
        FRIENDLY_NAME_MAP.put("BuildHashTableTime", "Hash Table Build Time");
        FRIENDLY_NAME_MAP.put("SearchHashTableTime", "Hash Table Lookup Time");
        FRIENDLY_NAME_MAP.put("BuildBuckets", "Hash Table Buckets");
        FRIENDLY_NAME_MAP.put("BuildKeysPerBucket%", "Hash Table Load Factor %");
        FRIENDLY_NAME_MAP.put("HashTableSize", "Hash Table Entry Count");
        
        // I/O and serialization metrics
        FRIENDLY_NAME_MAP.put("SerializeChunkTime", "Data Serialization Time");
        FRIENDLY_NAME_MAP.put("DeserializeChunkTime", "Data Deserialization Time");
        FRIENDLY_NAME_MAP.put("CompressedBytesRead", "Compressed Data Read");
        FRIENDLY_NAME_MAP.put("UncompressedBytesRead", "Uncompressed Data Read");
        FRIENDLY_NAME_MAP.put("MaterializeTupleTime(*)", "Data Materialization Time");
        FRIENDLY_NAME_MAP.put("TotalRawReadTime(*)", "Raw Data Read Time");
        FRIENDLY_NAME_MAP.put("BytesRead", "Total Data Read");
        
        // Buffer and pipeline metrics
        FRIENDLY_NAME_MAP.put("BufferUnplugCount", "Buffer Overflow Events");
        FRIENDLY_NAME_MAP.put("PeakChunkBufferMemoryUsage", "Peak Buffer Memory");
        FRIENDLY_NAME_MAP.put("ChunkBufferCapacity", "Buffer Capacity");
        
        // Timing metrics
        FRIENDLY_NAME_MAP.put("PrepareTime", "Operator Preparation Time");
        FRIENDLY_NAME_MAP.put("CloseTime", "Operator Cleanup Time");
        FRIENDLY_NAME_MAP.put("SetFinishingTime", "Operator Finishing Time");
        FRIENDLY_NAME_MAP.put("OperatorTotalTime", "Operator Execution Time");
        FRIENDLY_NAME_MAP.put("ScanTime", "Data Scan Time");
        FRIENDLY_NAME_MAP.put("TotalTime", "Total Execution Time");
        
        // Runtime filter metrics
        FRIENDLY_NAME_MAP.put("JoinRuntimeFilterTime", "Runtime Filter Build Time");
        FRIENDLY_NAME_MAP.put("JoinRuntimeFilterHashTime", "Runtime Filter Hash Time");
        FRIENDLY_NAME_MAP.put("JoinRuntimeFilterEvaluate", "Runtime Filter Evaluations");
        FRIENDLY_NAME_MAP.put("JoinRuntimeFilterInputScanRanges", "Runtime Filter Input Ranges");
        FRIENDLY_NAME_MAP.put("JoinRuntimeFilterOutputScanRanges", "Runtime Filter Output Ranges");
        FRIENDLY_NAME_MAP.put("RuntimeFilterBuildTime", "Runtime Filter Construction Time");
        FRIENDLY_NAME_MAP.put("RuntimeFilterNum", "Runtime Filter Count");
        
        // Network metrics
        FRIENDLY_NAME_MAP.put("NetworkTime", "Network Transfer Time");
        FRIENDLY_NAME_MAP.put("NetworkBandwidth", "Network Bandwidth");
        
        // File operations metrics
        FRIENDLY_NAME_MAP.put("IOTaskExecTime", "I/O Execution Time");
        FRIENDLY_NAME_MAP.put("IOTaskWaitTime", "I/O Wait Time");
        FRIENDLY_NAME_MAP.put("FileWriterCloseTime", "File Writer Close Time");
        
        // Access pattern metrics
        FRIENDLY_NAME_MAP.put("AccessPathHits", "Index Access Hits");
        FRIENDLY_NAME_MAP.put("AccessPathUnhits", "Index Access Misses");
        FRIENDLY_NAME_MAP.put("NonPushdownPredicates", "Non-Pushdown Filters");
        FRIENDLY_NAME_MAP.put("PushdownPredicates", "Pushdown Filters");
        FRIENDLY_NAME_MAP.put("PushdownAccessPaths", "Pushdown Access Paths");
        
        // Join processing metrics
        FRIENDLY_NAME_MAP.put("OutputBuildColumnTime", "Build Side Output Time");
        FRIENDLY_NAME_MAP.put("OutputProbeColumnTime", "Probe Side Output Time");
        FRIENDLY_NAME_MAP.put("ProbeConjunctEvaluateTime", "Probe Condition Evaluation Time");
        FRIENDLY_NAME_MAP.put("OtherJoinConjunctEvaluateTime", "Join Condition Evaluation Time");
        FRIENDLY_NAME_MAP.put("WhereConjunctEvaluateTime", "Where Condition Evaluation Time");
        FRIENDLY_NAME_MAP.put("BuildConjunctEvaluateTime", "Build Condition Evaluation Time");
        FRIENDLY_NAME_MAP.put("CopyRightTableChunkTime", "Right Table Copy Time");
        
        // Scanner and connector metrics
        FRIENDLY_NAME_MAP.put("ScannerTotalTime", "Scanner Total Time");
        FRIENDLY_NAME_MAP.put("ReadCounter", "Read Operation Count");
        FRIENDLY_NAME_MAP.put("CreateSegmentIter", "Segment Iterator Creation Time");
        FRIENDLY_NAME_MAP.put("GetDelVec", "Delete Vector Retrieval Time");
        FRIENDLY_NAME_MAP.put("ExprFilterTime", "Expression Filter Time");
        
        // Partition metrics
        FRIENDLY_NAME_MAP.put("PartitionNums", "Partition Count");
        FRIENDLY_NAME_MAP.put("PartitionProbeOverhead", "Partition Probe Overhead");
        
        // Pipeline metrics (developer/trace tier)
        FRIENDLY_NAME_MAP.put("DriverPrepareTime", "Driver Preparation Time");
        FRIENDLY_NAME_MAP.put("FragmentInstancePrepareTime", "Fragment Preparation Time");
        FRIENDLY_NAME_MAP.put("prepare-query-ctx", "Query Context Preparation");
        FRIENDLY_NAME_MAP.put("prepare-fragment-ctx", "Fragment Context Preparation");
        FRIENDLY_NAME_MAP.put("prepare-runtime-state", "Runtime State Preparation");
        FRIENDLY_NAME_MAP.put("PullTotalTime", "Total Pull Time");
        FRIENDLY_NAME_MAP.put("PushTotalTime", "Total Push Time");
        
        // Column processing metrics (developer/trace tier)
        FRIENDLY_NAME_MAP.put("ColumnResizeTime", "Column Resize Time");
        FRIENDLY_NAME_MAP.put("RemoveUnusedRowsCount", "Unused Rows Removed");
        
        // Stage metrics (developer/trace tier)
        FRIENDLY_NAME_MAP.put("DeploySerializeConcurrencyTime", "Serialize Concurrency Deploy Time");
        FRIENDLY_NAME_MAP.put("DeployStageByStageTime", "Stage-by-Stage Deploy Time");
        
        // Runtime filter detailed metrics
        FRIENDLY_NAME_MAP.put("PartialRuntimeMembershipFilterBytes", "Partial Runtime Filter Size");
    }
}
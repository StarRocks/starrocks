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

#include "util/metric_categorizer.h"

namespace starrocks {

// Static member initialization following StarRocks patterns
bool RuntimeProfileMetricHelper::initialized = false;

// NOLINTNEXTLINE
const std::unordered_set<std::string> RuntimeProfileMetricHelper::BASIC_METRICS = {
    // Query-level basic metrics (top priority)
    "TotalTime", "QueryExecutionWallTime", "QueryPeakMemoryUsagePerNode", 
    "QuerySpillBytes", "QueryCumulativeCpuTime",
    
    // Scan operators - basic metrics
    "RowsRead", "BytesRead", "ScanTime",
    
    // Join operators - basic metrics  
    "BuildHashTableTime", "SearchHashTableTime", "HashTableMemoryUsage", "probeCount",
    
    // Aggregation operators - basic metrics
    "HashTableSize", "OperatorTotalTime",
    
    // Network/Exchange operators - basic metrics
    "NetworkTime", "SerializeChunkTime"
};

// NOLINTNEXTLINE
const std::unordered_set<std::string> RuntimeProfileMetricHelper::TRACE_METRICS = {
    // Lifecycle timing (developers only)
    "PrepareTime", "CloseTime", "SetFinishingTime", "SetFinishedTime",
    
    // Buffer management (developers only)
    "BufferUnplugCount", "PullChunkNum", "PushChunkNum", "PullTotalTime", "PushTotalTime",
    
    // Detailed pipeline timing (developers only)
    "DriverPrepareTime", "FragmentInstancePrepareTime", "prepare-query-ctx",
    "prepare-fragment-ctx", "prepare-runtime-state",
    
    // Column processing details (developers only)
    "ColumnResizeTime", "RemoveUnusedRowsCount",
    
    // Stage details (developers only)
    "DeploySerializeConcurrencyTime", "DeployStageByStageTime",
    
    // File operations timing (developers only)
    "FileWriterCloseTime", "GetDelVec",
    
    // Partition details (developers only)
    "PartitionNums", "PartitionProbeOverhead",
    
    // Access path details (developers only)
    "AccessPathHits", "AccessPathUnhits", "NonPushdownPredicates", 
    "PushdownPredicates", "PushdownAccessPaths",
    
    // Scanner details (developers only)
    "ScannerTotalTime", "ReadCounter"
};

// NOLINTNEXTLINE  
const std::unordered_map<std::string, std::string> RuntimeProfileMetricHelper::FRIENDLY_NAMES = {
    // Data processing
    {"PullChunkNum", "Data Chunks Processed"},
    {"QuerySpillBytes", "Data Spilled to Disk"},
    {"QueryPeakMemoryUsagePerNode", "Peak Memory per Node"},
    {"QueryCumulativeCpuTime", "Total CPU Time"},
    {"QueryExecutionWallTime", "Query Duration"},
    
    // Row processing
    {"RawRowsRead", "Rows Scanned"},
    {"RowsRead", "Rows Returned (after filters)"},
    {"probeCount", "Hash Table Probes"},
    
    // Hash table
    {"HashTableMemoryUsage", "Join Memory Usage"},
    {"BuildHashTableTime", "Hash Table Build Time"},
    {"SearchHashTableTime", "Hash Table Lookup Time"},
    
    // I/O and serialization
    {"SerializeChunkTime", "Data Serialization Time"},
    {"NetworkTime", "Network Transfer Time"},
    {"ScanTime", "Data Scan Time"}
};

// NOLINTNEXTLINE
const std::unordered_set<std::string> RuntimeProfileMetricHelper::ALWAYS_DISPLAY_ZERO = {
    "TotalTime", "QueryExecutionWallTime", "QueryPeakMemoryUsagePerNode", 
    "RowsRead", "OperatorTotalTime"
};

bool RuntimeProfileMetricHelper::is_basic_metric(const std::string& metric_name) {
    return BASIC_METRICS.count(metric_name) > 0;
}

bool RuntimeProfileMetricHelper::is_trace_metric(const std::string& metric_name) {
    return TRACE_METRICS.count(metric_name) > 0;
}

std::string RuntimeProfileMetricHelper::get_friendly_name(const std::string& metric_name) {
    auto it = FRIENDLY_NAMES.find(metric_name);
    if (it != FRIENDLY_NAMES.end()) {
        return it->second;
    }
    return metric_name; // Return original name if no friendly name exists
}

bool RuntimeProfileMetricHelper::always_display_when_zero(const std::string& metric_name) {
    return ALWAYS_DISPLAY_ZERO.count(metric_name) > 0;
}

} // namespace starrocks

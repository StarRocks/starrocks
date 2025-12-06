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

#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace starrocks::lake {

// Log Type
enum class LogType : uint8_t {
    LOAD = 1,       // Data Load (Transaction Commit)
    COMPACTION = 2  // Compaction
};

// Single log entry
struct TabletWriteLogEntry {
    int64_t begin_time;         // Begin timestamp (ms)
    int64_t finish_time;        // Finish timestamp (ms)
    int64_t backend_id;         // Backend ID
    int64_t txn_id;             // Transaction ID
    int64_t tablet_id;          // Tablet ID
    int64_t table_id;           // Table ID
    int64_t partition_id;       // Partition ID
    LogType log_type;           // Log Type
    int64_t input_rows;         // Input rows
    int64_t input_bytes;        // Input bytes
    int64_t output_rows;        // Output rows
    int64_t output_bytes;       // Output bytes
    int32_t input_segments;     // Input segment count (For Compaction)
    int32_t output_segments;    // Output segment count
    std::string label;          // Load label (For Load)
    int64_t compaction_score;   // Compaction score (For Compaction)
    std::string compaction_type; // Compaction type (For Compaction)

    TabletWriteLogEntry()
            : begin_time(0),
              finish_time(0),
              backend_id(0),
              txn_id(0),
              tablet_id(0),
              table_id(0),
              partition_id(0),
              log_type(LogType::LOAD),
              input_rows(0),
              input_bytes(0),
              output_rows(0),
              output_bytes(0),
              input_segments(0),
              output_segments(0),
              compaction_score(0) {}
};

// TabletWriteLogManager: Manages write logs in memory
// Uses a ring buffer to keep recent log entries (default 30 minutes)
class TabletWriteLogManager {
public:
    TabletWriteLogManager();
    ~TabletWriteLogManager() = default;

    // Disable copy
    TabletWriteLogManager(const TabletWriteLogManager&) = delete;
    TabletWriteLogManager& operator=(const TabletWriteLogManager&) = delete;

    // Record Load log
    void add_load_log(int64_t backend_id, int64_t txn_id, int64_t tablet_id, int64_t table_id, int64_t partition_id,
                      int64_t input_rows, int64_t input_bytes, int64_t output_rows, int64_t output_bytes,
                      int32_t output_segments, const std::string& label, int64_t begin_time, int64_t finish_time);

    // Record Compaction log
    void add_compaction_log(int64_t backend_id, int64_t txn_id, int64_t tablet_id, int64_t table_id, int64_t partition_id,
                            int64_t input_rows, int64_t input_bytes, int64_t output_rows, int64_t output_bytes,
                            int32_t input_segments, int32_t output_segments, int64_t compaction_score,
                            const std::string& compaction_type, int64_t begin_time, int64_t finish_time);

    // Get logs (For SchemaScanner query)
    // Optional filters: table_id, partition_id, tablet_id, log_type, start_finish_time, end_finish_time
    std::vector<TabletWriteLogEntry> get_logs(int64_t table_id = -1, int64_t partition_id = -1,
                                               int64_t tablet_id = -1, int64_t log_type = -1,
                                               int64_t start_finish_time = 0, int64_t end_finish_time = 0) const;

    // Cleanup old logs (finish_time < threshold_time)
    void cleanup_old_logs(int64_t threshold_finish_time);

    // Get current buffer size
    size_t size() const;

    // Get singleton instance
    static TabletWriteLogManager* instance();

private:
    // Add log to buffer (internal method)
    void _add_log(TabletWriteLogEntry entry);

    // Check and clean overflowing logs
    void _check_buffer_overflow();

    mutable std::mutex _mutex;                  // Protects _log_buffer
    std::deque<TabletWriteLogEntry> _log_buffer; // Ring buffer
    size_t _max_buffer_size;                     // Max buffer size
    int64_t _retention_time_ms;                  // Log retention time (ms)
    std::atomic<uint64_t> _total_logs_added{0}; // Stats: Total logs added
    std::atomic<uint64_t> _total_logs_dropped{0}; // Stats: Logs dropped due to overflow
};

} // namespace starrocks::lake

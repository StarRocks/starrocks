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

#include "storage/lake/tablet_write_log_manager.h"

#include <chrono>

#include "common/config.h"
#include "gutil/strings/substitute.h"
#include "util/time.h"

namespace starrocks::lake {

TabletWriteLogManager::TabletWriteLogManager()
        : _max_buffer_size(config::tablet_write_log_buffer_size),
          _retention_time_ms(config::tablet_write_log_retention_time_ms) {}

TabletWriteLogManager* TabletWriteLogManager::instance() {
    static TabletWriteLogManager instance;
    return &instance;
}

void TabletWriteLogManager::add_load_log(int64_t backend_id, int64_t txn_id, int64_t tablet_id, int64_t table_id,
                                         int64_t partition_id, int64_t input_rows, int64_t input_bytes,
                                         int64_t output_rows, int64_t output_bytes, int32_t output_segments,
                                         const std::string& label, int64_t begin_time, int64_t finish_time) {
    TabletWriteLogEntry entry;
    entry.begin_time = begin_time;
    entry.finish_time = finish_time;
    entry.backend_id = backend_id;
    entry.txn_id = txn_id;
    entry.tablet_id = tablet_id;
    entry.table_id = table_id;
    entry.partition_id = partition_id;
    entry.log_type = LogType::LOAD;
    entry.input_rows = input_rows;
    entry.input_bytes = input_bytes;
    entry.output_rows = output_rows;
    entry.output_bytes = output_bytes;
    entry.input_segments = 0; // Load does not need input_segments
    entry.output_segments = output_segments;
    entry.label = label;
    entry.compaction_score = 0;
    entry.compaction_type = "";

    _add_log(std::move(entry));
}

void TabletWriteLogManager::add_compaction_log(int64_t backend_id, int64_t txn_id, int64_t tablet_id, int64_t table_id,
                                               int64_t partition_id, int64_t input_rows, int64_t input_bytes,
                                               int64_t output_rows, int64_t output_bytes, int32_t input_segments,
                                               int32_t output_segments, int64_t compaction_score,
                                               const std::string& compaction_type, int64_t begin_time,
                                               int64_t finish_time) {
    TabletWriteLogEntry entry;
    entry.begin_time = begin_time;
    entry.finish_time = finish_time;
    entry.backend_id = backend_id;
    entry.txn_id = txn_id;
    entry.tablet_id = tablet_id;
    entry.table_id = table_id;
    entry.partition_id = partition_id;
    entry.log_type = LogType::COMPACTION;
    entry.input_rows = input_rows;
    entry.input_bytes = input_bytes;
    entry.output_rows = output_rows;
    entry.output_bytes = output_bytes;
    entry.input_segments = input_segments;
    entry.output_segments = output_segments;
    entry.label = "";
    entry.compaction_score = compaction_score;
    entry.compaction_type = compaction_type;

    _add_log(std::move(entry));
}

void TabletWriteLogManager::_add_log(TabletWriteLogEntry entry) {
    std::lock_guard<std::mutex> lock(_mutex);
    _log_buffer.push_back(std::move(entry));
    _total_logs_added.fetch_add(1, std::memory_order_relaxed);
    _check_buffer_overflow();
}

void TabletWriteLogManager::_check_buffer_overflow() {
    // Note: Must hold _mutex when calling this method
    if (_log_buffer.size() > _max_buffer_size) {
        size_t to_remove = _log_buffer.size() - _max_buffer_size;
        _log_buffer.erase(_log_buffer.begin(), _log_buffer.begin() + to_remove);
        _total_logs_dropped.fetch_add(to_remove, std::memory_order_relaxed);
    }
}

std::vector<TabletWriteLogEntry> TabletWriteLogManager::get_logs(int64_t table_id, int64_t partition_id,
                                                                 int64_t tablet_id, int64_t log_type,
                                                                 int64_t start_finish_time,
                                                                 int64_t end_finish_time) const {
    std::lock_guard<std::mutex> lock(_mutex);
    std::vector<TabletWriteLogEntry> result;
    result.reserve(_log_buffer.size());

    for (const auto& entry : _log_buffer) {
        // Filter conditions
        if (table_id > 0 && entry.table_id != table_id) continue;
        if (partition_id > 0 && entry.partition_id != partition_id) continue;
        if (tablet_id > 0 && entry.tablet_id != tablet_id) continue;
        if (log_type > 0 && static_cast<int64_t>(entry.log_type) != log_type) continue;
        if (start_finish_time > 0 && entry.finish_time < start_finish_time) continue;
        if (end_finish_time > 0 && entry.finish_time > end_finish_time) continue;

        result.push_back(entry);
    }

    return result;
}

void TabletWriteLogManager::cleanup_old_logs(int64_t threshold_finish_time) {
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = std::remove_if(_log_buffer.begin(), _log_buffer.end(),
                             [threshold_finish_time](const TabletWriteLogEntry& entry) {
                                 return entry.finish_time < threshold_finish_time;
                             });
    _log_buffer.erase(it, _log_buffer.end());
}

size_t TabletWriteLogManager::size() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _log_buffer.size();
}

} // namespace starrocks::lake

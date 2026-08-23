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

#include "connector/hive/paimon/paimon_scanner.h"

#include <utility>

#include "connector/hive/paimon/paimon_file_system.h"
#include "connector/hive/paimon/paimon_native_reader.h"

namespace starrocks {
namespace {

void update_paimon_io_profile(RuntimeProfile* profile, const PaimonFileSystemStats::Snapshot& io_stats) {
    const std::string paimon_fs_section = "PaimonFileSystem";
    ADD_COUNTER(profile, paimon_fs_section, TUnit::NONE);
    auto update_counter = [&](const char* name, TUnit::type unit, int64_t value) {
        auto* counter = ADD_CHILD_COUNTER(profile, name, unit, paimon_fs_section);
        COUNTER_UPDATE(counter, value);
    };
    update_counter("PaimonFSAppIOCount", TUnit::UNIT, io_stats.app_io_count());
    update_counter("PaimonFSAppIOBytes", TUnit::BYTES, io_stats.app_io_bytes());
    update_counter("PaimonFSAppIOTime", TUnit::TIME_NS, io_stats.app_io_ns());
    update_counter("PaimonFSIOCount", TUnit::UNIT, io_stats.fs_io_count);
    update_counter("PaimonFSIOBytes", TUnit::BYTES, io_stats.fs_io_bytes);
    update_counter("PaimonFSIOTime", TUnit::TIME_NS, io_stats.fs_io_ns);
    update_counter("PaimonFSSequentialReadCount", TUnit::UNIT, io_stats.sequential_read_count);
    update_counter("PaimonFSSequentialReadBytes", TUnit::BYTES, io_stats.sequential_read_bytes);
    update_counter("PaimonFSSequentialReadTime", TUnit::TIME_NS, io_stats.sequential_read_ns);
    update_counter("PaimonFSPositionalReadCount", TUnit::UNIT, io_stats.positional_read_count);
    update_counter("PaimonFSPositionalReadBytes", TUnit::BYTES, io_stats.positional_read_bytes);
    update_counter("PaimonFSPositionalReadTime", TUnit::TIME_NS, io_stats.positional_read_ns);
    update_counter("PaimonFSAsyncReadCount", TUnit::UNIT, io_stats.async_read_count);
    update_counter("PaimonFSAsyncReadBytes", TUnit::BYTES, io_stats.async_read_bytes);
    update_counter("PaimonFSAsyncReadTime", TUnit::TIME_NS, io_stats.async_read_ns);
}

} // namespace

PaimonScanner::~PaimonScanner() = default;

Status PaimonScanner::do_init(RuntimeState*, const HdfsScannerContext&) {
    return Status::OK();
}

Status PaimonScanner::do_open(RuntimeState* runtime_state) {
    _reader = std::make_unique<PaimonNativeReader>(*_scanner_ctx, runtime_state, &_app_stats);
    return _reader->open();
}

Status PaimonScanner::do_get_next(RuntimeState*, ChunkPtr* chunk) {
    ChunkPtr output;
    RETURN_IF_ERROR(_reader->get_next(&output));
    const size_t row_count = output->num_rows();
    RETURN_IF_ERROR(_scanner_ctx->format_scan_context.append_side_columns_to_chunk(&output, row_count));
    RETURN_IF_ERROR(_scanner_ctx->format_scan_context.evaluate_all_predicates(&output));
    *chunk = std::move(output);
    return Status::OK();
}

void PaimonScanner::do_prepare_close() noexcept {
    if (_reader != nullptr) {
        _reader->close();
    }
}

void PaimonScanner::do_close(RuntimeState*) noexcept {
    if (_reader != nullptr) {
        _reader->close();
        _reader.reset();
    }
}

void PaimonScanner::do_update_counter(HdfsScannerProfile* profile) {
    if (_reader == nullptr) {
        return;
    }

    RuntimeProfile* runtime_profile = profile->runtime_profile;
    auto metrics = _reader->get_reader_metrics();
    if (metrics != nullptr) {
        const std::string paimon_section = "PaimonNativeReader";
        ADD_COUNTER(runtime_profile, paimon_section, TUnit::NONE);
        for (const auto& [key, value] : metrics->GetAllCounters()) {
            TUnit::type unit = TUnit::UNIT;
            int64_t counter_value = static_cast<int64_t>(value);
            if (key.find("bytes") != std::string::npos) {
                unit = TUnit::BYTES;
            } else if (key.find("latency") != std::string::npos) {
                unit = TUnit::TIME_NS;
                counter_value *= 1000;
            }
            auto* counter = ADD_CHILD_COUNTER(runtime_profile, key, unit, paimon_section);
            COUNTER_UPDATE(counter, counter_value);
        }
    }

    const auto paimon_file_system = _reader->get_paimon_file_system();
    if (paimon_file_system == nullptr) {
        return;
    }
    const auto fs_stats = paimon_file_system->get_stats();
    update_paimon_io_profile(runtime_profile, fs_stats);
    if (!paimon_file_system->datacache_enabled()) {
        return;
    }

    const auto& stats = fs_stats.datacache;
    COUNTER_UPDATE(profile->datacache_read_counter, stats.read_block_cache_count);
    COUNTER_UPDATE(profile->datacache_read_bytes, stats.read_block_cache_bytes);
    COUNTER_UPDATE(profile->datacache_read_mem_bytes, stats.read_mem_cache_bytes);
    COUNTER_UPDATE(profile->datacache_read_disk_bytes, stats.read_disk_cache_bytes);
    COUNTER_UPDATE(profile->datacache_read_timer, stats.read_block_cache_ns);
    COUNTER_UPDATE(profile->datacache_read_peer_bytes, stats.read_peer_cache_bytes);
    COUNTER_UPDATE(profile->datacache_read_peer_counter, stats.read_peer_cache_count);
    COUNTER_UPDATE(profile->datacache_read_peer_timer, stats.read_peer_cache_ns);
    COUNTER_UPDATE(profile->datacache_skip_read_counter, stats.skip_read_cache_count);
    COUNTER_UPDATE(profile->datacache_skip_read_bytes, stats.skip_read_cache_bytes);
    COUNTER_UPDATE(profile->datacache_skip_read_peer_counter, stats.skip_read_peer_cache_count);
    COUNTER_UPDATE(profile->datacache_skip_read_peer_bytes, stats.skip_read_peer_cache_bytes);
    COUNTER_UPDATE(profile->datacache_write_counter, stats.write_block_cache_count);
    COUNTER_UPDATE(profile->datacache_write_bytes, stats.write_block_cache_bytes);
    COUNTER_UPDATE(profile->datacache_write_timer, stats.write_block_cache_ns);
    COUNTER_UPDATE(profile->datacache_write_fail_counter, stats.write_cache_fail_count);
    COUNTER_UPDATE(profile->datacache_write_fail_bytes, stats.write_cache_fail_bytes);
    COUNTER_UPDATE(profile->datacache_skip_write_counter, stats.skip_write_cache_count);
    COUNTER_UPDATE(profile->datacache_skip_write_bytes, stats.skip_write_cache_bytes);
    COUNTER_UPDATE(profile->datacache_read_block_buffer_counter, stats.read_block_buffer_count);
    COUNTER_UPDATE(profile->datacache_read_block_buffer_bytes, stats.read_block_buffer_bytes);

    const auto& shared_stats = fs_stats.shared_buffered;
    COUNTER_UPDATE(profile->shared_buffered_shared_io_count, shared_stats.shared_io_count);
    COUNTER_UPDATE(profile->shared_buffered_shared_io_bytes, shared_stats.shared_io_bytes);
    COUNTER_UPDATE(profile->shared_buffered_shared_align_io_bytes, shared_stats.shared_align_io_bytes);
    COUNTER_UPDATE(profile->shared_buffered_shared_io_timer, shared_stats.shared_io_timer);
    COUNTER_UPDATE(profile->shared_buffered_direct_io_count, shared_stats.direct_io_count);
    COUNTER_UPDATE(profile->shared_buffered_direct_io_bytes, shared_stats.direct_io_bytes);
    COUNTER_UPDATE(profile->shared_buffered_direct_io_timer, shared_stats.direct_io_timer);
}

} // namespace starrocks

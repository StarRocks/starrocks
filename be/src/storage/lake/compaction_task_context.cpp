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

#include "storage/lake/compaction_task_context.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <algorithm>

#include "base/time/time.h"
#include "storage/olap_common.h"

namespace starrocks::lake {

static constexpr long TIME_UNIT_NS_PER_SECOND = 1000000000;
static constexpr long BYTES_UNIT_MB = 1048576;

void CompactionTaskStats::collect(const OlapReaderStatistics& reader_stats) {
    create_segment_iter_ns = reader_stats.create_segment_iter_ns;
    decompress_ns = reader_stats.decompress_ns;
    block_load_ns = reader_stats.block_load_ns;
    block_fetch_ns = reader_stats.block_fetch_ns;
    block_seek_ns = reader_stats.block_seek_ns;
    block_seek_count = reader_stats.block_seek_num;
    decode_dict_ns = reader_stats.decode_dict_ns;
    get_rowsets_ns = reader_stats.get_rowsets_ns;
    get_delvec_ns = reader_stats.get_delvec_ns;
    get_delta_column_group_ns = reader_stats.get_delta_column_group_ns;
    del_filter_ns = reader_stats.del_filter_ns;
    blocks_load = reader_stats.blocks_load;
    raw_rows_read = reader_stats.raw_rows_read;
    compressed_bytes_read = reader_stats.compressed_bytes_read;
    uncompressed_bytes_read = reader_stats.uncompressed_bytes_read;
    io_ns_read_remote = reader_stats.io_ns_remote;
    io_ns_read_local_disk = reader_stats.io_ns_read_local_disk;
    io_bytes_read_remote = reader_stats.compressed_bytes_read_remote;
    io_bytes_read_local_disk = reader_stats.compressed_bytes_read_local_disk;
    segment_init_ns = reader_stats.segment_init_ns;
    column_iterator_init_ns = reader_stats.column_iterator_init_ns;
    io_count_local_disk = reader_stats.io_count_local_disk;
    io_count_remote = reader_stats.io_count_remote;
    // Note: read_segment_count is managed explicitly in compaction task code
    // by summing rowset->num_segments(), not from reader_stats.
}

void CompactionTaskStats::collect(const OlapWriterStatistics& writer_stats) {
    write_segment_count = writer_stats.segment_count;
    write_segment_bytes = writer_stats.bytes_write_remote;
    io_ns_write_remote = writer_stats.write_remote_ns;
}

CompactionTaskStats CompactionTaskStats::operator+(const CompactionTaskStats& that) const {
    CompactionTaskStats diff = *this;
    if (diff.compaction_type == "unknown") {
        diff.compaction_type = that.compaction_type;
    } else if (that.compaction_type != "unknown" && diff.compaction_type != that.compaction_type) {
        diff.compaction_type = "mixed";
    }
    diff.task_attempt_count += that.task_attempt_count;
    diff.queue_wait_ns += that.queue_wait_ns;
    diff.task_prepare_ns += that.task_prepare_ns;
    diff.task_execute_ns += that.task_execute_ns;
    diff.task_total_ns += that.task_total_ns;
    diff.input_prepare_ns += that.input_prepare_ns;
    diff.reader_prepare_ns += that.reader_prepare_ns;
    diff.reader_open_ns += that.reader_open_ns;
    diff.reader_get_next_ns += that.reader_get_next_ns;
    diff.reader_close_ns += that.reader_close_ns;
    diff.chunk_transform_ns += that.chunk_transform_ns;
    diff.writer_create_ns += that.writer_create_ns;
    diff.writer_open_ns += that.writer_open_ns;
    diff.writer_write_ns += that.writer_write_ns;
    diff.writer_flush_ns += that.writer_flush_ns;
    diff.writer_finish_ns += that.writer_finish_ns;
    diff.writer_close_ns += that.writer_close_ns;
    diff.mask_io_ns += that.mask_io_ns;
    diff.txn_log_build_ns += that.txn_log_build_ns;
    diff.txn_log_write_ns += that.txn_log_write_ns;
    diff.preload_compaction_state_ns += that.preload_compaction_state_ns;
    diff.tablet_write_log_ns += that.tablet_write_log_ns;
    diff.create_segment_iter_ns += that.create_segment_iter_ns;
    diff.decompress_ns += that.decompress_ns;
    diff.block_load_ns += that.block_load_ns;
    diff.block_fetch_ns += that.block_fetch_ns;
    diff.block_seek_ns += that.block_seek_ns;
    diff.block_seek_count += that.block_seek_count;
    diff.decode_dict_ns += that.decode_dict_ns;
    diff.get_rowsets_ns += that.get_rowsets_ns;
    diff.get_delvec_ns += that.get_delvec_ns;
    diff.get_delta_column_group_ns += that.get_delta_column_group_ns;
    diff.del_filter_ns += that.del_filter_ns;
    diff.blocks_load += that.blocks_load;
    diff.raw_rows_read += that.raw_rows_read;
    diff.compressed_bytes_read += that.compressed_bytes_read;
    diff.uncompressed_bytes_read += that.uncompressed_bytes_read;
    diff.io_ns_read_remote += that.io_ns_read_remote;
    diff.io_ns_read_local_disk += that.io_ns_read_local_disk;
    diff.io_bytes_read_remote += that.io_bytes_read_remote;
    diff.io_bytes_read_local_disk += that.io_bytes_read_local_disk;
    diff.segment_init_ns += that.segment_init_ns;
    diff.column_iterator_init_ns += that.column_iterator_init_ns;
    diff.io_count_local_disk += that.io_count_local_disk;
    diff.io_count_remote += that.io_count_remote;
    diff.input_rowset_count += that.input_rowset_count;
    diff.input_row_count += that.input_row_count;
    diff.output_row_count += that.output_row_count;
    diff.read_chunk_count += that.read_chunk_count;
    diff.write_chunk_count += that.write_chunk_count;
    diff.column_group_count += that.column_group_count;
    diff.vertical_key_group_ns += that.vertical_key_group_ns;
    diff.vertical_value_group_ns += that.vertical_value_group_ns;
    diff.read_segment_count += that.read_segment_count;
    diff.write_segment_count += that.write_segment_count;
    diff.write_segment_bytes += that.write_segment_bytes;
    diff.io_ns_write_remote += that.io_ns_write_remote;
    diff.in_queue_time_sec += that.in_queue_time_sec;
    diff.pk_sst_merge_ns += that.pk_sst_merge_ns;
    diff.input_file_size += that.input_file_size;
    return diff;
}

CompactionTaskStats CompactionTaskStats::operator-(const CompactionTaskStats& that) const {
    CompactionTaskStats diff = *this;
    diff.task_attempt_count -= that.task_attempt_count;
    diff.queue_wait_ns -= that.queue_wait_ns;
    diff.task_prepare_ns -= that.task_prepare_ns;
    diff.task_execute_ns -= that.task_execute_ns;
    diff.task_total_ns -= that.task_total_ns;
    diff.input_prepare_ns -= that.input_prepare_ns;
    diff.reader_prepare_ns -= that.reader_prepare_ns;
    diff.reader_open_ns -= that.reader_open_ns;
    diff.reader_get_next_ns -= that.reader_get_next_ns;
    diff.reader_close_ns -= that.reader_close_ns;
    diff.chunk_transform_ns -= that.chunk_transform_ns;
    diff.writer_create_ns -= that.writer_create_ns;
    diff.writer_open_ns -= that.writer_open_ns;
    diff.writer_write_ns -= that.writer_write_ns;
    diff.writer_flush_ns -= that.writer_flush_ns;
    diff.writer_finish_ns -= that.writer_finish_ns;
    diff.writer_close_ns -= that.writer_close_ns;
    diff.mask_io_ns -= that.mask_io_ns;
    diff.txn_log_build_ns -= that.txn_log_build_ns;
    diff.txn_log_write_ns -= that.txn_log_write_ns;
    diff.preload_compaction_state_ns -= that.preload_compaction_state_ns;
    diff.tablet_write_log_ns -= that.tablet_write_log_ns;
    diff.create_segment_iter_ns -= that.create_segment_iter_ns;
    diff.decompress_ns -= that.decompress_ns;
    diff.block_load_ns -= that.block_load_ns;
    diff.block_fetch_ns -= that.block_fetch_ns;
    diff.block_seek_ns -= that.block_seek_ns;
    diff.block_seek_count -= that.block_seek_count;
    diff.decode_dict_ns -= that.decode_dict_ns;
    diff.get_rowsets_ns -= that.get_rowsets_ns;
    diff.get_delvec_ns -= that.get_delvec_ns;
    diff.get_delta_column_group_ns -= that.get_delta_column_group_ns;
    diff.del_filter_ns -= that.del_filter_ns;
    diff.blocks_load -= that.blocks_load;
    diff.raw_rows_read -= that.raw_rows_read;
    diff.compressed_bytes_read -= that.compressed_bytes_read;
    diff.uncompressed_bytes_read -= that.uncompressed_bytes_read;
    diff.io_ns_read_remote -= that.io_ns_read_remote;
    diff.io_ns_read_local_disk -= that.io_ns_read_local_disk;
    diff.io_bytes_read_remote -= that.io_bytes_read_remote;
    diff.io_bytes_read_local_disk -= that.io_bytes_read_local_disk;
    diff.segment_init_ns -= that.segment_init_ns;
    diff.column_iterator_init_ns -= that.column_iterator_init_ns;
    diff.io_count_local_disk -= that.io_count_local_disk;
    diff.io_count_remote -= that.io_count_remote;
    diff.input_rowset_count -= that.input_rowset_count;
    diff.input_row_count -= that.input_row_count;
    diff.output_row_count -= that.output_row_count;
    diff.read_chunk_count -= that.read_chunk_count;
    diff.write_chunk_count -= that.write_chunk_count;
    diff.column_group_count -= that.column_group_count;
    diff.vertical_key_group_ns -= that.vertical_key_group_ns;
    diff.vertical_value_group_ns -= that.vertical_value_group_ns;
    diff.read_segment_count -= that.read_segment_count;
    diff.write_segment_count -= that.write_segment_count;
    diff.write_segment_bytes -= that.write_segment_bytes;
    diff.io_ns_write_remote -= that.io_ns_write_remote;
    diff.in_queue_time_sec -= that.in_queue_time_sec;
    diff.pk_sst_merge_ns -= that.pk_sst_merge_ns;
    diff.input_file_size -= that.input_file_size;
    return diff;
}

int64_t CompactionTaskStats::task_accounted_ns() const {
    return task_prepare_ns + input_prepare_ns + reader_prepare_ns + reader_open_ns + reader_get_next_ns +
           reader_close_ns + chunk_transform_ns + writer_create_ns + writer_open_ns + writer_write_ns +
           writer_flush_ns + writer_finish_ns + writer_close_ns + mask_io_ns + txn_log_build_ns + pk_sst_merge_ns +
           txn_log_write_ns + preload_compaction_state_ns + tablet_write_log_ns;
}

int64_t CompactionTaskStats::task_unaccounted_ns() const {
    return std::max<int64_t>(0, task_total_ns - task_accounted_ns());
}

static void fill_stats_fields(rapidjson::Document& root, const CompactionTaskStats& s, bool profile_final) {
    auto& allocator = root.GetAllocator();
    root.AddMember("profile_version", rapidjson::Value(1), allocator);
    root.AddMember("profile_final", rapidjson::Value(profile_final), allocator);
    root.AddMember("compaction_type", rapidjson::Value(s.compaction_type.c_str(), allocator), allocator);
    root.AddMember("task_attempt_count", rapidjson::Value(s.task_attempt_count), allocator);
    root.AddMember("queue_wait_ns", rapidjson::Value(s.queue_wait_ns), allocator);
    root.AddMember("task_prepare_ns", rapidjson::Value(s.task_prepare_ns), allocator);
    root.AddMember("task_execute_ns", rapidjson::Value(s.task_execute_ns), allocator);
    root.AddMember("task_total_ns", rapidjson::Value(s.task_total_ns), allocator);
    root.AddMember("task_accounted_ns", rapidjson::Value(s.task_accounted_ns()), allocator);
    root.AddMember("task_unaccounted_ns", rapidjson::Value(s.task_unaccounted_ns()), allocator);
    root.AddMember("input_prepare_ns", rapidjson::Value(s.input_prepare_ns), allocator);
    root.AddMember("reader_prepare_ns", rapidjson::Value(s.reader_prepare_ns), allocator);
    root.AddMember("reader_open_ns", rapidjson::Value(s.reader_open_ns), allocator);
    root.AddMember("reader_get_next_ns", rapidjson::Value(s.reader_get_next_ns), allocator);
    root.AddMember("reader_close_ns", rapidjson::Value(s.reader_close_ns), allocator);
    root.AddMember("chunk_transform_ns", rapidjson::Value(s.chunk_transform_ns), allocator);
    root.AddMember("writer_create_ns", rapidjson::Value(s.writer_create_ns), allocator);
    root.AddMember("writer_open_ns", rapidjson::Value(s.writer_open_ns), allocator);
    root.AddMember("writer_write_ns", rapidjson::Value(s.writer_write_ns), allocator);
    root.AddMember("writer_flush_ns", rapidjson::Value(s.writer_flush_ns), allocator);
    root.AddMember("writer_finish_ns", rapidjson::Value(s.writer_finish_ns), allocator);
    root.AddMember("writer_close_ns", rapidjson::Value(s.writer_close_ns), allocator);
    root.AddMember("mask_io_ns", rapidjson::Value(s.mask_io_ns), allocator);
    root.AddMember("txn_log_build_ns", rapidjson::Value(s.txn_log_build_ns), allocator);
    root.AddMember("txn_log_write_ns", rapidjson::Value(s.txn_log_write_ns), allocator);
    root.AddMember("preload_compaction_state_ns", rapidjson::Value(s.preload_compaction_state_ns), allocator);
    root.AddMember("tablet_write_log_ns", rapidjson::Value(s.tablet_write_log_ns), allocator);
    root.AddMember("create_segment_iter_ns", rapidjson::Value(s.create_segment_iter_ns), allocator);
    root.AddMember("decompress_ns", rapidjson::Value(s.decompress_ns), allocator);
    root.AddMember("block_load_ns", rapidjson::Value(s.block_load_ns), allocator);
    root.AddMember("block_fetch_ns", rapidjson::Value(s.block_fetch_ns), allocator);
    root.AddMember("block_seek_ns", rapidjson::Value(s.block_seek_ns), allocator);
    root.AddMember("block_seek_count", rapidjson::Value(s.block_seek_count), allocator);
    root.AddMember("decode_dict_ns", rapidjson::Value(s.decode_dict_ns), allocator);
    root.AddMember("get_rowsets_ns", rapidjson::Value(s.get_rowsets_ns), allocator);
    root.AddMember("get_delvec_ns", rapidjson::Value(s.get_delvec_ns), allocator);
    root.AddMember("get_delta_column_group_ns", rapidjson::Value(s.get_delta_column_group_ns), allocator);
    root.AddMember("del_filter_ns", rapidjson::Value(s.del_filter_ns), allocator);
    root.AddMember("blocks_load", rapidjson::Value(s.blocks_load), allocator);
    root.AddMember("raw_rows_read", rapidjson::Value(s.raw_rows_read), allocator);
    root.AddMember("compressed_bytes_read", rapidjson::Value(s.compressed_bytes_read), allocator);
    root.AddMember("uncompressed_bytes_read", rapidjson::Value(s.uncompressed_bytes_read), allocator);
    root.AddMember("read_remote_ns", rapidjson::Value(s.io_ns_read_remote), allocator);
    root.AddMember("read_local_ns", rapidjson::Value(s.io_ns_read_local_disk), allocator);
    root.AddMember("read_local_sec", rapidjson::Value(s.io_ns_read_local_disk / TIME_UNIT_NS_PER_SECOND), allocator);
    root.AddMember("read_local_mb", rapidjson::Value(s.io_bytes_read_local_disk / BYTES_UNIT_MB), allocator);
    root.AddMember("read_remote_sec", rapidjson::Value(s.io_ns_read_remote / TIME_UNIT_NS_PER_SECOND), allocator);
    root.AddMember("read_remote_mb", rapidjson::Value(s.io_bytes_read_remote / BYTES_UNIT_MB), allocator);
    root.AddMember("read_remote_count", rapidjson::Value(s.io_count_remote), allocator);
    root.AddMember("read_local_count", rapidjson::Value(s.io_count_local_disk), allocator);
    root.AddMember("segment_init_ns", rapidjson::Value(s.segment_init_ns), allocator);
    root.AddMember("column_iterator_init_ns", rapidjson::Value(s.column_iterator_init_ns), allocator);
    root.AddMember("segment_init_sec", rapidjson::Value(s.segment_init_ns / TIME_UNIT_NS_PER_SECOND), allocator);
    root.AddMember("column_iterator_init_sec", rapidjson::Value(s.column_iterator_init_ns / TIME_UNIT_NS_PER_SECOND),
                   allocator);
    root.AddMember("input_rowset_count", rapidjson::Value(s.input_rowset_count), allocator);
    root.AddMember("input_row_count", rapidjson::Value(s.input_row_count), allocator);
    root.AddMember("output_row_count", rapidjson::Value(s.output_row_count), allocator);
    root.AddMember("read_chunk_count", rapidjson::Value(s.read_chunk_count), allocator);
    root.AddMember("write_chunk_count", rapidjson::Value(s.write_chunk_count), allocator);
    root.AddMember("column_group_count", rapidjson::Value(s.column_group_count), allocator);
    root.AddMember("vertical_key_group_ns", rapidjson::Value(s.vertical_key_group_ns), allocator);
    root.AddMember("vertical_value_group_ns", rapidjson::Value(s.vertical_value_group_ns), allocator);
    root.AddMember("read_segment_count", rapidjson::Value(s.read_segment_count), allocator);
    root.AddMember("write_segment_count", rapidjson::Value(s.write_segment_count), allocator);
    root.AddMember("write_remote_mb", rapidjson::Value(s.write_segment_bytes / BYTES_UNIT_MB), allocator);
    root.AddMember("write_remote_ns", rapidjson::Value(s.io_ns_write_remote), allocator);
    root.AddMember("write_remote_sec", rapidjson::Value(s.io_ns_write_remote / TIME_UNIT_NS_PER_SECOND), allocator);
    root.AddMember("in_queue_sec", rapidjson::Value(s.in_queue_time_sec), allocator);
    root.AddMember("pk_sst_merge_sec", rapidjson::Value(s.pk_sst_merge_ns / TIME_UNIT_NS_PER_SECOND), allocator);
    root.AddMember("input_file_size", rapidjson::Value(s.input_file_size), allocator);
}

static std::string serialize(const rapidjson::Document& root) {
    rapidjson::StringBuffer strbuf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    return {strbuf.GetString()};
}

std::string CompactionTaskStats::to_json_stats(bool profile_final) const {
    rapidjson::Document root;
    root.SetObject();
    fill_stats_fields(root, *this, profile_final);
    return serialize(root);
}

std::string CompactionTaskStats::to_json_stats_with_subtask_metadata(int32_t subtask_id, size_t input_rowsets,
                                                                     bool profile_final) const {
    rapidjson::Document root;
    root.SetObject();
    fill_stats_fields(root, *this, profile_final);
    auto& allocator = root.GetAllocator();
    root.AddMember("subtask_id", rapidjson::Value(subtask_id), allocator);
    root.AddMember("input_rowsets", rapidjson::Value(static_cast<int64_t>(input_rowsets)), allocator);
    root.AddMember("is_parallel_subtask", rapidjson::Value(true), allocator);
    return serialize(root);
}

void CompactionTaskContext::reset_attempt_stats() {
    *stats = CompactionTaskStats();
}

void CompactionTaskContext::publish_stats_snapshot() {
    std::lock_guard lock(_stats_snapshot_mutex);
    _published_stats = *stats;
    _published_task_attempt_start_ns = task_attempt_start_ns.load(std::memory_order_relaxed);
    _published_task_execute_start_ns = task_execute_start_ns.load(std::memory_order_relaxed);
}

CompactionTaskStats CompactionTaskContext::stats_snapshot(bool include_live_timers) const {
    CompactionTaskStats snapshot;
    int64_t attempt_start_ns;
    int64_t execute_start_ns;
    {
        std::lock_guard lock(_stats_snapshot_mutex);
        snapshot = _published_stats;
        attempt_start_ns = _published_task_attempt_start_ns;
        execute_start_ns = _published_task_execute_start_ns;
    }
    if (!include_live_timers) {
        return snapshot;
    }

    const int64_t now_ns = MonotonicNanos();
    if (attempt_start_ns > 0 && now_ns > attempt_start_ns) {
        snapshot.task_total_ns += now_ns - attempt_start_ns;
    }
    if (execute_start_ns > 0 && now_ns > execute_start_ns) {
        snapshot.task_execute_ns += now_ns - execute_start_ns;
    }
    return snapshot;
}
} // namespace starrocks::lake

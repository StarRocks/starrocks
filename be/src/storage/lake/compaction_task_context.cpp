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

#include "storage/olap_common.h"

namespace starrocks::lake {

static constexpr long TIME_UNIT_NS_PER_SECOND = 1000000000;
static constexpr long BYTES_UNIT_MB = 1048576;

void CompactionTaskStats::collect(const OlapReaderStatistics& reader_stats) {
    io_ns_read_remote = reader_stats.io_ns_remote;
    io_ns_read_local_disk = reader_stats.io_ns_read_local_disk;
    io_bytes_read_remote = reader_stats.compressed_bytes_read_remote;
    io_bytes_read_local_disk = reader_stats.compressed_bytes_read_local_disk;
    segment_init_ns = reader_stats.segment_init_ns;
    column_iterator_init_ns = reader_stats.column_iterator_init_ns;
    io_count_local_disk = reader_stats.io_count_local_disk;
    io_count_remote = reader_stats.io_count_remote;
    // read segment count is managed else where
    // read_segment_count = reader_stats.segments_read_count;
}

void CompactionTaskStats::collect(const OlapWriterStatistics& writer_stats) {
    write_segment_count = writer_stats.segment_count;
    write_segment_bytes = writer_stats.bytes_write_remote;
    io_ns_write_remote = writer_stats.write_remote_ns;
}

CompactionTaskStats CompactionTaskStats::operator+(const CompactionTaskStats& that) const {
    CompactionTaskStats diff = *this;
    diff.io_ns_read_remote += that.io_ns_read_remote;
    diff.io_ns_read_local_disk += that.io_ns_read_local_disk;
    diff.io_bytes_read_remote += that.io_bytes_read_remote;
    diff.io_bytes_read_local_disk += that.io_bytes_read_local_disk;
    diff.segment_init_ns += that.segment_init_ns;
    diff.column_iterator_init_ns += that.column_iterator_init_ns;
    diff.io_count_local_disk += that.io_count_local_disk;
    diff.io_count_remote += that.io_count_remote;
<<<<<<< HEAD
    // read segment count is managed else where
    // diff.read_segment_count += that.read_segment_count;
=======
    diff.input_rowset_count += that.input_rowset_count;
    diff.input_row_count += that.input_row_count;
    diff.output_row_count += that.output_row_count;
    diff.read_chunk_count += that.read_chunk_count;
    diff.write_chunk_count += that.write_chunk_count;
    diff.column_group_count += that.column_group_count;
    diff.stream_buffer_shrunk_passes += that.stream_buffer_shrunk_passes;
    diff.vertical_key_group_ns += that.vertical_key_group_ns;
    diff.vertical_value_group_ns += that.vertical_value_group_ns;
    diff.read_segment_count += that.read_segment_count;
>>>>>>> 80ca09f ([BugFix] Bound lake compaction read buffers by the per-worker memory budget (#80229))
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
    diff.io_ns_read_remote -= that.io_ns_read_remote;
    diff.io_ns_read_local_disk -= that.io_ns_read_local_disk;
    diff.io_bytes_read_remote -= that.io_bytes_read_remote;
    diff.io_bytes_read_local_disk -= that.io_bytes_read_local_disk;
    diff.segment_init_ns -= that.segment_init_ns;
    diff.column_iterator_init_ns -= that.column_iterator_init_ns;
    diff.io_count_local_disk -= that.io_count_local_disk;
    diff.io_count_remote -= that.io_count_remote;
<<<<<<< HEAD
    // read segment count is managed else where
    // diff.read_segment_count -= that.read_segment_count;
=======
    diff.input_rowset_count -= that.input_rowset_count;
    diff.input_row_count -= that.input_row_count;
    diff.output_row_count -= that.output_row_count;
    diff.read_chunk_count -= that.read_chunk_count;
    diff.write_chunk_count -= that.write_chunk_count;
    diff.column_group_count -= that.column_group_count;
    diff.stream_buffer_shrunk_passes -= that.stream_buffer_shrunk_passes;
    diff.vertical_key_group_ns -= that.vertical_key_group_ns;
    diff.vertical_value_group_ns -= that.vertical_value_group_ns;
    diff.read_segment_count -= that.read_segment_count;
>>>>>>> 80ca09f ([BugFix] Bound lake compaction read buffers by the per-worker memory budget (#80229))
    diff.write_segment_count -= that.write_segment_count;
    diff.write_segment_bytes -= that.write_segment_bytes;
    diff.io_ns_write_remote -= that.io_ns_write_remote;
    diff.in_queue_time_sec -= that.in_queue_time_sec;
    diff.pk_sst_merge_ns -= that.pk_sst_merge_ns;
    diff.input_file_size -= that.input_file_size;
    return diff;
}

std::string CompactionTaskStats::to_json_stats() {
    rapidjson::Document root;
    root.SetObject();
    auto& allocator = root.GetAllocator();
    // add stats
    root.AddMember("read_local_sec", rapidjson::Value(io_ns_read_local_disk / TIME_UNIT_NS_PER_SECOND), allocator);
    root.AddMember("read_local_mb", rapidjson::Value(io_bytes_read_local_disk / BYTES_UNIT_MB), allocator);
    root.AddMember("read_remote_sec", rapidjson::Value(io_ns_read_remote / TIME_UNIT_NS_PER_SECOND), allocator);
    root.AddMember("read_remote_mb", rapidjson::Value(io_bytes_read_remote / BYTES_UNIT_MB), allocator);
    root.AddMember("read_remote_count", rapidjson::Value(io_count_remote), allocator);
    root.AddMember("read_local_count", rapidjson::Value(io_count_local_disk), allocator);
    root.AddMember("segment_init_sec", rapidjson::Value(segment_init_ns / TIME_UNIT_NS_PER_SECOND), allocator);
    root.AddMember("column_iterator_init_sec", rapidjson::Value(column_iterator_init_ns / TIME_UNIT_NS_PER_SECOND),
                   allocator);
<<<<<<< HEAD
    root.AddMember("read_segment_count", rapidjson::Value(read_segment_count), allocator);
    root.AddMember("write_segment_count", rapidjson::Value(write_segment_count), allocator);
    root.AddMember("write_remote_mb", rapidjson::Value(write_segment_bytes / BYTES_UNIT_MB), allocator);
    root.AddMember("write_remote_sec", rapidjson::Value(io_ns_write_remote / TIME_UNIT_NS_PER_SECOND), allocator);
    root.AddMember("in_queue_sec", rapidjson::Value(in_queue_time_sec), allocator);
    root.AddMember("pk_sst_merge_sec", rapidjson::Value(pk_sst_merge_ns / TIME_UNIT_NS_PER_SECOND), allocator);
    root.AddMember("input_file_size", rapidjson::Value(input_file_size), allocator);
=======
    root.AddMember("input_rowset_count", rapidjson::Value(s.input_rowset_count), allocator);
    root.AddMember("input_row_count", rapidjson::Value(s.input_row_count), allocator);
    root.AddMember("output_row_count", rapidjson::Value(s.output_row_count), allocator);
    root.AddMember("read_chunk_count", rapidjson::Value(s.read_chunk_count), allocator);
    root.AddMember("write_chunk_count", rapidjson::Value(s.write_chunk_count), allocator);
    root.AddMember("column_group_count", rapidjson::Value(s.column_group_count), allocator);
    root.AddMember("stream_buffer_shrunk_passes", rapidjson::Value(s.stream_buffer_shrunk_passes), allocator);
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
>>>>>>> 80ca09f ([BugFix] Bound lake compaction read buffers by the per-worker memory budget (#80229))

    rapidjson::StringBuffer strbuf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    return {strbuf.GetString()};
}
} // namespace starrocks::lake
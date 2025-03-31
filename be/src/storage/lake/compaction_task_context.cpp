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
    io_ns_remote = reader_stats.io_ns_remote;
    io_ns_local_disk = reader_stats.io_ns_read_local_disk;
    io_bytes_read_remote = reader_stats.compressed_bytes_read_remote;
    io_bytes_read_local_disk = reader_stats.compressed_bytes_read_local_disk;
    segment_init_ns = reader_stats.segment_init_ns;
    column_iterator_init_ns = reader_stats.column_iterator_init_ns;
    io_count_local_disk = reader_stats.io_count_local_disk;
    io_count_remote = reader_stats.io_count_remote;
}

CompactionTaskStats CompactionTaskStats::operator+(const CompactionTaskStats& that) const {
    CompactionTaskStats diff;
    diff.io_ns_remote = io_ns_remote + that.io_ns_remote;
    diff.io_ns_local_disk = io_ns_local_disk + that.io_ns_local_disk;
    diff.io_bytes_read_remote = io_bytes_read_remote + that.io_bytes_read_remote;
    diff.io_bytes_read_local_disk = io_bytes_read_local_disk + that.io_bytes_read_local_disk;
    diff.segment_init_ns = segment_init_ns + that.segment_init_ns;
    diff.column_iterator_init_ns = column_iterator_init_ns + that.column_iterator_init_ns;
    diff.io_count_local_disk = io_count_local_disk + that.io_count_local_disk;
    diff.io_count_remote = io_count_remote + that.io_count_remote;
    diff.in_queue_time_sec = in_queue_time_sec + that.in_queue_time_sec;
    diff.pk_sst_merge_ns = pk_sst_merge_ns + that.pk_sst_merge_ns;
    return diff;
}

CompactionTaskStats CompactionTaskStats::operator-(const CompactionTaskStats& that) const {
    CompactionTaskStats diff;
    diff.io_ns_remote = io_ns_remote - that.io_ns_remote;
    diff.io_ns_local_disk = io_ns_local_disk - that.io_ns_local_disk;
    diff.io_bytes_read_remote = io_bytes_read_remote - that.io_bytes_read_remote;
    diff.io_bytes_read_local_disk = io_bytes_read_local_disk - that.io_bytes_read_local_disk;
    diff.segment_init_ns = segment_init_ns - that.segment_init_ns;
    diff.column_iterator_init_ns = column_iterator_init_ns - that.column_iterator_init_ns;
    diff.io_count_local_disk = io_count_local_disk - that.io_count_local_disk;
    diff.io_count_remote = io_count_remote - that.io_count_remote;
    diff.in_queue_time_sec = in_queue_time_sec - that.in_queue_time_sec;
    diff.pk_sst_merge_ns = pk_sst_merge_ns - that.pk_sst_merge_ns;
    return diff;
}

std::string CompactionTaskStats::to_json_stats() {
    rapidjson::Document root;
    root.SetObject();
    auto& allocator = root.GetAllocator();
    // add stats
    root.AddMember("read_local_sec", rapidjson::Value(io_ns_local_disk / TIME_UNIT_NS_PER_SECOND), allocator);
    root.AddMember("read_local_mb", rapidjson::Value(io_bytes_read_local_disk / BYTES_UNIT_MB), allocator);
    root.AddMember("read_remote_sec", rapidjson::Value(io_ns_remote / TIME_UNIT_NS_PER_SECOND), allocator);
    root.AddMember("read_remote_mb", rapidjson::Value(io_bytes_read_remote / BYTES_UNIT_MB), allocator);
    root.AddMember("read_remote_count", rapidjson::Value(io_count_remote), allocator);
    root.AddMember("read_local_count", rapidjson::Value(io_count_local_disk), allocator);
    root.AddMember("segment_init_sec", rapidjson::Value(segment_init_ns / TIME_UNIT_NS_PER_SECOND), allocator);
    root.AddMember("column_iterator_init_sec", rapidjson::Value(column_iterator_init_ns / TIME_UNIT_NS_PER_SECOND),
                   allocator);
    root.AddMember("in_queue_sec", rapidjson::Value(in_queue_time_sec), allocator);
    root.AddMember("pk_sst_merge_sec", rapidjson::Value(pk_sst_merge_ns / TIME_UNIT_NS_PER_SECOND), allocator);

    rapidjson::StringBuffer strbuf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    return {strbuf.GetString()};
}
} // namespace starrocks::lake
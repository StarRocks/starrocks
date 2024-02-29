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

void CompactionTaskStats::accumulate(const OlapReaderStatistics& reader_stats) {
    io_ns += reader_stats.io_ns;
    io_ns_remote += reader_stats.io_ns_remote;
    io_ns_local_disk += reader_stats.io_ns_local_disk;
    segment_init_ns += reader_stats.segment_init_ns;
    column_iterator_init_ns += reader_stats.column_iterator_init_ns;
    io_count_local_disk += reader_stats.io_count_local_disk;
    io_count_remote += reader_stats.io_count_remote;
    compressed_bytes_read += reader_stats.compressed_bytes_read;
}

std::string CompactionTaskStats::to_json_stats() {
    rapidjson::Document root;
    root.SetObject();
    auto& allocator = root.GetAllocator();
    // add stats
    root.AddMember("reader_total_time_ms", rapidjson::Value(reader_time_ns / 1000000), allocator);
    root.AddMember("reader_io_ms", rapidjson::Value(io_ns / 1000000), allocator);
    root.AddMember("reader_io_ms_remote", rapidjson::Value(io_ns_remote / 1000000), allocator);
    root.AddMember("reader_io_ms_local_disk", rapidjson::Value(io_ns_local_disk / 1000000), allocator);
    root.AddMember("reader_io_count_remote", rapidjson::Value(io_count_remote), allocator);
    root.AddMember("reader_io_count_local_disk", rapidjson::Value(io_count_local_disk), allocator);
    root.AddMember("compressed_bytes_read", rapidjson::Value(compressed_bytes_read), allocator);
    root.AddMember("segment_init_ms", rapidjson::Value(segment_init_ns / 1000000), allocator);
    root.AddMember("column_iterator_init_ms", rapidjson::Value(column_iterator_init_ns / 1000000), allocator);
    root.AddMember("segment_write_ms", rapidjson::Value(segment_write_ns / 1000000), allocator);

    rapidjson::StringBuffer strbuf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    return {strbuf.GetString()};
}
} // namespace starrocks::lake
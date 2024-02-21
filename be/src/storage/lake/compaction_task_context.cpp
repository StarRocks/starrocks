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
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace starrocks::lake {

std::string CompactionTaskContext::to_json_stats() {
    rapidjson::Document root;
    root.SetObject();
    auto& allocator = root.GetAllocator();
    // add stats
    root.AddMember("reader_total_time_ms", rapidjson::Value(stats->reader_time_ns / 1000000), allocator);
    root.AddMember("reader_io_ms", rapidjson::Value(stats->io_ns / 1000000), allocator);
    root.AddMember("reader_io_count_remote", rapidjson::Value(stats->io_count_remote), allocator);
    root.AddMember("reader_io_count_local_disk", rapidjson::Value(stats->io_count_local_disk), allocator);
    root.AddMember("compressed_bytes_read", rapidjson::Value(stats->compressed_bytes_read), allocator);
    root.AddMember("segment_init_ms", rapidjson::Value(stats->segment_init_ns / 1000000), allocator);
    root.AddMember("column_iterator_init_ms", rapidjson::Value(stats->column_iterator_init_ns / 1000000), allocator);
    root.AddMember("segment_write_ms", rapidjson::Value(stats->segment_write_ns / 1000000), allocator);

    rapidjson::StringBuffer strbuf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    return {strbuf.GetString()};
}
} // namespace starrocks::lake
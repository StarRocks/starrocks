// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "http/action/memory_metrics_action.h"

#include <runtime/exec_env.h>
#include <runtime/mem_tracker.h>

#include "common/tracer.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"

namespace starrocks {

void MemoryMetricsAction::handle(HttpRequest* req) {
    LOG(INFO) << "Start collect memory metrics.";
    auto scoped_span = trace::Scope(Tracer::Instance().start_trace("http_handle_memory_metrics"));
    MemTracker* process_mem_tracker = GlobalEnv::GetInstance()->process_mem_tracker();
    std::stringstream result;
    std::vector<std::string> metric_labels_to_print = {"process",
                                                       "query_pool",
                                                       "load",
                                                       "metadata",
                                                       "tablet_metadata",
                                                       "rowset_metadata",
                                                       "segment_metadata",
                                                       "column_metadata",
                                                       "tablet_schema",
                                                       "segment_zonemap",
                                                       "short_key_index",
                                                       "column_zonemap_index",
                                                       "ordinal_index",
                                                       "bitmap_index",
                                                       "bloom_filter_index",
                                                       "compaction",
                                                       "schema_change",
                                                       "column_pool",
                                                       "page_cache",
                                                       "datacache",
                                                       "update",
                                                       "chunk_allocator",
                                                       "clone",
                                                       "consistency",
                                                       "rowset_update_state",
                                                       "index_cache",
                                                       "del_vec_cache",
                                                       "compaction_state"};
    result << "[";
    getMemoryMetricTree(process_mem_tracker, result, process_mem_tracker->consumption(), metric_labels_to_print);
    result << ",";
    getMemoryMetricTree(GlobalEnv::GetInstance()->metadata_mem_tracker(), result, process_mem_tracker->consumption(),
                        metric_labels_to_print);
    result << ",";
    getMemoryMetricTree(GlobalEnv::GetInstance()->update_mem_tracker(), result, process_mem_tracker->consumption(),
                        metric_labels_to_print);
    result << "]";
    req->add_output_header(HttpHeaders::CONTENT_TYPE, "text/plain; version=0.0.4");
    LOG(INFO) << "End collect memory metrics. " << result.str();

    HttpChannel::send_reply(req, result.str());
}
void MemoryMetricsAction::getMemoryMetricTree(MemTracker* memTracker, std::stringstream& result, int64_t total_size,
                                              std::vector<std::string> metric_labels_to_print) {
    result << "{";
    result << R"("name":")" << memTracker->label() << "\",";
    result << R"("size":")" << memTracker->consumption() << "\",";
    result << R"("percent":")" << std::setprecision(3)
           << static_cast<double>(memTracker->consumption()) / total_size * 100 << "%\",";
    result << "\"child\":[";
    for (const auto& child : memTracker->getChild()) {
        if (find(metric_labels_to_print.begin(), metric_labels_to_print.end(), child->label()) ==
            metric_labels_to_print.end()) {
            break;
        }
        if (child != memTracker->getChild().front()) {
            result << ",";
        }
        getMemoryMetricTree(child, result, total_size, metric_labels_to_print);
    }

    result << "]}";
}

} // namespace starrocks

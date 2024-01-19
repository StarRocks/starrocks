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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/http/default_path_handlers.cpp

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

#include "http/default_path_handlers.h"

#ifdef USE_JEMALLOC
#include "jemalloc/jemalloc.h"
#else
#include <gperftools/malloc_extension.h>
#endif
#include <gutil/strings/numbers.h>
#include <gutil/strings/substitute.h>

#include <boost/algorithm/string.hpp>
#include <sstream>

#include "common/configbase.h"
#include "http/web_page_handler.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "storage/storage_engine.h"
#include "storage/update_manager.h"
#include "util/pretty_printer.h"

namespace starrocks {

// Writes the last config::web_log_bytes of the INFO logfile to a webpage
// Note to get best performance, set GLOG_logbuflevel=-1 to prevent log buffering
void logs_handler(const WebPageHandler::ArgumentMap& args, std::stringstream* output) {
    /*std::string logfile;
    get_full_log_filename(google::INFO, &logfile);
    (*output) << "<h2>INFO logs</h2>" << std::endl;
    (*output) << "Log path is: " << logfile << std::endl;

    struct stat file_stat;

    if (stat(logfile.c_str(), &file_stat) == 0) {
        long size = file_stat.st_size;
        long seekpos = size < config::web_log_bytes ? 0L : size - config::web_log_bytes;
        std::ifstream log(logfile.c_str(), std::ios::in);
        // Note if the file rolls between stat and seek, this could fail
        // (and we could wind up reading the whole file). But because the
        // file is likely to be small, this is unlikely to be an issue in
        // practice.
        log.seekg(seekpos);
        (*output) << "<br/>Showing last " << config::web_log_bytes << " bytes of log" << std::endl;
        (*output) << "<br/><pre>" << log.rdbuf() << "</pre>";

    } else {
        (*output) << "<br/>Couldn't open INFO log file: " << logfile;
    }*/

    (*output) << "<br/>Couldn't open INFO log file: ";
}

// Registered to handle "/varz", and prints out all command-line flags and their values
void config_handler(const WebPageHandler::ArgumentMap& args, std::stringstream* output) {
    (*output) << "<h2>Configurations</h2>";
    (*output) << "<pre>";
    std::lock_guard<std::mutex> l(*config::get_mstring_conf_lock());
    for (const auto& it : *(config::full_conf_map)) {
        (*output) << it.first << "=" << it.second << std::endl;
    }
    (*output) << "</pre>";
}

void mem_tracker_handler(MemTracker* mem_tracker, const WebPageHandler::ArgumentMap& args, std::stringstream* output) {
    (*output) << "<h1>Memory Usage Detail</h1>\n";
    (*output) << "<table data-toggle='table' "
                 "       data-pagination='true' "
                 "       data-search='true' "
                 "       class='table table-striped'>\n";
    (*output) << "<thead><tr>"
                 "<th>level</th>"
                 "<th>Label</th>"
                 "<th>Parent</th>"
                 "<th>Limit</th>"
                 "<th data-sorter='bytesSorter' "
                 "    data-sortable='true' "
                 ">Current Consumption</th>"
                 "<th data-sorter='bytesSorter' "
                 "    data-sortable='true' "
                 ">Peak Consumption</th>";
    (*output) << "<tbody>\n";

    size_t upper_level;
    size_t cur_level;
    auto iter = args.find("upper_level");
    if (iter != args.end()) {
        upper_level = std::stol(iter->second);
    } else {
        upper_level = 2;
    }

    MemTracker* start_mem_tracker;
    iter = args.find("type");
    if (iter != args.end()) {
        if (iter->second == "compaction") {
            start_mem_tracker = ExecEnv::GetInstance()->compaction_mem_tracker();
            cur_level = 2;
        } else if (iter->second == "load") {
            start_mem_tracker = ExecEnv::GetInstance()->load_mem_tracker();
            cur_level = 2;
        } else if (iter->second == "metadata") {
            start_mem_tracker = ExecEnv::GetInstance()->metadata_mem_tracker();
            cur_level = 2;
        } else if (iter->second == "query_pool") {
            start_mem_tracker = ExecEnv::GetInstance()->query_pool_mem_tracker();
            cur_level = 2;
        } else if (iter->second == "schema_change") {
            start_mem_tracker = ExecEnv::GetInstance()->schema_change_mem_tracker();
            cur_level = 2;
        } else if (iter->second == "clone") {
            start_mem_tracker = ExecEnv::GetInstance()->clone_mem_tracker();
            cur_level = 2;
        } else if (iter->second == "column_pool") {
            start_mem_tracker = ExecEnv::GetInstance()->column_pool_mem_tracker();
            cur_level = 2;
        } else if (iter->second == "page_cache") {
            start_mem_tracker = ExecEnv::GetInstance()->page_cache_mem_tracker();
            cur_level = 2;
        } else if (iter->second == "update") {
            start_mem_tracker = ExecEnv::GetInstance()->update_mem_tracker();
            cur_level = 2;
        } else if (iter->second == "chunk_allocator") {
            start_mem_tracker = ExecEnv::GetInstance()->chunk_allocator_mem_tracker();
            cur_level = 2;
        } else if (iter->second == "consistency") {
            start_mem_tracker = ExecEnv::GetInstance()->consistency_mem_tracker();
            cur_level = 2;
        } else if (iter->second == "datacache") {
            start_mem_tracker = ExecEnv::GetInstance()->datacache_mem_tracker();
            cur_level = 2;
        } else {
            start_mem_tracker = mem_tracker;
            cur_level = 1;
        }
    } else {
        start_mem_tracker = mem_tracker;
        cur_level = 1;
    }

    std::vector<MemTracker::SimpleItem> items;

    // Metadata memory statistics use the old memory framework,
    // not in RootMemTrackerTree, so it needs to be added here
    MemTracker* meta_mem_tracker = ExecEnv::GetInstance()->metadata_mem_tracker();
    MemTracker::SimpleItem meta_item{"metadata",
                                     "process",
                                     2,
                                     meta_mem_tracker->limit(),
                                     meta_mem_tracker->consumption(),
                                     meta_mem_tracker->peak_consumption()};

    // Update memory statistics use the old memory framework,
    // not in RootMemTrackerTree, so it needs to be added here
    MemTracker* update_mem_tracker = ExecEnv::GetInstance()->update_mem_tracker();
    MemTracker::SimpleItem update_item{"update",
                                       "process",
                                       2,
                                       update_mem_tracker->limit(),
                                       update_mem_tracker->consumption(),
                                       update_mem_tracker->peak_consumption()};

    if (start_mem_tracker != nullptr) {
        start_mem_tracker->list_mem_usage(&items, cur_level, upper_level);
        if (start_mem_tracker == ExecEnv::GetInstance()->process_mem_tracker()) {
            items.emplace_back(meta_item);
            items.emplace_back(update_item);
        }

        for (const auto& item : items) {
            std::string level_str = ItoaKMGT(item.level);
            std::string limit_str = item.limit == -1 ? "none" : ItoaKMGT(item.limit);
            string current_consumption_str = ItoaKMGT(item.cur_consumption);
            string peak_consumption_str = ItoaKMGT(item.peak_consumption);
            (*output) << strings::Substitute(
                    "<tr><td>$0</td><td>$1</td><td>$2</td><td>$3</td><td>$4</td><td>$5</td></tr>\n", item.level,
                    item.label, item.parent_label, limit_str, current_consumption_str, peak_consumption_str);
        }
    }

    (*output) << "</tbody></table>\n";
}

#ifdef USE_JEMALLOC
void malloc_stats_write_cb(void* opaque, const char* data) {
    auto* buf = static_cast<std::string*>(opaque);
    buf->append(data);
}
#endif

// Registered to handle "/memz", and prints out memory allocation statistics.
void mem_usage_handler(MemTracker* mem_tracker, const WebPageHandler::ArgumentMap& args, std::stringstream* output) {
    if (mem_tracker != nullptr) {
        (*output) << "<pre>"
                  << "Mem Limit: " << PrettyPrinter::print(mem_tracker->limit(), TUnit::BYTES) << std::endl
                  << "Mem Consumption: " << PrettyPrinter::print(mem_tracker->consumption(), TUnit::BYTES) << std::endl
                  << "</pre>";
    } else {
        (*output) << "<pre>"
                  << "No process memory limit set."
                  << "</pre>";
    }

    (*output) << "<pre>";
#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER)
    (*output) << "Memory tracking is not available with address sanitizer builds.";
#elif defined(USE_JEMALLOC)
    std::string buf;
    je_malloc_stats_print(malloc_stats_write_cb, &buf, "a");
    boost::replace_all(buf, "\n", "<br>");
    (*output) << buf << "</pre>";
#else
    char buf[2048];
    MallocExtension::instance()->GetStats(buf, 2048);
    // Replace new lines with <br> for html
    std::string tmp(buf);
    boost::replace_all(tmp, "\n", "<br>");
    (*output) << tmp << "</pre>";
#endif
    (*output) << "<pre>";
    string stats = StorageEngine::instance()->update_manager()->detail_memory_stats();
    (*output) << stats << "</pre>";
}

void add_default_path_handlers(WebPageHandler* web_page_handler, MemTracker* process_mem_tracker) {
    // TODO(yingchun): logs_handler is not implemented yet, so not show it on navigate bar
    web_page_handler->register_page("/logs", "Logs", logs_handler, false /* is_on_nav_bar */);
    web_page_handler->register_page("/varz", "Configs", config_handler, true /* is_on_nav_bar */);
    web_page_handler->register_page(
            "/memz", "Memory",
            [process_mem_tracker](auto&& PH1, auto&& PH2) {
                return mem_usage_handler(process_mem_tracker, std::forward<decltype(PH1)>(PH1),
                                         std::forward<decltype(PH2)>(PH2));
            },
            true /* is_on_nav_bar */);
    web_page_handler->register_page(
            "/mem_tracker", "MemTracker",
            [process_mem_tracker](auto&& PH1, auto&& PH2) {
                return mem_tracker_handler(process_mem_tracker, std::forward<decltype(PH1)>(PH1),
                                           std::forward<decltype(PH2)>(PH2));
            },
            true);
}

} // namespace starrocks

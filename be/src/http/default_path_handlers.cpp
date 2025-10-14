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

#include <gutil/strings/numbers.h>
#include <gutil/strings/substitute.h>

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <cctype>
#include <filesystem>
#include <sstream>
#include <vector>

#include "common/configbase.h"
#include "http/action/profile_utils.h"
#include "http/web_page_handler.h"
#include "jemalloc/jemalloc.h"
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
    std::vector<config::ConfigInfo> configs = config::list_configs();
    (*output) << "<h2>Configurations</h2>";
    (*output) << "<pre>";
    for (const auto& cfg : configs) {
        (*output) << cfg.name << '=' << cfg.value << '\n';
    }
    (*output) << "</pre>";
}

void print_mem_str(std::stringstream* output, const MemTracker::SimpleItem& item) {
    std::string level_str = ItoaKMGT(item.level);
    std::string limit_str = item.limit == -1 ? "none" : ItoaKMGT(item.limit);
    string current_consumption_str = ItoaKMGT(item.cur_consumption);
    string peak_consumption_str = ItoaKMGT(item.peak_consumption);
    std::string parent_label;
    if (item.parent != nullptr) {
        parent_label = item.parent->label;
    }
    (*output) << strings::Substitute("<tr><td>$0</td><td>$1</td><td>$2</td><td>$3</td><td>$4</td><td>$5</td></tr>\n",
                                     item.level, item.label, parent_label, limit_str, current_consumption_str,
                                     peak_consumption_str);
    for (const auto* child : item.childs) {
        print_mem_str(output, *child);
    }
}

void MemTrackerWebPageHandler::handle(MemTracker* mem_tracker, const WebPageHandler::ArgumentMap& args,
                                      std::stringstream* output) {
    (*output) << "<h1>Memory Usage Detail</h1>\n";
    (*output) << "<table data-toggle='table' "
                 "       data-page-size='25' "
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
    auto iter = args.find("upper_level");
    if (iter != args.end()) {
        upper_level = std::stol(iter->second);
    } else {
        upper_level = 2;
    }

    MemTracker* start_mem_tracker;
    iter = args.find("type");
    if (iter != args.end()) {
        auto item = GlobalEnv::GetInstance()->get_mem_tracker_by_type(MemTracker::label_to_type(iter->second));
        if (item != nullptr) {
            start_mem_tracker = item.get();
        } else {
            start_mem_tracker = mem_tracker;
        }
    } else {
        start_mem_tracker = mem_tracker;
    }

    ObjectPool obj_pool;

    std::vector<MemTracker::SimpleItem> items;

    if (start_mem_tracker != nullptr) {
        MemTracker::SimpleItem* root = start_mem_tracker->get_snapshot(&obj_pool, upper_level);
        if (start_mem_tracker == GlobalEnv::GetInstance()->process_mem_tracker()) {
            // Metadata memory statistics use the old memory framework,
            // not in RootMemTrackerTree, so it needs to be added here
            MemTracker* meta_mem_tracker = GlobalEnv::GetInstance()->metadata_mem_tracker();
            auto* meta_item = meta_mem_tracker->get_snapshot(&obj_pool, upper_level);
            meta_item->parent = root;

            // Update memory statistics use the old memory framework,
            // not in RootMemTrackerTree, so it needs to be added here
            MemTracker* update_mem_tracker = GlobalEnv::GetInstance()->update_mem_tracker();
            auto* update_item = update_mem_tracker->get_snapshot(&obj_pool, upper_level);
            update_item->parent = root;

            root->childs.emplace_back(meta_item);
            root->childs.emplace_back(update_item);
        }

        print_mem_str(output, *root);
    }

    (*output) << "</tbody></table>\n";
}

void malloc_stats_write_cb(void* opaque, const char* data) {
    auto* buf = static_cast<std::string*>(opaque);
    buf->append(data);
}

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
#else
    std::string buf;
    je_malloc_stats_print(malloc_stats_write_cb, &buf, "a");
    boost::replace_all(buf, "\n", "<br>");
    (*output) << buf << "</pre>";
#endif
    (*output) << "<pre>";
    string stats = StorageEngine::instance()->update_manager()->detail_memory_stats();
    (*output) << stats << "</pre>";
}

void proc_profile_handler(const WebPageHandler::ArgumentMap& args, std::stringstream* output) {
    (*output) << "<h2>Process Profiles</h2>";
    (*output) << "<p>This page displays collected CPU and contention profiles from BRPC.</p>";
    (*output) << "<p>To collect profiles, use the <code>collect_be_profile.sh</code> script.</p>";
    (*output) << "<p>Profiles are stored in: <code>" << config::sys_log_dir << "/proc_profile</code></p>";

    // List profile files directly in this page
    std::string profile_log_dir = std::string(config::sys_log_dir) + "/proc_profile";
    std::filesystem::path dir_path(profile_log_dir);

    if (!std::filesystem::exists(dir_path) || !std::filesystem::is_directory(dir_path)) {
        (*output) << "<p><strong>No profile directory found:</strong> " << profile_log_dir << "</p>";
        return;
    }

    std::vector<std::pair<std::string, std::string>> profile_files; // filename, timestamp

    try {
        for (const auto& entry : std::filesystem::directory_iterator(dir_path)) {
            if (entry.is_regular_file()) {
                std::string filename = entry.path().filename().string();
                if (filename.ends_with(".gz")) {
                    // Extract timestamp from filename using utility function
                    std::string timestamp = ProfileUtils::extract_timestamp_from_filename(filename);
                    if (!timestamp.empty()) {
                        profile_files.emplace_back(filename, timestamp);
                    }
                }
            }
        }
    } catch (const std::filesystem::filesystem_error& e) {
        (*output) << "<p><strong>Error reading profile directory:</strong> " << e.what() << "</p>";
        return;
    }

    // Sort by timestamp descending (newest first)
    std::sort(profile_files.begin(), profile_files.end(),
              [](const std::pair<std::string, std::string>& a, const std::pair<std::string, std::string>& b) {
                  return a.second > b.second;
              });

    if (profile_files.empty()) {
        (*output) << "<p><strong>No profile files found.</strong></p>";
        return;
    }

    (*output) << "<h3>Available Profile Files</h3>";
    (*output) << "<table class=\"table table-hover table-bordered table-striped table-condensed\">";
    (*output) << "<thead><tr>";
    (*output) << "<th>Type</th>";
    (*output) << "<th>Format</th>";
    (*output) << "<th>Timestamp</th>";
    (*output) << "<th>File Size</th>";
    (*output) << "<th>Actions</th>";
    (*output) << "</tr></thead>";
    (*output) << "<tbody>";

    for (const auto& file_info : profile_files) {
        const std::string& filename = file_info.first;
        const std::string& timestamp = file_info.second;

        // Determine profile type and format using utility functions
        std::string profile_type = ProfileUtils::get_profile_type(filename);
        std::string format = ProfileUtils::get_profile_format(filename);

        // Get file size
        std::string file_path = profile_log_dir + "/" + filename;
        std::string file_size_str = "Unknown";
        try {
            auto file_size = std::filesystem::file_size(file_path);
            file_size_str = std::to_string(file_size) + " bytes";
        } catch (const std::filesystem::filesystem_error&) {
            // Keep "Unknown" if we can't get file size
        }

        (*output) << "<tr>";
        (*output) << "<td>" << profile_type << "</td>";
        (*output) << "<td>" << format << "</td>";
        (*output) << "<td>" << timestamp << "</td>";
        (*output) << "<td>" << file_size_str << "</td>";
        (*output) << "<td>";
        (*output) << "<a href=\"/proc_profile/file?filename=" << filename << R"(" target="_blank">View</a>)";

        (*output) << "</td>";
        (*output) << "</tr>";
    }

    (*output) << "</tbody>";
    (*output) << "</table>";
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
                return MemTrackerWebPageHandler::handle(process_mem_tracker, std::forward<decltype(PH1)>(PH1),
                                                        std::forward<decltype(PH2)>(PH2));
            },
            true);
    web_page_handler->register_page("/proc_profile", "Proc Profiles", proc_profile_handler, true);
}

} // namespace starrocks

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

#include "http/action/proc_profile_action.h"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <ctime>
#include <iomanip>

#include "common/config.h"
#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "http/action/profile_utils.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_method.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"

#if !(defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER))
#include <gperftools/profiler.h>
#endif

namespace starrocks {

const static std::string HEADER_JSON = "application/json";
const static std::string ACTION_KEY = "action";
const static std::string ACTION_LIST = "list";
const static std::string ACTION_COLLECT = "collect";

void ProcProfileAction::handle(HttpRequest* req) {
    VLOG_ROW << req->debug_string();
    const auto& action = req->param(ACTION_KEY);
    if (req->method() == HttpMethod::GET) {
        if (action == ACTION_LIST) {
            _handle_list(req);
        } else {
            _handle_error(req, strings::Substitute("Not support action: '$0'", action));
        }
    } else if (req->method() == HttpMethod::POST) {
        if (action == ACTION_COLLECT) {
            _handle_collect(req);
        } else {
            _handle_error(req, strings::Substitute("Not support action: '$0'", action));
        }
    } else {
        _handle_error(req,
                      strings::Substitute("Not support $0 method: '$1'", to_method_desc(req->method()), req->uri()));
    }
}

void ProcProfileAction::_handle_list(HttpRequest* req) {
    rapidjson::Document root;
    root.SetObject();
    rapidjson::Document::AllocatorType& allocator = root.GetAllocator();

    rapidjson::Value profiles(rapidjson::kArrayType);

    std::string profile_log_dir = std::string(config::sys_log_dir) + "/proc_profile";
    std::filesystem::path dir_path(profile_log_dir);

    if (!std::filesystem::exists(dir_path) || !std::filesystem::is_directory(dir_path)) {
        root.AddMember("profiles", profiles, allocator);
        rapidjson::StringBuffer strbuf;
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
        root.Accept(writer);
        req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
        HttpChannel::send_reply(req, HttpStatus::OK, strbuf.GetString());
        return;
    }

    std::vector<std::pair<std::string, std::string>> profile_files; // filename, timestamp

    try {
        for (const auto& entry : std::filesystem::directory_iterator(dir_path)) {
            if (entry.is_regular_file()) {
                std::string filename = entry.path().filename().string();
                // Only include files in the pprof format
                if (ProfileUtils::get_profile_format(filename) == "Pprof") {
                    // Extract timestamp from filename using utility function
                    std::string timestamp = ProfileUtils::extract_timestamp_from_filename(filename);
                    if (!timestamp.empty()) {
                        profile_files.emplace_back(filename, timestamp);
                    }
                }
            }
        }
    } catch (const std::filesystem::filesystem_error& e) {
        _handle_error(req, strings::Substitute("Error reading profile directory: $0", e.what()));
        return;
    }

    // Sort by timestamp descending (newest first)
    std::sort(profile_files.begin(), profile_files.end(),
              [](const std::pair<std::string, std::string>& a, const std::pair<std::string, std::string>& b) {
                  return a.second > b.second;
              });

    for (const auto& file_info : profile_files) {
        const std::string& filename = file_info.first;
        const std::string& timestamp = file_info.second;

        // Determine profile type and format using utility functions
        std::string profile_type = ProfileUtils::get_profile_type(filename);
        std::string format = ProfileUtils::get_profile_format(filename);

        // Get file size
        std::string file_path = profile_log_dir + "/" + filename;
        uint64_t file_size = 0;
        try {
            file_size = std::filesystem::file_size(file_path);
        } catch (const std::filesystem::filesystem_error&) {
            // Keep 0 if we can't get file size
        }

        rapidjson::Value profile(rapidjson::kObjectType);
        profile.AddMember("filename", rapidjson::Value(filename.c_str(), allocator), allocator);
        profile.AddMember("type", rapidjson::Value(profile_type.c_str(), allocator), allocator);
        profile.AddMember("format", rapidjson::Value(format.c_str(), allocator), allocator);
        profile.AddMember("timestamp", rapidjson::Value(timestamp.c_str(), allocator), allocator);
        profile.AddMember("size", file_size, allocator);

        profiles.PushBack(profile, allocator);
    }

    root.AddMember("profiles", profiles, allocator);

    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, strbuf.GetString());
}

void ProcProfileAction::_handle_collect(HttpRequest* req) {
    rapidjson::Document root;
    root.SetObject();
    rapidjson::Document::AllocatorType& allocator = root.GetAllocator();

#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER)
    root.AddMember("status", rapidjson::Value("error", allocator), allocator);
    root.AddMember("message", rapidjson::Value("Profiling is not available with address sanitizer builds.", allocator), allocator);
    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, strbuf.GetString());
    return;
#else
    // Parse parameters
    int seconds = 10; // default 10 seconds
    const std::string& seconds_str = req->param("seconds");
    if (!seconds_str.empty()) {
        seconds = std::atoi(seconds_str.c_str());
        if (seconds <= 0 || seconds > 3600) {
            seconds = 10;
        }
    }

    std::string profile_type = "both"; // default to both
    const std::string& type_str = req->param("type");
    if (!type_str.empty()) {
        if (type_str == "cpu" || type_str == "contention" || type_str == "both") {
            profile_type = type_str;
        }
    }

    std::string profile_log_dir = std::string(config::sys_log_dir) + "/proc_profile";
    std::filesystem::path dir_path(profile_log_dir);
    if (!std::filesystem::exists(dir_path)) {
        std::filesystem::create_directories(dir_path);
    }

    // Generate timestamp
    auto now = std::time(nullptr);
    std::tm* timeinfo = std::localtime(&now);
    std::ostringstream timestamp_stream;
    timestamp_stream << std::put_time(timeinfo, "%Y%m%d-%H%M%S");
    std::string timestamp = timestamp_stream.str();

    std::vector<std::string> collected_files;

    // Collect CPU profile
    if (profile_type == "cpu" || profile_type == "both") {
        try {
            std::ostringstream tmp_prof_file_name;
            tmp_prof_file_name << config::pprof_profile_dir << "/starrocks_profile." << getpid() << "." << rand();
            std::string tmp_file = tmp_prof_file_name.str();

            ProfilerStart(tmp_file.c_str());
            sleep(seconds);
            ProfilerStop();

            // Read the profile data and compress it
            std::ifstream prof_file(tmp_file, std::ios::in | std::ios::binary);
            if (prof_file.is_open()) {
                // Save to proc_profile directory
                std::string output_filename = "cpu-profile-" + timestamp + "-pprof.gz";
                std::string output_path = profile_log_dir + "/" + output_filename;

                // Use gzip command to compress the file directly
                std::string gzip_cmd = "gzip -c '" + tmp_file + "' > '" + output_path + "'";
                int result = system(gzip_cmd.c_str());
                if (result == 0 && std::filesystem::exists(output_path)) {
                    collected_files.push_back(output_filename);
                } else {
                    LOG(WARNING) << "Failed to compress profile file: " << output_path;
                }

                prof_file.close();
                // Clean up temp file
                std::remove(tmp_file.c_str());
            }
        } catch (const std::exception& e) {
            LOG(WARNING) << "Failed to collect CPU profile: " << e.what();
        }
    }

    // Collect contention profile (similar to memory profiling)
    if (profile_type == "contention" || profile_type == "both") {
        // For contention, we use the same approach but with a different endpoint
        // Since ContentionAction::handle is empty, we'll skip it for now
        // and just note that contention profiling needs to be implemented
        LOG(INFO) << "Contention profile collection requested but not yet implemented";
    }

    if (collected_files.empty()) {
        root.AddMember("status", rapidjson::Value("error", allocator), allocator);
        root.AddMember("message", rapidjson::Value("Failed to collect any profiles", allocator), allocator);
    } else {
        root.AddMember("status", rapidjson::Value("success", allocator), allocator);
        std::string message = "Profile collection completed. Collected " + std::to_string(collected_files.size()) + " file(s)";
        root.AddMember("message", rapidjson::Value(message.c_str(), allocator), allocator);
        rapidjson::Value files(rapidjson::kArrayType);
        for (const auto& file : collected_files) {
            files.PushBack(rapidjson::Value(file.c_str(), allocator), allocator);
        }
        root.AddMember("files", files, allocator);
    }

    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, strbuf.GetString());
#endif
}

void ProcProfileAction::_handle_error(HttpRequest* req, const std::string& error_msg) {
    rapidjson::Document root;
    root.SetObject();
    rapidjson::Document::AllocatorType& allocator = root.GetAllocator();
    root.AddMember("error", rapidjson::Value(error_msg.c_str(), allocator), allocator);

    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, strbuf.GetString());
}

} // end namespace starrocks

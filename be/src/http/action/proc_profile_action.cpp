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
#include <string>
#include <vector>

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

namespace starrocks {

const static std::string HEADER_JSON = "application/json";
const static std::string ACTION_KEY = "action";
const static std::string ACTION_LIST = "list";

void ProcProfileAction::handle(HttpRequest* req) {
    VLOG_ROW << req->debug_string();
    const auto& action = req->param(ACTION_KEY);
    if (req->method() == HttpMethod::GET) {
        if (action == ACTION_LIST) {
            _handle_list(req);
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

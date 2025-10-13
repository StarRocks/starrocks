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

#include <algorithm>
#include <ctime>
#include <filesystem>
#include <sstream>
#include <string>
#include <vector>

#include "common/config.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"

namespace starrocks {

const static std::string HEADER_HTML = "text/html; charset=utf-8";

ProcProfileAction::ProcProfileAction(ExecEnv* exec_env) : _exec_env(exec_env) {}

void ProcProfileAction::handle(HttpRequest* req) {
    std::stringstream ss;
    get_page_header(req, &ss);
    add_profile_list_info(&ss);
    get_page_footer(&ss);

    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_HTML.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, ss.str());
}

void ProcProfileAction::get_page_header(HttpRequest* req, std::stringstream* output) {
    *output << "<!DOCTYPE html>";
    *output << "<html>";
    *output << "<head>";
    *output << "<title>StarRocks BE - Process Profiles</title>";
    *output << "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" >";
    *output << "<link href=\"/static/css?res=bootstrap.css\" rel=\"stylesheet\" media=\"screen\"/>";
    *output << "<link href=\"/static/css?res=bootstrap-theme.css\" rel=\"stylesheet\" media=\"screen\"/>";
    *output << "<link href=\"/static/css?res=datatables_bootstrap.css\" rel=\"stylesheet\" media=\"screen\"/>";
    *output << "<script type=\"text/javascript\" src=\"/static?res=jquery.js\"></script>";
    *output << "<script type=\"text/javascript\" src=\"/static?res=jquery.dataTables.js\"></script>";
    *output << "<script type=\"text/javascript\" src=\"/static?res=datatables_bootstrap.js\"></script>";
    *output << "<script type=\"text/javascript\">";
    *output << "$(document).ready(function() {";
    *output << "$('#table_id').dataTable({";
    *output << "\"aaSorting\": [],";
    *output << "\"lengthMenu\": [[10, 25, 50, 100,-1], [10, 25, 50, 100, \"All\"]],";
    *output << "\"iDisplayLength\": 50";
    *output << "});";
    *output << "});";
    *output << "</script>";
    *output << "</head>";
    *output << "<body>";
    *output << "<div class=\"container-fluid\">";
    *output << "<h2>Process Profiles</h2>";
    *output << "<p>This table lists all available CPU and contention profile files collected from BRPC</p>";
}

void ProcProfileAction::get_page_footer(std::stringstream* output) {
    *output << "</div>";
    *output << "</body>";
    *output << "</html>";
}

void ProcProfileAction::add_profile_list_info(std::stringstream* output) {
    std::vector<ProfileFileInfo> profile_files = get_profile_files();

    if (profile_files.empty()) {
        *output << "<p>No profile files found in directory: " << config::sys_log_dir << "/proc_profile</p>";
        return;
    }

    append_profile_table_header(output);
    append_profile_table_body(output, profile_files);
    append_table_footer(output);
}

std::vector<ProcProfileAction::ProfileFileInfo> ProcProfileAction::get_profile_files() {
    std::vector<ProfileFileInfo> profile_files;
    std::string profile_log_dir = std::string(config::sys_log_dir) + "/proc_profile";
    std::filesystem::path dir_path(profile_log_dir);

    if (!std::filesystem::exists(dir_path) || !std::filesystem::is_directory(dir_path)) {
        LOG(WARNING) << "Profile directory does not exist: " << profile_log_dir;
        return profile_files;
    }

    try {
        for (const auto& entry : std::filesystem::directory_iterator(dir_path)) {
            if (entry.is_regular_file()) {
                std::string filename = entry.path().filename().string();
                if (filename.ends_with(".tar.gz")) {
                    std::string profile_type = get_profile_type(filename);
                    if (!profile_type.empty()) {
                        std::string time_part = get_time_part(filename);
                        std::string format = get_format_part(filename);
                        if (!time_part.empty() && !format.empty()) {
                            ProfileFileInfo info;
                            info.type = profile_type;
                            info.timestamp = time_part;
                            info.file_size = std::filesystem::file_size(entry.path());
                            info.filename = filename;
                            info.format = format;
                            profile_files.push_back(info);
                        }
                    }
                }
            }
        }
    } catch (const std::filesystem::filesystem_error& e) {
        LOG(WARNING) << "Error reading profile directory: " << e.what();
        return profile_files;
    }

    // Sort by timestamp descending (newest first)
    std::sort(profile_files.begin(), profile_files.end(),
              [](const ProfileFileInfo& a, const ProfileFileInfo& b) { return a.timestamp > b.timestamp; });

    return profile_files;
}

std::string ProcProfileAction::get_profile_type(const std::string& filename) {
    if (filename.starts_with("cpu-profile-")) {
        return "CPU";
    } else if (filename.starts_with("contention-profile-")) {
        return "Contention";
    }
    return "";
}

std::string ProcProfileAction::get_time_part(const std::string& filename) {
    // Extract timestamp from filename like: cpu-profile-20231201-143022-flame.html.tar.gz
    size_t start_pos = filename.find('-', 0);
    if (start_pos == std::string::npos) return "";

    start_pos = filename.find('-', start_pos + 1);
    if (start_pos == std::string::npos) return "";
    start_pos++; // Skip the '-'

    size_t end_pos = filename.find('-', start_pos);
    if (end_pos == std::string::npos) return "";

    return filename.substr(start_pos, end_pos - start_pos);
}

std::string ProcProfileAction::get_format_part(const std::string& filename) {
    if (filename.find("-flame.html.tar.gz") != std::string::npos) {
        return "flame";
    } else if (filename.find("-pprof.tar.gz") != std::string::npos) {
        return "pprof";
    }
    return "";
}

void ProcProfileAction::append_profile_table_header(std::stringstream* output) {
    *output << "<table id=\"table_id\" class=\"table table-hover table-bordered table-striped table-condensed\">";
    *output << "<thead><tr>";
    *output << "<th>Type</th>";
    *output << "<th>Format</th>";
    *output << "<th>Timestamp</th>";
    *output << "<th>File Size</th>";
    *output << "<th>Actions</th>";
    *output << "</tr></thead>";
}

void ProcProfileAction::append_profile_table_body(std::stringstream* output,
                                                  const std::vector<ProfileFileInfo>& profile_files) {
    *output << "<tbody>";

    for (const auto& profile : profile_files) {
        *output << "<tr>"
                << "<td>" << profile.type << "</td>"
                << "<td>" << profile.format << "</td>"
                << "<td>" << profile.timestamp << "</td>"
                << "<td>" << std::to_string(profile.file_size) << " bytes</td>"
                << "<td>";

        if (profile.format == "flame") {
            *output << "<a href=\"/proc_profile/file?filename=" << profile.filename << "\" target=\"_blank\">View</a>";
        } else {
            *output << "<a href=\"/proc_profile/file?filename=" << profile.filename << "\">Download</a>";
        }

        *output << "</td>"
                << "</tr>";
    }

    *output << "</tbody>";
}

void ProcProfileAction::append_table_footer(std::stringstream* output) {
    *output << "</table>";
}

} // end namespace starrocks

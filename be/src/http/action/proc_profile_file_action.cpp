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

#include "http/action/proc_profile_file_action.h"

#include <unistd.h>

#include <fstream>
#include <string>
#include <vector>

#include "common/config.h"
#include "http/action/profile_utils.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"

namespace starrocks {

const static std::string HEADER_HTML = "text/html; charset=utf-8";
const static std::string HEADER_BINARY = "application/octet-stream";

ProcProfileFileAction::ProcProfileFileAction(ExecEnv* exec_env) : _exec_env(exec_env) {}

void ProcProfileFileAction::handle(HttpRequest* req) {
    const std::string& filename = req->param("filename");

    LOG(INFO) << "ProcProfileFileAction: Handling request for filename: " << filename;

    if (filename.empty()) {
        LOG(ERROR) << "ProcProfileFileAction: Missing filename parameter";
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "Missing filename parameter");
        return;
    }

    // Validate filename security
    if (!is_valid_filename(filename)) {
        LOG(ERROR) << "ProcProfileFileAction: Invalid filename: " << filename;
        HttpChannel::send_reply(req, HttpStatus::FORBIDDEN, "Invalid filename");
        return;
    }

    std::string profile_log_dir = std::string(config::sys_log_dir) + "/proc_profile";
    std::string profile_file_path = profile_log_dir + "/" + filename;

    LOG(INFO) << "ProcProfileFileAction: Looking for file at: " << profile_file_path;

    std::ifstream file(profile_file_path, std::ios::binary);
    if (!file.good()) {
        LOG(ERROR) << "ProcProfileFileAction: Profile file not found: " << profile_file_path;
        HttpChannel::send_reply(req, HttpStatus::NOT_FOUND, "Profile file not found");
        return;
    }

    try {
        std::string format = ProfileUtils::get_profile_format(filename);
        LOG(INFO) << "ProcProfileFileAction: Detected format: " << format << " for file: " << filename;

        if (format == "Flame") {
            LOG(INFO) << "ProcProfileFileAction: Serving gzipped HTML content for: " << filename;
            // Serve gzipped HTML content with proper headers
            serve_gzipped_html(req, profile_file_path);
        } else if (format == "Pprof") {
            LOG(INFO) << "ProcProfileFileAction: Serving gzipped binary file for: " << filename;
            // Serve gz file directly for pprof files
            serve_gz_file(req, profile_file_path);
        } else {
            LOG(ERROR) << "ProcProfileFileAction: Unsupported profile file format: " << filename;
            HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "Unsupported profile file format");
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "ProcProfileFileAction: Error serving profile file: " << filename << ", error: " << e.what();
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, "Error serving profile file");
    }
}

bool ProcProfileFileAction::is_valid_filename(const std::string& filename) {
    if (filename.empty()) {
        return false;
    }

    // Check for directory traversal attempts
    if (filename.find("..") != std::string::npos || filename.find('/') != std::string::npos ||
        filename.find('\\') != std::string::npos) {
        return false;
    }

    // Use ProfileUtils to validate the filename format
    std::string profile_type = ProfileUtils::get_profile_type(filename);
    std::string profile_format = ProfileUtils::get_profile_format(filename);

    // Must be a valid profile type and format
    if (profile_type == "Unknown" || profile_format == "Unknown") {
        return false;
    }

    return true;
}

void ProcProfileFileAction::serve_gzipped_html(HttpRequest* req, const std::string& file_path) {
    // Serve the gzipped HTML file directly with proper Content-Encoding header
    std::ifstream file(file_path, std::ios::binary);
    if (!file.good()) {
        HttpChannel::send_reply(req, HttpStatus::NOT_FOUND, "File not found");
        return;
    }

    // Read file content
    file.seekg(0, std::ios::end);
    size_t file_size = file.tellg();
    file.seekg(0, std::ios::beg);

    std::vector<char> buffer(file_size);
    file.read(buffer.data(), file_size);
    file.close();

    // Set headers for gzipped HTML content
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_HTML.c_str());
    req->add_output_header(HttpHeaders::CONTENT_ENCODING, "gzip");
    req->add_output_header(HttpHeaders::CONTENT_LENGTH, std::to_string(file_size).c_str());

    HttpChannel::send_reply(req, HttpStatus::OK, std::string(buffer.data(), file_size));
}

void ProcProfileFileAction::serve_gz_file(HttpRequest* req, const std::string& file_path) {
    std::ifstream file(file_path, std::ios::binary);
    if (!file.good()) {
        HttpChannel::send_reply(req, HttpStatus::NOT_FOUND, "File not found");
        return;
    }

    // Read file content
    file.seekg(0, std::ios::end);
    size_t file_size = file.tellg();
    file.seekg(0, std::ios::beg);

    std::vector<char> buffer(file_size);
    file.read(buffer.data(), file_size);

    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_BINARY.c_str());
    req->add_output_header(HttpHeaders::CONTENT_LENGTH, std::to_string(file_size).c_str());

    HttpChannel::send_reply(req, HttpStatus::OK, std::string(buffer.data(), file_size));
}

} // end namespace starrocks

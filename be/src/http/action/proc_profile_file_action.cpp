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

#include <fstream>
#include <sstream>
#include <string>

#include "common/config.h"
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

    if (filename.empty()) {
        LOG(ERROR) << "Missing filename parameter";
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "Missing filename parameter");
        return;
    }

    // Validate filename security
    if (!is_valid_filename(filename)) {
        LOG(ERROR) << "Invalid filename: " << filename;
        HttpChannel::send_reply(req, HttpStatus::FORBIDDEN, "Invalid filename");
        return;
    }

    std::string profile_log_dir = std::string(config::sys_log_dir) + "/proc_profile";
    std::string profile_file_path = profile_log_dir + "/" + filename;

    std::ifstream file(profile_file_path, std::ios::binary);
    if (!file.good()) {
        LOG(ERROR) << "Profile file not found: " << profile_file_path;
        HttpChannel::send_reply(req, HttpStatus::NOT_FOUND, "Profile file not found");
        return;
    }

    try {
        if (filename.find("-flame.html.tar.gz") != std::string::npos) {
            // Extract and serve HTML content
            std::string html_content = extract_html_from_tar_gz(profile_file_path);
            if (html_content.empty()) {
                LOG(ERROR) << "Failed to extract HTML content from: " << filename;
                HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, "Failed to extract HTML content");
                return;
            }

            req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_HTML.c_str());
            HttpChannel::send_reply(req, HttpStatus::OK, html_content);
        } else {
            // Serve tar.gz file directly for pprof files
            serve_tar_gz_file(req, profile_file_path);
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "Error serving profile file: " << filename << ", error: " << e.what();
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

    // Check for dangerous characters
    if (filename.find('<') != std::string::npos || filename.find('>') != std::string::npos ||
        filename.find('&') != std::string::npos || filename.find('"') != std::string::npos) {
        return false;
    }

    // Must be a valid profile file
    if (!filename.ends_with(".tar.gz")) {
        return false;
    }

    // Must start with known prefixes
    if (!filename.starts_with("cpu-profile-") && !filename.starts_with("contention-profile-")) {
        return false;
    }

    return true;
}

std::string ProcProfileFileAction::extract_html_from_tar_gz(const std::string& file_path) {
    // For now, we'll implement a simple approach that reads the tar.gz file
    // In a production environment, you might want to use a proper tar.gz library
    // This is a simplified implementation that assumes the HTML content is at the beginning

    std::ifstream file(file_path, std::ios::binary);
    if (!file.good()) {
        return "";
    }

    // Read the entire file into memory
    file.seekg(0, std::ios::end);
    size_t file_size = file.tellg();
    file.seekg(0, std::ios::beg);

    std::vector<char> buffer(file_size);
    file.read(buffer.data(), file_size);

    // For this simplified implementation, we'll return a placeholder
    // In a real implementation, you would decompress the tar.gz and extract the HTML
    std::stringstream ss;
    ss << R"(<!DOCTYPE html>
<html><head><title>Profile Viewer</title></head>
<body>
<h1>Profile File: )"
       << file_path << R"(</h1>
<p>This is a placeholder for the extracted HTML content.</p>
<p>File size: )"
       << file_size << R"( bytes</p>
<p>Note: Full tar.gz extraction not implemented in this demo.</p>
</body></html>)";

    return ss.str();
}

void ProcProfileFileAction::serve_tar_gz_file(HttpRequest* req, const std::string& file_path) {
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

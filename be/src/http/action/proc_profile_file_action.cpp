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

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "fmt/format.h"
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

    // Validate filename security
    if (!is_valid_filename(filename)) {
        LOG(WARNING) << "ProcProfileFileAction: Invalid filename: " << filename;
        HttpChannel::send_reply(req, HttpStatus::FORBIDDEN, "Invalid filename");
        return;
    }

    std::string profile_log_dir = std::string(config::sys_log_dir) + "/proc_profile";
    std::string profile_file_path = profile_log_dir + "/" + filename;
    std::filesystem::path path(profile_file_path);

    if (!std::filesystem::exists(path) || !std::filesystem::is_regular_file(path)) {
        LOG(WARNING) << "ProcProfileFileAction: Profile file not found: " << path;
        HttpChannel::send_reply(req, HttpStatus::NOT_FOUND, "Profile file not found");
        return;
    }

    try {
        std::string format = ProfileUtils::get_profile_format(filename);
        if (format == "Pprof") {
            // Convert pprof to flame format and serve as HTML
            serve_pprof_as_flame(req, profile_file_path);
        } else {
            serve_gz_file(req, profile_file_path);
        }
    } catch (const std::exception& e) {
        LOG(WARNING) << "ProcProfileFileAction: Error serving profile file: " << filename << ", error: " << e.what();
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

void ProcProfileFileAction::serve_pprof_as_flame(HttpRequest* req, const std::string& file_path) {
    // Check if required tools are available
    std::string pprof_path = config::flamegraph_tool_dir + "/pprof";
    std::string stackcollapse_path = config::flamegraph_tool_dir + "/stackcollapse-go.pl";
    std::string flamegraph_path = config::flamegraph_tool_dir + "/flamegraph.pl";

    if (!std::filesystem::exists(pprof_path) || !std::filesystem::is_regular_file(pprof_path)) {
        LOG(WARNING) << "ProcProfileFileAction: pprof tool not found at " << pprof_path
                     << ", falling back to serving raw pprof file";
        serve_gz_file(req, file_path);
        return;
    }

    if (!std::filesystem::exists(flamegraph_path) || !std::filesystem::is_regular_file(flamegraph_path)) {
        LOG(WARNING) << "ProcProfileFileAction: FlameGraph tools not found at " << flamegraph_path
                     << ", falling back to serving raw pprof file";
        serve_gz_file(req, file_path);
        return;
    }

    if (!std::filesystem::exists(stackcollapse_path) || !std::filesystem::is_regular_file(stackcollapse_path)) {
        LOG(WARNING) << "ProcProfileFileAction: stackcollapse-go.pl not found at " << stackcollapse_path
                     << ", falling back to serving raw pprof file";
        serve_gz_file(req, file_path);
        return;
    }

    std::string flame_svg_content;
    Status status = convert_pprof_to_flame(file_path, flame_svg_content);
    if (!status.ok()) {
        LOG(WARNING) << "ProcProfileFileAction: Failed to convert pprof to flame format: " << status.to_string();
        serve_gz_file(req, file_path);
        return;
    }

    // Set headers for gzipped SVG content
    size_t file_size = flame_svg_content.size();
    req->add_output_header(HttpHeaders::CONTENT_TYPE, "image/svg+xml; charset=utf-8");
    req->add_output_header(HttpHeaders::CONTENT_ENCODING, "gzip");
    req->add_output_header(HttpHeaders::CONTENT_LENGTH, std::to_string(file_size).c_str());

    LOG(INFO) << "ProcProfileFileAction: Successfully converted pprof to flame format and compressed, serving "
              << file_size << " bytes";
    HttpChannel::send_reply(req, HttpStatus::OK, flame_svg_content);
}

Status ProcProfileFileAction::convert_pprof_to_flame(const std::string& pprof_file_path,
                                                     std::string& flame_svg_content) {
    LOG(INFO) << "ProcProfileFileAction: Converting pprof to flame format: " << pprof_file_path;

    // Generate the corresponding flame file path in proc_profile directory
    std::string pprof_filename = std::filesystem::path(pprof_file_path).filename().string();
    std::string flame_filename = pprof_filename;
    // Replace -pprof.gz with -flame.html.gz
    size_t pprof_pos = flame_filename.find("-pprof.gz");
    if (pprof_pos != std::string::npos) {
        flame_filename.replace(pprof_pos, 9, "-flame.html.gz");
    }

    std::string profile_log_dir = std::string(config::sys_log_dir) + "/proc_profile";
    std::string cached_flame_file = profile_log_dir + "/" + flame_filename;

    // Check if cached flame file already exists
    std::ifstream cached_file(cached_flame_file, std::ios::binary);
    if (cached_file.good()) {
        LOG(INFO) << "ProcProfileFileAction: Found cached flame file: " << cached_flame_file;

        // Read cached file content
        cached_file.seekg(0, std::ios::end);
        size_t file_size = cached_file.tellg();
        cached_file.seekg(0, std::ios::beg);

        flame_svg_content.resize(file_size);
        cached_file.read(flame_svg_content.data(), file_size);
        cached_file.close();

        LOG(INFO) << "ProcProfileFileAction: Loaded cached flame file, " << file_size << " bytes";
        return {};
    }

    // Create temporary files for the conversion process
    std::string temp_pprof = "/tmp/pprof_" + std::to_string(getpid()) + "_" + std::to_string(time(nullptr)) + ".pprof";
    std::string temp_flame = "/tmp/flame_" + std::to_string(getpid()) + "_" + std::to_string(time(nullptr)) + ".svg";
    std::string temp_flame_gz = temp_flame + ".gz";

    try {
        // First, decompress the gzipped pprof file to a temporary location
        std::string gunzip_cmd = "gunzip -c '" + pprof_file_path + "' > '" + temp_pprof + "'";
        int gunzip_result = system(gunzip_cmd.c_str());
        RETURN_IF(gunzip_result != 0, Status::InternalError("Failed to decompress pprof file, gunzip returned: " +
                                                            std::to_string(gunzip_result)));

        std::string pprof_path = config::flamegraph_tool_dir + "/pprof";
        std::string stackcollapse_path = config::flamegraph_tool_dir + "/stackcollapse-go.pl";
        std::string flamegraph_path = config::flamegraph_tool_dir + "/flamegraph.pl";
        // Convert pprof to flame format using the command: pprof -raw cpu.pprof | stackcollapse-go.pl | flamegraph.pl > flame.svg
        std::string pprof_cmd =
                fmt::format("{} -symbolize=fastlocal -raw '{}' 2>/dev/null | {} 2>/dev/null | {} > '{}' 2>/dev/null",
                            pprof_path, temp_pprof, stackcollapse_path, flamegraph_path, temp_flame);

        int pprof_result = system(pprof_cmd.c_str());
        RETURN_IF(pprof_result != 0, Status::InternalError("Failed to convert pprof to flame format, pprof returned: " +
                                                           std::to_string(pprof_result)));

        // Compress the generated flame SVG file
        std::string gzip_cmd = "gzip -c '" + temp_flame + "' > '" + temp_flame_gz + "'";

        int gzip_result = system(gzip_cmd.c_str());
        RETURN_IF(gzip_result != 0, Status::InternalError("Failed to compress flame SVG file, gzip returned: " +
                                                          std::to_string(gzip_result)));

        // Read the compressed flame SVG file
        std::ifstream flame_gz_file(temp_flame_gz, std::ios::binary);
        RETURN_IF(!flame_gz_file.good(),
                  Status::InternalError("Failed to read compressed flame file: " + temp_flame_gz));

        // Read compressed file content
        flame_gz_file.seekg(0, std::ios::end);
        size_t file_size = flame_gz_file.tellg();
        flame_gz_file.seekg(0, std::ios::beg);

        RETURN_IF(file_size == 0, Status::InternalError("Compressed flame file is empty"));

        flame_svg_content.resize(file_size);
        flame_gz_file.read(flame_svg_content.data(), file_size);
        flame_gz_file.close();

        // Save the converted flame file to cache directory
        std::ofstream cache_file(cached_flame_file, std::ios::binary);
        if (cache_file.good()) {
            cache_file.write(flame_svg_content.data(), file_size);
            cache_file.close();
        } else {
            LOG(WARNING) << "ProcProfileFileAction: Failed to save cached flame file to: " << cached_flame_file;
        }

        // Clean up temporary files
        unlink(temp_pprof.c_str());
        unlink(temp_flame.c_str());
        unlink(temp_flame_gz.c_str());

        return {};

    } catch (const std::exception& e) {
        return Status::InternalError("Exception during pprof to flame conversion: " + std::string(e.what()));
    }
}

} // end namespace starrocks

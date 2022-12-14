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

#include "common/minidump.h"

#include <client/linux/handler/exception_handler.h>
#include <common/linux/linux_libc_support.h>
#include <glob.h>
#include <google_breakpad/common/minidump_format.h>

#include <csignal>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <sstream>
#include <system_error>

#include "common/config.h"
#include "util/logging.h"

namespace starrocks {
void Minidump::init() {
    get_instance();
}

Minidump& Minidump::get_instance() {
    // Singleton Pattern to ensure just one instance to generate minidump.
    static Minidump instance;
    return instance;
}

/// Signal handler to write a minidump file outside of crashes.
void Minidump::handle_signal(int signal) {
    //google_breakpad::ExceptionHandler::WriteMinidump(get_instance()._path, Minidump::dump_callback, NULL);
    google_breakpad::MinidumpDescriptor descriptor(get_instance()._minidump_dir);

    // Set size limit for generated minidumps
    size_t size_limit = 1024 * static_cast<int64_t>(config::sys_minidump_limit);
    descriptor.set_size_limit(size_limit);

    google_breakpad::ExceptionHandler eh(descriptor, nullptr, Minidump::dump_callback, nullptr, false, -1);
    eh.WriteMinidump();
}

Minidump::Minidump() : _minidump(), _minidump_dir(config::sys_minidump_dir) {
    // Clean old minidumps
    check_and_rotate_minidumps(config::sys_minidump_max_files, _minidump_dir);

    google_breakpad::MinidumpDescriptor descriptor(_minidump_dir);

    // Set size limit for generated minidumps
    size_t size_limit = 1024 * static_cast<int64_t>(config::sys_minidump_limit);
    descriptor.set_size_limit(size_limit);

    // Step 1: use breakpad to generate minidump caused by crash.
    _minidump = std::make_unique<google_breakpad::ExceptionHandler>(descriptor, Minidump::filter_callback,
                                                                    Minidump::dump_callback, nullptr, true, -1);

    // Step 2: write minidump as reactive to SIGUSR1.
    struct sigaction signal_action;
    memset(&signal_action, 0, sizeof(signal_action));
    sigemptyset(&signal_action.sa_mask);
    signal_action.sa_handler = Minidump::handle_signal;
    // kill -10 pid
    sigaction(SIGUSR1, &signal_action, nullptr);
}

// This Implementations for clean oldest and malformed files is modified from IMPALA.
void Minidump::check_and_rotate_minidumps(int max_minidumps, const std::string& minidump_dir) {
    if (max_minidumps <= 0) return;

    // Search for minidumps. There could be multiple minidumps for a single second.
    std::multimap<int, std::filesystem::path> timestamp_to_path;
    // For example: 2b1af619-8a02-49d7-72652c8d-a9b32de1.dmp.
    string pattern = minidump_dir + "/*.dmp";

    glob_t result;
    glob(pattern.c_str(), GLOB_TILDE, nullptr, &result);
    for (size_t i = 0; i < result.gl_pathc; ++i) {
        const std::filesystem::path minidump_path(result.gl_pathv[i]);
        std::error_code err;
        bool is_file = std::filesystem::is_regular_file(minidump_path, err);
        // std::filesystem::is_regular_file() calls stat() eventually, which can return errors, e.g. if the
        // file permissions prevented access or the path was wrong (see 'man 2 stat' for
        // details). In these cases we assume that the issue is out of our control and err on
        // the safe side by keeping the minidump around, hoping it will aid in debugging the
        // issue. The alternative, removing a ~2MB file, will probably not help much anyways.
        if (err) {
            LOG(WARNING) << "Failed to stat() file " << minidump_path << ": " << err;
            continue;
        }
        if (is_file) {
            std::ifstream stream(minidump_path.c_str(), std::ios::in | std::ios::binary);
            if (!stream.good()) {
                // Error opening file, probably broken, remove it.
                LOG(WARNING) << "Failed to open file " << minidump_path << ". Removing it.";
                stream.close();
                // Best effort, ignore error.
                std::filesystem::remove(minidump_path.c_str(), err);
                continue;
            }
            // Read minidump header from file.
            MDRawHeader header;
            constexpr int header_size = sizeof(header);
            stream.read((char*)(&header), header_size);
            // Check for minidump header signature and version. We don't need to check for
            // endianness issues here since the file was written on the same machine. Ignore the
            // higher 16 bit of the version as per a comment in the breakpad sources.
            if (stream.gcount() != header_size || header.signature != MD_HEADER_SIGNATURE ||
                (header.version & 0x0000ffff) != MD_HEADER_VERSION) {
                LOG(WARNING) << "Found file in minidump folder, but it does not look like a "
                             << "minidump file: " << minidump_path.string() << ". Removing it.";
                std::filesystem::remove(minidump_path, err);
                if (err) {
                    LOG(ERROR) << "Failed to delete file: " << minidump_path << "(error was: " << err << ")";
                }
                continue;
            }
            int timestamp = header.time_date_stamp;
            timestamp_to_path.emplace(timestamp, minidump_path);
        }
    }
    globfree(&result);

    // Remove oldest entries until max_minidumps are left.
    if (timestamp_to_path.size() <= max_minidumps) return;
    int files_to_delete = timestamp_to_path.size() - max_minidumps;
    DCHECK_GT(files_to_delete, 0);
    auto to_delete = timestamp_to_path.begin();
    for (int i = 0; i < files_to_delete; ++i, ++to_delete) {
        std::error_code err;
        std::filesystem::remove(to_delete->second, err);
        if (!err) {
            LOG(INFO) << "Removed old minidump file : " << to_delete->second;
        } else {
            LOG(ERROR) << "Failed to delete old minidump file: " << to_delete->second << "(error was: " << err << ")";
        }
    }
}

bool Minidump::dump_callback(const google_breakpad::MinidumpDescriptor& descriptor, void* context, bool succeeded) {
    // Output minidump file path
    if (succeeded) {
        // Write message to stdout/stderr
        const char msg[] = "Dump path: ";
        const int msg_len = sizeof(msg) / sizeof(msg[0]) - 1;
        const char* path = descriptor.path();
        // We use breakpad's reimplementation of strlen to avoid calling into libc.
        const int path_len = my_strlen(path);
        // We use the linux syscall support methods from chromium here as per the
        // recommendation of the breakpad docs to avoid calling into other shared libraries.
        sys_write(STDOUT_FILENO, msg, msg_len);
        sys_write(STDOUT_FILENO, path, path_len);
        sys_write(STDOUT_FILENO, "\n", 1);
    }
    return succeeded;
}

bool Minidump::filter_callback(void* context) {
    return true;
}
} // namespace starrocks

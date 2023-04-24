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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/utils.cpp

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

#include "storage/utils.h"

#include <bvar/bvar.h>
#include <dirent.h>
#include <fmt/format.h>
#include <lz4/lz4.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <boost/regex.hpp>
#include <cerrno>
#include <chrono>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <mutex>
#include <string>
#include <vector>

#include "common/logging.h"
#include "common/status.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "gutil/strings/substitute.h"
#include "storage/olap_define.h"
#include "util/errno.h"
#include "util/string_parser.hpp"

using std::string;
using std::set;
using std::vector;

namespace starrocks {

static bvar::LatencyRecorder g_move_trash("starrocks", "move_to_trash");

uint32_t olap_adler32(uint32_t adler, const char* buf, size_t len) {
    return adler32(adler, reinterpret_cast<const Bytef*>(buf), len);
}

Status gen_timestamp_string(string* out_string) {
    time_t now = time(nullptr);
    tm local_tm;

    if (localtime_r(&now, &local_tm) == nullptr) {
        return Status::InternalError("localtime_r", static_cast<int16_t>(errno), std::strerror(errno));
    }
    char time_suffix[16] = {0}; // Example: 20150706111404
    if (strftime(time_suffix, sizeof(time_suffix), "%Y%m%d%H%M%S", &local_tm) == 0) {
        return Status::InternalError("localtime_r", static_cast<int16_t>(errno), std::strerror(errno));
    }

    *out_string = time_suffix;
    return Status::OK();
}

Status move_to_trash(const std::filesystem::path& file_path) {
    static std::atomic<uint64_t> delete_counter{0}; // a global counter to avoid file name duplication.

    auto t0 = std::chrono::steady_clock::now();

    std::string old_file_path = file_path.string();
    std::string old_file_name = file_path.filename().string();
    std::string storage_root = file_path
                                       .parent_path() // shard_path
                                       .parent_path() // DATA_PREFIX
                                       .parent_path() // storage_root
                                       .string();

    // 1. get timestamp string
    std::string time_str;
    RETURN_IF_ERROR(gen_timestamp_string(&time_str));

    std::string new_file_dir = fmt::format("{}{}/{}.{}/", storage_root, TRASH_PREFIX, time_str,
                                           delete_counter.fetch_add(1, std::memory_order_relaxed));
    std::string new_file_path = fmt::format("{}/{}", new_file_dir, old_file_name);
    // 2. create target dir, or the rename() function will fail.
    if (auto st = FileSystem::Default()->create_dir(new_file_dir); !st.ok()) {
        // May be because the parent directory does not exist, try create directories recursively.
        RETURN_IF_ERROR(fs::create_directories(new_file_dir));
    }

    // 3. remove file to trash
    auto st = FileSystem::Default()->rename_file(old_file_path, new_file_path);
    auto t1 = std::chrono::steady_clock::now();
    g_move_trash << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    return st;
}

Status read_write_test_file(const string& test_file_path) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(test_file_path));

    if (fs->path_exists(test_file_path).ok()) {
        RETURN_IF_ERROR(fs->delete_file(test_file_path));
    }

    const size_t TEST_FILE_BUF_SIZE = 4096;
    const size_t DIRECT_IO_ALIGNMENT = 512;
    char* write_test_buff = nullptr;
    char* read_test_buff = nullptr;
    if (posix_memalign((void**)&write_test_buff, DIRECT_IO_ALIGNMENT, TEST_FILE_BUF_SIZE) != 0) {
        LOG(WARNING) << "fail to allocate write buffer memory. size=" << TEST_FILE_BUF_SIZE;
        return Status::Corruption("Fail to allocate write buffer memory");
    }
    std::unique_ptr<char, decltype(&std::free)> write_buff(write_test_buff, &std::free);
    if (posix_memalign((void**)&read_test_buff, DIRECT_IO_ALIGNMENT, TEST_FILE_BUF_SIZE) != 0) {
        LOG(WARNING) << "fail to allocate read buffer memory. size=" << TEST_FILE_BUF_SIZE;
        return Status::Corruption("Fail to allocate write buffer memory");
    }
    std::unique_ptr<char, decltype(&std::free)> read_buff(read_test_buff, &std::free);
    // generate random numbers
    auto rand_seed = static_cast<uint32_t>(time(nullptr));
    for (size_t i = 0; i < TEST_FILE_BUF_SIZE; ++i) {
        int32_t tmp_value = rand_r(&rand_seed);
        write_test_buff[i] = static_cast<char>(tmp_value);
    }

    WritableFileOptions opts{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(auto wf, fs->new_writable_file(opts, test_file_path));
    RETURN_IF_ERROR(wf->append(Slice(write_buff.get(), TEST_FILE_BUF_SIZE)));
    RETURN_IF_ERROR(wf->close());

    ASSIGN_OR_RETURN(auto rf, fs->new_sequential_file(test_file_path));
    RETURN_IF_ERROR(rf->read_fully(read_buff.get(), TEST_FILE_BUF_SIZE));
    rf.reset();
    RETURN_IF_ERROR(fs->delete_file(test_file_path));

    if (memcmp(write_buff.get(), read_buff.get(), TEST_FILE_BUF_SIZE) != 0) {
        LOG(WARNING) << "the test file write_buf and read_buf not equal, [filename = " << test_file_path << "]";
        return Status::InternalError("test file write_buf and read_buf not equal");
    }
    return Status::OK();
}

bool check_datapath_rw(const string& path) {
    if (!fs::path_exist(path)) return false;
    string file_path = path + "/.read_write_test_file";
    try {
        Status res = read_write_test_file(file_path);
        return res.ok();
    } catch (...) {
        // do nothing
    }
    LOG(WARNING) << "error when try to read and write temp file under the data path and return "
                    "false. [path="
                 << path << "]";
    return false;
}

Status copy_dir(const string& src_dir, const string& dst_dir) {
    std::filesystem::path src_path(src_dir.c_str());
    std::filesystem::path dst_path(dst_dir.c_str());

    try {
        // Check whether the function call is valid
        if (!std::filesystem::exists(src_path) || !std::filesystem::is_directory(src_path)) {
            LOG(WARNING) << "Not found dir:" << src_path.string();
            return Status::NotFound(fmt::format("Not found dir: {}", src_path.string()));
        }

        if (std::filesystem::exists(dst_path)) {
            LOG(WARNING) << "Dir already exist: " << dst_path.string();
            return Status::AlreadyExist(fmt::format("Dir already exist: {}", dst_path.string()));
        }

        // Create the destination directory
        if (!std::filesystem::create_directory(dst_path)) {
            LOG(WARNING) << "Error to create dir: " << dst_path.string();
            return Status::IOError(
                    fmt::format("Error to create dir: {}, error:{} ", dst_path.string(), std::strerror(Errno::no())));
        }
    } catch (...) {
        LOG(WARNING) << "input invalid. src_path=" << src_path.string() << " dst_path=" << dst_path.string();
        return Status::InternalError("Invalid input path");
    }

    // Iterate through the source directory
    for (const auto& file : std::filesystem::directory_iterator(src_path)) {
        try {
            const std::filesystem::path& current(file.path());
            if (std::filesystem::is_directory(current)) {
                // Found directory: Recursion
                Status res = copy_dir(current.string(), (dst_path / current.filename()).string());
                if (!res.ok()) {
                    LOG(WARNING) << "Fail to copy file. src_path=" << src_path.string()
                                 << " dst_path=" << dst_path.string();
                    return Status::InternalError("Fail to copy file.");
                }
            } else {
                // Found file: Copy
                std::filesystem::copy_file(current, (dst_path / current.filename()).string());
            }
        } catch (...) {
            LOG(WARNING) << "Fail to copy " << src_path.string() << " to " << dst_path.string();
            return Status::InternalError("Fail to copy file.");
        }
    }
    return Status::OK();
}

__thread char Errno::_buf[BUF_SIZE]; ///< buffer instance

const char* Errno::str() {
    return str(no());
}

const char* Errno::str(int no) {
    if (nullptr != strerror_r(no, _buf, BUF_SIZE)) {
        LOG(WARNING) << "fail to get errno string. no=" << no << " errno=" << errno;
        snprintf(_buf, BUF_SIZE, "unknown errno");
    }

    return _buf;
}

int Errno::no() {
    return errno;
}

template <>
bool valid_signed_number<int128_t>(const std::string& value_str) {
    char* endptr = nullptr;
    const char* value_string = value_str.c_str();
    int64_t value = strtol(value_string, &endptr, 10);
    if (*endptr != 0) {
        return false;
    } else if (value > LONG_MIN && value < LONG_MAX) {
        return true;
    } else {
        bool sign = false;
        if (*value_string == '-' || *value_string == '+') {
            if (*(value_string++) == '-') {
                sign = true;
            }
        }

        uint128_t current = 0;
        uint128_t max_int128 = std::numeric_limits<int128_t>::max();
        while (*value_string != 0) {
            if (current > max_int128 / 10) {
                return false;
            }

            current = current * 10 + (*(value_string++) - '0');
        }

        if ((!sign && current > max_int128) || (sign && current > max_int128 + 1)) {
            return false;
        }

        return true;
    }
}

bool valid_decimal(const string& value_str, uint32_t precision, uint32_t frac) {
    const char* decimal_pattern = "-?\\d+(.\\d+)?";
    boost::regex e(decimal_pattern);
    boost::smatch what;
    if (!boost::regex_match(value_str, what, e) || what[0].str().size() != value_str.size()) {
        LOG(WARNING) << "invalid decimal value. [value=" << value_str << "]";
        return false;
    }

    std::string s = value_str[0] == '-' ? value_str.substr(1) : value_str;
    size_t number_length = s.size();
    size_t integer_len = 0;
    size_t fractional_len = 0;
    size_t point_pos = s.find('.');
    if (point_pos == string::npos) {
        integer_len = number_length;
        fractional_len = 0;
    } else {
        integer_len = point_pos;
        fractional_len = number_length - point_pos - 1;
    }

    // when precision = frac, integer_len can be 1; i.e.
    // decimal(2,2) can accept 0.1, 0.11, 0, 0.0
    if (precision == frac && integer_len == 1 && s[0] == '0') {
        return true;
    }

    if (integer_len <= (precision - frac) && fractional_len <= frac) {
        return true;
    } else {
        return false;
    }
}

bool valid_datetime(const string& value_str) {
    const char* datetime_pattern =
            "((?:\\d){4})-((?:\\d){2})-((?:\\d){2})[ ]*"
            "(((?:\\d){2}):((?:\\d){2}):((?:\\d){2}))?";
    boost::regex e(datetime_pattern);
    boost::smatch what;

    if (boost::regex_match(value_str, what, e)) {
        if (what[0].str().size() != value_str.size()) {
            LOG(WARNING) << "datetime str does not fully match. value_str=" << value_str << " match=" << what[0].str();
            return false;
        }

        int month = strtol(what[2].str().c_str(), nullptr, 10);
        if (month < 1 || month > 12) {
            LOG(WARNING) << "invalid month " << month;
            return false;
        }

        int day = strtol(what[3].str().c_str(), nullptr, 10);
        if (day < 1 || day > 31) {
            LOG(WARNING) << "invalid day " << day;
            return false;
        }

        if (what[4].length()) {
            int hour = strtol(what[5].str().c_str(), nullptr, 10);
            if (hour < 0 || hour > 23) {
                LOG(WARNING) << "invalid hour " << hour;
                return false;
            }

            int minute = strtol(what[6].str().c_str(), nullptr, 10);
            if (minute < 0 || minute > 59) {
                LOG(WARNING) << "invalid minute " << minute;
                return false;
            }

            int second = strtol(what[7].str().c_str(), nullptr, 10);
            if (second < 0 || second > 59) {
                LOG(WARNING) << "invalid second " << second;
                return false;
            }
        }

        return true;
    } else {
        LOG(WARNING) << "datetime string does not match";
        return false;
    }
}

bool valid_bool(const std::string& value_str) {
    if (value_str == "0" || value_str == "1") {
        return true;
    }
    StringParser::ParseResult result;
    StringParser::string_to_bool(value_str.c_str(), value_str.length(), &result);
    return result == StringParser::PARSE_SUCCESS;
}

std::string parent_name(const std::string& fullpath) {
    std::filesystem::path path(fullpath);
    return path.parent_path().string();
}

std::string file_name(const std::string& fullpath) {
    std::filesystem::path path(fullpath);
    return path.filename().string();
}

} // namespace starrocks

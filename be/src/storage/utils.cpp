// This file is made available under Elastic License 2.0.
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

#include <dirent.h>
#include <lz4/lz4.h>
#include <sys/stat.h>
#include <unistd.h>

#include <boost/regex.hpp>
#include <cerrno>
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
#include "env/env.h"
#include "gutil/strings/substitute.h"
#include "storage/olap_define.h"
#include "util/errno.h"
#include "util/file_utils.h"
#include "util/string_parser.hpp"

using std::string;
using std::set;
using std::vector;

namespace starrocks {

uint32_t olap_adler32(uint32_t adler, const char* buf, size_t len) {
    return adler32(adler, reinterpret_cast<const Bytef*>(buf), len);
}

OLAPStatus gen_timestamp_string(string* out_string) {
    time_t now = time(nullptr);
    tm local_tm;

    if (localtime_r(&now, &local_tm) == nullptr) {
        LOG(WARNING) << "fail to localtime_r time. time=" << now;
        return OLAP_ERR_OS_ERROR;
    }
    char time_suffix[16] = {0}; // Example: 20150706111404
    if (strftime(time_suffix, sizeof(time_suffix), "%Y%m%d%H%M%S", &local_tm) == 0) {
        LOG(WARNING) << "fail to strftime time. time=" << now;
        return OLAP_ERR_OS_ERROR;
    }

    *out_string = time_suffix;
    return OLAP_SUCCESS;
}

OLAPStatus move_to_trash(const std::filesystem::path& schema_hash_root, const std::filesystem::path& file_path) {
    OLAPStatus res = OLAP_SUCCESS;
    string old_file_path = file_path.string();
    string old_file_name = file_path.filename().string();
    string storage_root = schema_hash_root
                                  .parent_path() // tablet_path
                                  .parent_path() // shard_path
                                  .parent_path() // DATA_PREFIX
                                  .parent_path() // storage_root
                                  .string();

    // 1. get timestamp string
    string time_str;
    if ((res = gen_timestamp_string(&time_str)) != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to generate time_string when move file to trash. err code=" << res;
        return res;
    }

    // 2. generate new file path
    static uint64_t delete_counter = 0; // a global counter to avoid file name duplication.
    static std::mutex lock;             // lock for delete_counter
    std::stringstream new_file_dir_stream;
    lock.lock();
    // when file_path points to a schema_path, we need to save tablet info in trash_path,
    // so we add file_path.parent_path().filename() in new_file_path.
    // other conditions are not considered, for they are nothing serious.
    new_file_dir_stream << storage_root << TRASH_PREFIX << "/" << time_str << "." << delete_counter++ << "/"
                        << file_path.parent_path().filename().string();
    lock.unlock();
    string new_file_dir = new_file_dir_stream.str();
    string new_file_path = new_file_dir + "/" + old_file_name;
    // create target dir, or the rename() function will fail.
    if (!FileUtils::check_exist(new_file_dir) && !FileUtils::create_dir(new_file_dir).ok()) {
        LOG(WARNING) << "delete file failed. due to mkdir failed. file=" << old_file_path
                     << " new_dir=" << new_file_dir;
        return OLAP_ERR_OS_ERROR;
    }

    // 3. remove file to trash
    VLOG(3) << "move file to trash. " << old_file_path << " -> " << new_file_path;
    if (rename(old_file_path.c_str(), new_file_path.c_str()) < 0) {
        LOG(WARNING) << "move file to trash failed. file=" << old_file_path << " target=" << new_file_path;
        return OLAP_ERR_OS_ERROR;
    }

    // 4. check parent dir of source file, delete it when empty
    string source_parent_dir = schema_hash_root.parent_path().string(); // tablet_id level
    std::set<std::string> sub_dirs, sub_files;

    RETURN_CODE_IF_ERROR_WITH_WARN(FileUtils::list_dirs_files(source_parent_dir, &sub_dirs, &sub_files, Env::Default()),
                                   OLAP_SUCCESS, "access dir failed. [dir=" + source_parent_dir + "].");

    if (sub_dirs.empty() && sub_files.empty()) {
        LOG(INFO) << "remove empty dir " << source_parent_dir;
        // no need to exam return status
        Env::Default()->delete_dir(source_parent_dir);
    }

    return OLAP_SUCCESS;
}

OLAPStatus copy_file(const string& src, const string& dest) {
    int src_fd = -1;
    int dest_fd = -1;
    char buf[1024 * 1024];
    OLAPStatus res = OLAP_SUCCESS;

    src_fd = ::open(src.c_str(), O_RDONLY);
    if (src_fd < 0) {
        PLOG(WARNING) << "failed to open " << src;
        res = OLAP_ERR_FILE_NOT_EXIST;
        goto COPY_EXIT;
    }

    dest_fd = ::open(dest.c_str(), O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
    if (dest_fd < 0) {
        PLOG(WARNING) << "failed to open " << dest;
        res = OLAP_ERR_FILE_NOT_EXIST;
        goto COPY_EXIT;
    }

    while (true) {
        ssize_t rd_size = ::read(src_fd, buf, sizeof(buf));
        if (rd_size < 0) {
            PLOG(WARNING) << "failed to read from " << src;
            return OLAP_ERR_IO_ERROR;
        } else if (0 == rd_size) {
            break;
        }

        ssize_t wr_size = ::write(dest_fd, buf, rd_size);
        if (wr_size != rd_size) {
            PLOG(WARNING) << "failed to write to " << dest;
            res = OLAP_ERR_IO_ERROR;
            goto COPY_EXIT;
        }
    }

COPY_EXIT:
    if (src_fd >= 0) {
        ::close(src_fd);
    }

    if (dest_fd >= 0) {
        ::close(dest_fd);
    }

    VLOG(3) << "copy file success. [src=" << src << " dest=" << dest << "]";

    return res;
}
OLAPStatus read_write_test_file(const string& test_file_path) {
    if (access(test_file_path.c_str(), F_OK) == 0) {
        if (remove(test_file_path.c_str()) != 0) {
            PLOG(WARNING) << "fail to delete " << test_file_path;
            return OLAP_ERR_IO_ERROR;
        }
    } else {
        if (errno != ENOENT) {
            PLOG(WARNING) << "fail to access " << test_file_path;
            return OLAP_ERR_IO_ERROR;
        }
    }
    std::unique_ptr<RandomRWFile> file;
    Status st = Env::Default()->new_random_rw_file(test_file_path, &file);
    if (!st.ok()) {
        LOG(WARNING) << "fail to create test file " << test_file_path << ": " << st;
        return OLAP_ERR_IO_ERROR;
    }
    const size_t TEST_FILE_BUF_SIZE = 4096;
    const size_t DIRECT_IO_ALIGNMENT = 512;
    char* write_test_buff = nullptr;
    char* read_test_buff = nullptr;
    if (posix_memalign((void**)&write_test_buff, DIRECT_IO_ALIGNMENT, TEST_FILE_BUF_SIZE) != 0) {
        LOG(WARNING) << "fail to allocate write buffer memory. size=" << TEST_FILE_BUF_SIZE;
        return OLAP_ERR_MALLOC_ERROR;
    }
    std::unique_ptr<char, decltype(&std::free)> write_buff(write_test_buff, &std::free);
    if (posix_memalign((void**)&read_test_buff, DIRECT_IO_ALIGNMENT, TEST_FILE_BUF_SIZE) != 0) {
        LOG(WARNING) << "fail to allocate read buffer memory. size=" << TEST_FILE_BUF_SIZE;
        return OLAP_ERR_MALLOC_ERROR;
    }
    std::unique_ptr<char, decltype(&std::free)> read_buff(read_test_buff, &std::free);
    // generate random numbers
    uint32_t rand_seed = static_cast<uint32_t>(time(nullptr));
    for (size_t i = 0; i < TEST_FILE_BUF_SIZE; ++i) {
        int32_t tmp_value = rand_r(&rand_seed);
        write_test_buff[i] = static_cast<char>(tmp_value);
    }
    st = file->write_at(0, Slice(write_buff.get(), TEST_FILE_BUF_SIZE));
    if (!st.ok()) {
        LOG(WARNING) << "fail to write " << test_file_path << ": " << st;
        return OLAP_ERR_IO_ERROR;
    }
    st = file->read_at(0, Slice(read_buff.get(), TEST_FILE_BUF_SIZE));
    if (!st.ok()) {
        LOG(WARNING) << "fail to read " << test_file_path << ": " << st;
        return OLAP_ERR_IO_ERROR;
    }
    if (memcmp(write_buff.get(), read_buff.get(), TEST_FILE_BUF_SIZE) != 0) {
        LOG(WARNING) << "the test file write_buf and read_buf not equal, [file_name = " << test_file_path << "]";
        return OLAP_ERR_TEST_FILE_ERROR;
    }
    st = file->close();
    if (!st.ok()) {
        LOG(WARNING) << "fail to close " << test_file_path << ": " << st;
        return OLAP_ERR_IO_ERROR;
    }
    if (remove(test_file_path.c_str()) != 0) {
        char errmsg[64];
        VLOG(3) << "fail to delete test file. [err='" << strerror_r(errno, errmsg, 64) << "' path='" << test_file_path
                << "']";
        return OLAP_ERR_IO_ERROR;
    }
    return OLAP_SUCCESS;
}

bool check_datapath_rw(const string& path) {
    if (!FileUtils::check_exist(path)) return false;
    string file_path = path + "/.read_write_test_file";
    try {
        OLAPStatus res = read_write_test_file(file_path);
        return res == OLAP_SUCCESS;
    } catch (...) {
        // do nothing
    }
    LOG(WARNING) << "error when try to read and write temp file under the data path and return "
                    "false. [path="
                 << path << "]";
    return false;
}

OLAPStatus copy_dir(const string& src_dir, const string& dst_dir) {
    std::filesystem::path src_path(src_dir.c_str());
    std::filesystem::path dst_path(dst_dir.c_str());

    try {
        // Check whether the function call is valid
        if (!std::filesystem::exists(src_path) || !std::filesystem::is_directory(src_path)) {
            LOG(WARNING) << "Source dir not exist or is not a dir. src_path=" << src_path.string();
            return OLAP_ERR_CREATE_FILE_ERROR;
        }

        if (std::filesystem::exists(dst_path)) {
            LOG(WARNING) << "Dst dir already exists.[dst_path=" << dst_path.string() << "]";
            return OLAP_ERR_CREATE_FILE_ERROR;
        }

        // Create the destination directory
        if (!std::filesystem::create_directory(dst_path)) {
            LOG(WARNING) << "Unable to create dst dir.[dst_path=" << dst_path.string() << "]";
            return OLAP_ERR_CREATE_FILE_ERROR;
        }
    } catch (...) {
        LOG(WARNING) << "input invalid. src_path=" << src_path.string() << " dst_path=" << dst_path.string();
        return OLAP_ERR_STL_ERROR;
    }

    // Iterate through the source directory
    for (const auto& file : std::filesystem::directory_iterator(src_path)) {
        try {
            const std::filesystem::path& current(file.path());
            if (std::filesystem::is_directory(current)) {
                // Found directory: Recursion
                OLAPStatus res = OLAP_SUCCESS;
                if (OLAP_SUCCESS != (res = copy_dir(current.string(), (dst_path / current.filename()).string()))) {
                    LOG(WARNING) << "Fail to copy file. src_path=" << src_path.string()
                                 << " dst_path=" << dst_path.string();
                    return OLAP_ERR_CREATE_FILE_ERROR;
                }
            } else {
                // Found file: Copy
                std::filesystem::copy_file(current, (dst_path / current.filename()).string());
            }
        } catch (...) {
            LOG(WARNING) << "Fail to copy " << src_path.string() << " to " << dst_path.string();
            return OLAP_ERR_STL_ERROR;
        }
    }
    return OLAP_SUCCESS;
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

} // namespace starrocks

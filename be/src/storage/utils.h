// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/utils.h

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

#ifndef STARROCKS_BE_SRC_OLAP_UTILS_H
#define STARROCKS_BE_SRC_OLAP_UTILS_H

#include <fcntl.h>
#include <pthread.h>
#include <sys/time.h>
#include <zlib.h>

#include <cstdio>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <iterator>
#include <limits>
#include <list>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "common/logging.h"
#if defined(__i386) || defined(__x86_64__)
#include "storage/bhp_lib.h"
#endif
#include "storage/olap_common.h"
#include "storage/olap_define.h"

namespace starrocks {

const static int32_t g_power_table[] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};

class OlapStopWatch {
public:
    uint64_t get_elapse_time_us() {
        struct timeval now;
        gettimeofday(&now, nullptr);
        return (uint64_t)((now.tv_sec - _begin_time.tv_sec) * 1e6 + (now.tv_usec - _begin_time.tv_usec));
    }

    double get_elapse_second() { return get_elapse_time_us() / 1000000.0; }

    void reset() { gettimeofday(&_begin_time, nullptr); }

    OlapStopWatch() { reset(); }

private:
    struct timeval _begin_time;
};

#define ADLER32_INIT adler32(0L, Z_NULL, 0)
uint32_t olap_adler32(uint32_t adler, const char* buf, size_t len);

OLAPStatus gen_timestamp_string(std::string* out_string);

// move file to storage_root/trash, file can be a directory
OLAPStatus move_to_trash(const std::filesystem::path& schema_hash_root, const std::filesystem::path& file_path);

OLAPStatus copy_file(const std::string& src, const std::string& dest);

OLAPStatus copy_dir(const std::string& src_dir, const std::string& dst_dir);

bool check_datapath_rw(const std::string& path);

OLAPStatus read_write_test_file(const std::string& test_file_path);

class Errno {
public:
    static const char* str();
    static const char* str(int no);
    static int no();

private:
    static const int BUF_SIZE = 256;
    static __thread char _buf[BUF_SIZE];
};

inline bool is_io_error(OLAPStatus status) {
    return (((OLAP_ERR_IO_ERROR == status || OLAP_ERR_READ_UNENOUGH == status) && errno == EIO) ||
            OLAP_ERR_CHECKSUM_ERROR == status || OLAP_ERR_FILE_DATA_ERROR == status ||
            OLAP_ERR_TEST_FILE_ERROR == status || OLAP_ERR_ROWBLOCK_READ_INFO_ERROR == status);
}

// check if int8_t, int16_t, int32_t, int64_t value is overflow
template <typename T>
bool valid_signed_number(const std::string& value_str) {
    char* endptr = nullptr;
    errno = 0;
    int64_t value = strtol(value_str.c_str(), &endptr, 10);

    if ((errno == ERANGE && (value == LONG_MAX || value == LONG_MIN)) || (errno != 0 && value == 0) ||
        endptr == value_str || *endptr != '\0') {
        return false;
    }

    if (value < std::numeric_limits<T>::lowest() || value > std::numeric_limits<T>::max()) {
        return false;
    }

    return true;
}

template <>
bool valid_signed_number<int128_t>(const std::string& value_str);

// check if uint8_t, uint16_t, uint32_t, uint64_t value is overflow
template <typename T>
bool valid_unsigned_number(const std::string& value_str) {
    if (value_str[0] == '-') {
        return false;
    }

    char* endptr = nullptr;
    errno = 0;
    uint64_t value = strtoul(value_str.c_str(), &endptr, 10);

    if ((errno == ERANGE && (value == ULONG_MAX)) || (errno != 0 && value == 0) || endptr == value_str ||
        *endptr != '\0') {
        return false;
    }

    if (value < std::numeric_limits<T>::lowest() || value > std::numeric_limits<T>::max()) {
        return false;
    }

    return true;
}

bool valid_decimal(const std::string& value_str, uint32_t precision, uint32_t frac);

// check if date and datetime value is valid
bool valid_datetime(const std::string& value_str);

bool valid_bool(const std::string& value_str);

// Util used to get string name of thrift enum item
#define EnumToString(enum_type, index, out)                                                         \
    do {                                                                                            \
        std::map<int, const char*>::const_iterator it = _##enum_type##_VALUES_TO_NAMES.find(index); \
        if (it == _##enum_type##_VALUES_TO_NAMES.end()) {                                           \
            out = "NULL";                                                                           \
        } else {                                                                                    \
            out = it->second;                                                                       \
        }                                                                                           \
    } while (0)

} // namespace starrocks

#endif // STARROCKS_BE_SRC_OLAP_UTILS_H

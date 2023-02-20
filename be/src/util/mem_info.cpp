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
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/mem_info.cpp

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

#include "util/mem_info.h"

#include <linux/magic.h>
#include <sys/vfs.h>

#include <cstdlib>
#include <fstream>
#include <iostream>

#include "fs/fs_util.h"
#include "gutil/strings/split.h"
#include "util/errno.h"
#include "util/file_util.h"
#include "util/pretty_printer.h"
#include "util/string_parser.hpp"

namespace starrocks {

// CGROUP2_SUPER_MAGIC is the indication for cgroup v2
// It is defined in kernel 4.5+
// I copy the defintion from linux/magic.h in higher kernel
#ifndef CGROUP2_SUPER_MAGIC
#define CGROUP2_SUPER_MAGIC 0x63677270
#endif

bool MemInfo::_s_initialized = false;
int64_t MemInfo::_s_physical_mem = -1;

void MemInfo::init() {
    set_memlimit_if_container();

    // Read from /proc/meminfo
    std::ifstream meminfo("/proc/meminfo", std::ios::in);
    std::string line;

    while (meminfo.good() && !meminfo.eof()) {
        getline(meminfo, line);
        std::vector<std::string> fields = strings::Split(line, " ", strings::SkipWhitespace());

        // We expect lines such as, e.g., 'MemTotal: 16129508 kB'
        if (fields.size() < 3) {
            continue;
        }

        if (fields[0].compare("MemTotal:") != 0) {
            continue;
        }

        StringParser::ParseResult result;
        auto mem_total_kb = StringParser::string_to_int<int64_t>(fields[1].data(), fields[1].size(), &result);

        if (result == StringParser::PARSE_SUCCESS) {
            // Entries in /proc/meminfo are in KB.
            if (_s_physical_mem <= 0 || _s_physical_mem > mem_total_kb * 1024L) {
                _s_physical_mem = mem_total_kb * 1024L;
            }
        }

        break;
    }

    if (meminfo.is_open()) {
        meminfo.close();
    }

    if (_s_physical_mem <= 0) {
        LOG(WARNING) << "Could not determine amount of physical memory on this machine.";
    }

    LOG(INFO) << "Physical Memory: " << PrettyPrinter::print(_s_physical_mem, TUnit::BYTES);

    _s_initialized = true;
}

std::string MemInfo::debug_string() {
    DCHECK(_s_initialized);
    std::stringstream stream;
    stream << "Mem Info: " << PrettyPrinter::print(_s_physical_mem, TUnit::BYTES) << std::endl;
    return stream.str();
}

void MemInfo::set_memlimit_if_container() {
    // check if application is in docker container or not via /.dockerenv file
    bool running_in_docker = fs::path_exist("/.dockerenv");
    if (!running_in_docker) {
        return;
    }
    struct statfs fs;
    if (statfs("/sys/fs/cgroup", &fs) < 0) {
        LOG(WARNING) << "Fail to get file system statistics. err: " << errno_to_string(errno);
        return;
    }

    StringParser::ParseResult result = StringParser::PARSE_FAILURE;
    if (fs.f_type == TMPFS_MAGIC) {
        // cgroup v1
        // Read from /sys/fs/cgroup/memory/memory.limit_in_bytes
        std::string limit_in_bytes_str;
        if (!FileUtil::read_whole_content("/sys/fs/cgroup/memory/memory.limit_in_bytes", limit_in_bytes_str)) {
            return;
        }
        _s_physical_mem =
                StringParser::string_to_int<int64_t>(limit_in_bytes_str.data(), limit_in_bytes_str.size(), &result);
    } else if (fs.f_type == CGROUP2_SUPER_MAGIC) {
        // cgroup v2
        // Read from /sys/fs/cgroup/memory/memory.max
        std::string memory_max;
        if (!FileUtil::read_whole_content("/sys/fs/cgroup/memory.max", memory_max)) {
            return;
        }
        _s_physical_mem = StringParser::string_to_int<int64_t>(memory_max.data(), memory_max.size(), &result);
    }

    if (result != StringParser::PARSE_SUCCESS) {
        _s_physical_mem = std::numeric_limits<int64_t>::max();
    } else {
        LOG(INFO) << "Init mem info by container's cgroup config, physical_mem=" << _s_physical_mem;
    }
}

} // namespace starrocks

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
        auto mem_total_kb =
                StringParser::string_to_int<int64_t>(fields[1].data(), static_cast<int>(fields[1].size()), &result);

        if (result == StringParser::PARSE_SUCCESS) {
            // Entries in /proc/meminfo are in KB.
            if (_s_physical_mem == -1 || _s_physical_mem > mem_total_kb * 1024L) {
                _s_physical_mem = mem_total_kb * 1024L;
            }
        }

        break;
    }

    if (meminfo.is_open()) {
        meminfo.close();
    }

    if (_s_physical_mem == -1) {
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
    if (running_in_docker) {
        struct statfs fs;
        int err = statfs("/sys/fs/cgroup", &fs);
        if (err < 0) {
            LOG(WARNING) << "Fail to get file system statistics. err: " << errno_to_string(err);
            return;
        }

        std::ifstream memoryLimit;
        std::string line;
        StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
        if (fs.f_type == TMPFS_MAGIC) {
            // cgroup v1
            // Read from /sys/fs/cgroup/memory/memory.limit_in_bytes
            memoryLimit.open("/sys/fs/cgroup/memory/memory.limit_in_bytes");
            getline(memoryLimit, line);
            _s_physical_mem = StringParser::string_to_int<int64_t>(line.data(), line.size(), &result);
        } else if (fs.f_type == CGROUP2_SUPER_MAGIC) {
            // cgroup v2
            // Read from /sys/fs/cgroup/memory/memory.max
            memoryLimit.open("/sys/fs/cgroup/memory.max");
            getline(memoryLimit, line);

            if (line == "max") {
                _s_physical_mem = std::numeric_limits<int64_t>::max();
            } else {
                _s_physical_mem = StringParser::string_to_int<int64_t>(line.data(), line.size(), &result);
            }
        }

        if (result != StringParser::PARSE_SUCCESS) {
            _s_physical_mem = std::numeric_limits<int64_t>::max();
        }

        if (memoryLimit.is_open()) {
            memoryLimit.close();
        }
    }
}

} // namespace starrocks

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
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/debug_util.cpp

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

#include "util/debug_util.h"

#include <iomanip>
#include <sstream>

#include "common/version.h"
#include "gen_cpp/types.pb.h"

#define PRECISION 2

#define SECOND (1000)

#define THOUSAND (1000)
#define MILLION (THOUSAND * 1000)

namespace starrocks {

std::string print_plan_node_type(const TPlanNodeType::type& type) {
    std::map<int, const char*>::const_iterator i;
    i = _TPlanNodeType_VALUES_TO_NAMES.find(type);

    if (i != _TPlanNodeType_VALUES_TO_NAMES.end()) {
        return i->second;
    }

    return "Invalid plan node type";
}

std::string get_build_version(bool compact) {
    std::stringstream ss;
    ss << STARROCKS_VERSION << " " << STARROCKS_BUILD_TYPE << " (build " << STARROCKS_COMMIT_HASH << ")";
    if (!compact) {
        ss << std::endl
           << "Built on " << STARROCKS_BUILD_TIME << " by " << STARROCKS_BUILD_USER << "@" << STARROCKS_BUILD_HOST;
    }

    return ss.str();
}

size_t get_build_version(char* buffer, size_t max_size) {
    size_t length = 0;
    length = snprintf(buffer + length, max_size - length, "%s ", STARROCKS_VERSION) + length;
    length = snprintf(buffer + length, max_size - length, "%s ", STARROCKS_BUILD_TYPE) + length;
    length = snprintf(buffer + length, max_size - length, "(build %s)\n", STARROCKS_COMMIT_HASH) + length;
    return length;
}

std::string get_short_version() {
    static std::string short_version(std::string(STARROCKS_VERSION) + "-" + STARROCKS_COMMIT_HASH);
    return short_version;
}

std::string get_version_string(bool compact) {
    std::stringstream ss;
    ss << " version " << get_build_version(compact);
    return ss.str();
}

std::string hexdump(const char* buf, size_t len) {
    std::stringstream ss;
    ss << std::hex << std::uppercase;
    for (size_t i = 0; i < len; ++i) {
        ss << std::setfill('0') << std::setw(2) << ((uint16_t)buf[i] & 0xff);
    }
    return ss.str();
}

} // namespace starrocks

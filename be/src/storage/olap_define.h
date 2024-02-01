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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/olap_define.h

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

#pragma once

#include <cstdint>
#include <cstdlib>
#include <sstream>
#include <string>

#include "common/constexpr.h"
#include "common/config.h"

namespace starrocks {

// data and index page size, default is 64k
static const size_t OLAP_PAGE_SIZE = 65536;

static const uint64_t OLAP_FIX_HEADER_MAGIC_NUMBER = 0;

inline uint32_t get_olap_string_max_length() {
    return config::olap_string_max_length;
}

// the max bytes for stored string length
using StringLengthType = uint16_t;
static const uint16_t OLAP_STRING_MAX_BYTES = sizeof(StringLengthType);

enum OLAPDataVersion {
    OLAP_V1 = 0,
    STARROCKS_V1 = 1,
};

// storage_root_path file path
static const std::string ALIGN_TAG_PREFIX = "/align_tag";             // NOLINT
static const std::string MINI_PREFIX = "/mini_download";              // NOLINT
static const std::string CLUSTER_ID_PREFIX = "/cluster_id";           // NOLINT
static const std::string DATA_PREFIX = "/data";                       // NOLINT
static const std::string DPP_PREFIX = "/dpp_download";                // NOLINT
static const std::string SNAPSHOT_PREFIX = "/snapshot";               // NOLINT
static const std::string TRASH_PREFIX = "/trash";                     // NOLINT
static const std::string UNUSED_PREFIX = "/unused";                   // NOLINT
static const std::string ERROR_LOG_PREFIX = "/error_log";             // NOLINT
static const std::string REJECTED_RECORD_PREFIX = "/rejected_record"; // NOLINT
static const std::string CLONE_PREFIX = "/clone";                     // NOLINT
static const std::string TMP_PREFIX = "/tmp";                         // NOLINT
static const std::string PERSISTENT_INDEX_PREFIX = "/persistent";     // NOLINT
static const std::string REPLICATION_PREFIX = "/replication";         // NOLINT

static const int32_t OLAP_DATA_VERSION_APPLIED = STARROCKS_V1;

// bloom filter fpp
static const double BLOOM_FILTER_DEFAULT_FPP = 0.05;

enum ColumnFamilyIndex {
    DEFAULT_COLUMN_FAMILY_INDEX = 0,
    STARROCKS_COLUMN_FAMILY_INDEX,
    META_COLUMN_FAMILY_INDEX,
    // Newly-added item should be placed above this item.
    NUM_COLUMN_FAMILY_INDEX,
};

static const std::string DEFAULT_COLUMN_FAMILY = "default"; // NOLINT
static const std::string STARROCKS_COLUMN_FAMILY = "doris"; // NOLINT
static const std::string META_COLUMN_FAMILY = "meta";       // NOLINT
const std::string TABLET_ID_KEY = "tablet_id";              // NOLINT

#define DECLARE_SINGLETON(classname) \
public:                              \
    static classname* instance() {   \
        static classname s_instance; \
        return &s_instance;          \
    }                                \
                                     \
protected:                           \
    classname();                     \
                                     \
private:                             \
    ~classname();

#define SAFE_DELETE(ptr)     \
    do {                     \
        if (NULL != (ptr)) { \
            delete (ptr);    \
            (ptr) = NULL;    \
        }                    \
    } while (0)

#define SAFE_DELETE_ARRAY(ptr) \
    do {                       \
        if (NULL != (ptr)) {   \
            delete[](ptr);     \
            (ptr) = NULL;      \
        }                      \
    } while (0)

#ifndef BUILD_VERSION
#define BUILD_VERSION "Unknown"
#endif

} // namespace starrocks

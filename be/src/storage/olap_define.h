// This file is made available under Elastic License 2.0.
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

namespace starrocks {

// The block size of the column storage file, which may be loaded into memory in its entirety, needs to be strictly controlled, defined here as 256MB
static const uint32_t OLAP_MAX_COLUMN_SEGMENT_FILE_SIZE = 268435456;
static const double OLAP_COLUMN_FILE_SEGMENT_SIZE_SCALE = 0.9;

// data and index page size, default is 64k
static const size_t OLAP_PAGE_SIZE = 65536;

static const uint64_t OLAP_FIX_HEADER_MAGIC_NUMBER = 0;

// the max length supported for varchar type
static const uint32_t OLAP_STRING_MAX_LENGTH = 1048576;

// the max bytes for stored string length
using StringLengthType = uint16_t;
static const uint16_t OLAP_STRING_MAX_BYTES = sizeof(StringLengthType);

enum OLAPDataVersion {
    OLAP_V1 = 0,
    STARROCKS_V1 = 1,
};

// storage_root_path file path
static const std::string ALIGN_TAG_PREFIX = "/align_tag";   // NOLINT
static const std::string MINI_PREFIX = "/mini_download";    // NOLINT
static const std::string CLUSTER_ID_PREFIX = "/cluster_id"; // NOLINT
static const std::string DATA_PREFIX = "/data";             // NOLINT
static const std::string DPP_PREFIX = "/dpp_download";      // NOLINT
static const std::string SNAPSHOT_PREFIX = "/snapshot";     // NOLINT
static const std::string TRASH_PREFIX = "/trash";           // NOLINT
static const std::string UNUSED_PREFIX = "/unused";         // NOLINT
static const std::string ERROR_LOG_PREFIX = "/error_log";   // NOLINT
static const std::string CLONE_PREFIX = "/clone";           // NOLINT
static const std::string TMP_PREFIX = "/tmp";               // NOLINT

static const int32_t OLAP_DATA_VERSION_APPLIED = STARROCKS_V1;

static const uint32_t MAX_OP_IN_FIELD_NUM = 100;

static const uint64_t GB_EXCHANGE_BYTE = 1024 * 1024 * 1024;

// bloom filter fpp
static const double BLOOM_FILTER_DEFAULT_FPP = 0.05;

#define OLAP_GOTO(label) goto label

enum ColumnFamilyIndex {
    DEFAULT_COLUMN_FAMILY_INDEX = 0,
    STARROCKS_COLUMN_FAMILY_INDEX,
    META_COLUMN_FAMILY_INDEX,
    // Newly-added item should be placed above this item.
    NUM_COLUMN_FAMILY_INDEX,
};

static const std::string DEFAULT_COLUMN_FAMILY = "default";                   // NOLINT
static const std::string STARROCKS_COLUMN_FAMILY = "doris";                   // NOLINT
static const std::string META_COLUMN_FAMILY = "meta";                         // NOLINT
static const std::string END_ROWSET_ID = "end_rowset_id";                     // NOLINT
static const std::string CONVERTED_FLAG = "true";                             // NOLINT
static const std::string TABLET_CONVERT_FINISHED = "tablet_convert_finished"; // NOLINT
const std::string TABLET_ID_KEY = "tablet_id";                                // NOLINT
const std::string TABLET_SCHEMA_HASH_KEY = "schema_hash";                     // NOLINT
const std::string TABLET_ID_PREFIX = "t_";                                    // NOLINT
const std::string ROWSET_ID_PREFIX = "s_";                                    // NOLINT

#if defined(__GNUC__)
#define OLAP_LIKELY(x) __builtin_expect((x), 1)
#define OLAP_UNLIKELY(x) __builtin_expect((x), 0)
#else
#define OLAP_LIKELY(x)
#define OLAP_UNLIKELY(x)
#endif

// Declare copy constructor and equal operator as private
#ifndef DISALLOW_COPY_AND_ASSIGN
#define DISALLOW_COPY_AND_ASSIGN(type_t) \
    type_t& operator=(const type_t&);    \
    type_t(const type_t&);
#endif

// thread-safe(gcc only) method for obtaining singleton
#define DECLARE_SINGLETON(classname)     \
public:                                  \
    static classname* instance() {       \
        classname* p_instance = NULL;    \
        try {                            \
            static classname s_instance; \
            p_instance = &s_instance;    \
        } catch (...) {                  \
            p_instance = NULL;           \
        }                                \
        return p_instance;               \
    }                                    \
                                         \
protected:                               \
    classname();                         \
                                         \
private:                                 \
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

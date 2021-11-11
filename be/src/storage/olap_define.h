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

#ifndef STARROCKS_BE_SRC_OLAP_OLAP_DEFINE_H
#define STARROCKS_BE_SRC_OLAP_OLAP_DEFINE_H

#include <cstdint>
#include <cstdlib>
#include <sstream>
#include <string>

namespace starrocks {

// The block size of the column storage file, which may be loaded into memory in its entirety, needs to be strictly controlled, defined here as 256MB
static const uint32_t OLAP_MAX_COLUMN_SEGMENT_FILE_SIZE = 268435456;
static const double OLAP_COLUMN_FILE_SEGMENT_SIZE_SCALE = 0.9;

static const uint64_t OLAP_FIX_HEADER_MAGIC_NUMBER = 0;

// the max length supported for varchar type
static const uint16_t OLAP_STRING_MAX_LENGTH = 65535;

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

// default value of chunk_size, it's a value decided at compile time
static const int32_t DEFAULT_CHUNK_SIZE = 4096;

#define OLAP_GOTO(label) goto label

enum OLAPStatus {
    OLAP_SUCCESS = 0,

    // other errors except following errors
    OLAP_ERR_OTHER_ERROR = -1,

    // system error codessuch as file system memory and other system call failures
    // [-100, -200)
    OLAP_ERR_OS_ERROR = -100,
    OLAP_ERR_DIR_NOT_EXIST = -101,
    OLAP_ERR_FILE_NOT_EXIST = -102,
    OLAP_ERR_CREATE_FILE_ERROR = -103,
    OLAP_ERR_MALLOC_ERROR = -104,
    OLAP_ERR_STL_ERROR = -105,
    OLAP_ERR_IO_ERROR = -106,
    OLAP_ERR_COMPRESS_ERROR = -111,
    OLAP_ERR_DECOMPRESS_ERROR = -112,
    OLAP_ERR_RWLOCK_ERROR = -115,
    OLAP_ERR_READ_UNENOUGH = -116,
    OLAP_ERR_CANNOT_CREATE_DIR = -117,
    OLAP_ERR_FILE_FORMAT_ERROR = -119,
    OLAP_ERR_FILE_ALREADY_EXIST = -122,

    // common errors codes
    // [-200, -300)
    OLAP_ERR_NOT_INITED = -200,
    OLAP_ERR_FUNC_NOT_IMPLEMENTED = -201,
    OLAP_ERR_INPUT_PARAMETER_ERROR = -203,
    OLAP_ERR_BUFFER_OVERFLOW = -204,
    OLAP_ERR_INIT_FAILED = -206,
    OLAP_ERR_INVALID_SCHEMA = -207,
    OLAP_ERR_CHECKSUM_ERROR = -208,
    OLAP_ERR_PARSE_PROTOBUF_ERROR = -211,
    OLAP_ERR_SERIALIZE_PROTOBUF_ERROR = -212,
    OLAP_ERR_VERSION_NOT_EXIST = -214,
    OLAP_ERR_TABLE_NOT_FOUND = -215,
    OLAP_ERR_TRY_LOCK_FAILED = -216,
    OLAP_ERR_FILE_DATA_ERROR = -220,
    OLAP_ERR_TEST_FILE_ERROR = -221,
    OLAP_ERR_INVALID_ROOT_PATH = -222,
    OLAP_ERR_NO_AVAILABLE_ROOT_PATH = -223,
    OLAP_ERR_CHECK_LINES_ERROR = -224,
    OLAP_ERR_TRANSACTION_NOT_EXIST = -226,
    OLAP_ERR_DISK_FAILURE = -227,
    OLAP_ERR_TRANSACTION_ALREADY_COMMITTED = -228,
    OLAP_ERR_TRANSACTION_ALREADY_VISIBLE = -229,
    OLAP_ERR_VERSION_ALREADY_MERGED = -230,
    OLAP_ERR_DISK_REACH_CAPACITY_LIMIT = -232,
    OLAP_ERR_TOO_MANY_TRANSACTIONS = -233,
    OLAP_ERR_INVALID_SNAPSHOT_VERSION = -234,

    // CommandExecutor
    // [-300, -400)
    OLAP_ERR_CE_CMD_PARAMS_ERROR = -300,

    // Tablet
    // [-400, -500)
    OLAP_ERR_TABLE_VERSION_DUPLICATE_ERROR = -400,

    // Reader
    // [-700, -800)
    OLAP_ERR_READER_GET_ITERATOR_ERROR = -701,
    OLAP_ERR_CAPTURE_ROWSET_READER_ERROR = -702,

    // BaseCompaction
    // [-800, -900)
    OLAP_ERR_CAPTURE_ROWSET_ERROR = -804,
    OLAP_ERR_BE_NO_SUITABLE_VERSION = -808,
    OLAP_ERR_BE_SEGMENTS_OVERLAPPING = -812,

    // PUSH
    // [-900, -1000)
    OLAP_ERR_PUSH_INIT_ERROR = -900,
    OLAP_ERR_PUSH_VERSION_ALREADY_EXIST = -908,
    OLAP_ERR_PUSH_TABLE_NOT_EXIST = -909,
    OLAP_ERR_PUSH_INPUT_DATA_ERROR = -910,
    OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST = -911,
    // only support realtime push api, batch process is deprecated and is removed
    OLAP_ERR_PUSH_BATCH_PROCESS_REMOVED = -912,
    OLAP_ERR_PUSH_ROWSET_NOT_FOUND = -914,

    // OLAPData
    // [-1100, -1200)
    OLAP_ERR_DATA_EOF = -1102,

    // OLAPDataWriter
    // [-1200, -1300)
    OLAP_ERR_WRITER_DATA_WRITE_ERROR = -1201,

    // RowBlock
    // [-1300, -1400)
    OLAP_ERR_ROWBLOCK_READ_INFO_ERROR = -1302,

    // TabletMeta
    // [-1400, -1500)
    OLAP_ERR_HEADER_DELETE_VERSION = -1401,
    OLAP_ERR_HEADER_INVALID_FLAG = -1404,
    OLAP_ERR_HEADER_LOAD_JSON_HEADER = -1410,
    OLAP_ERR_HEADER_HAS_PENDING_DATA = -1413,

    // SchemaHandler
    // [-1600, -1606)
    OLAP_ERR_ALTER_DELTA_DOES_NOT_EXISTS = -1601,
    OLAP_ERR_ALTER_STATUS_ERR = -1602,

    // DeleteHandler
    // [-1900, -2000)
    OLAP_ERR_DELETE_INVALID_CONDITION = -1900,
    OLAP_ERR_DELETE_INVALID_PARAMETERS = -1903,
    OLAP_ERR_DELETE_INVALID_VERSION = -1904,

    // Cumulative Handler
    // [-2000, -3000)
    OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS = -2000,
    OLAP_ERR_CUMULATIVE_INVALID_PARAMETERS = -2002,
    OLAP_ERR_CUMULATIVE_MISS_VERSION = -2006,

    // KVStore
    // [-3000, -3100)
    OLAP_ERR_META_INVALID_ARGUMENT = -3000,
    OLAP_ERR_META_OPEN_DB = -3001,
    OLAP_ERR_META_KEY_NOT_FOUND = -3002,
    OLAP_ERR_META_GET = -3003,
    OLAP_ERR_META_PUT = -3004,
    OLAP_ERR_META_ITERATOR = -3005,
    OLAP_ERR_META_DELETE = -3006,
    OLAP_ERR_META_ALREADY_EXIST = -3007,

    // Rowset
    // [-3100, -3200)
    OLAP_ERR_ROWSET_SAVE_FAILED = -3101,
    OLAP_ERR_ROWSET_DELETE_FILE_FAILED = -3103,
    OLAP_ERR_ROWSET_BUILDER_INIT = -3104,
    OLAP_ERR_ROWSET_TYPE_NOT_FOUND = -3105,
    OLAP_ERR_ROWSET_ALREADY_EXIST = -3106,
    OLAP_ERR_ROWSET_INVALID = -3108,
    OLAP_ERR_ROWSET_LOAD_FAILED = -3109,
    OLAP_ERR_ROWSET_READER_INIT = -3110,
    OLAP_ERR_ROWSET_READ_FAILED = -3111,
};

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

#ifndef RETURN_NOT_OK
#define RETURN_NOT_OK(s)                         \
    do {                                         \
        OLAPStatus _s = (s);                     \
        if (OLAP_UNLIKELY(_s != OLAP_SUCCESS)) { \
            return _s;                           \
        }                                        \
    } while (0)
#endif

#ifndef RETURN_NOT_OK_LOG
#define RETURN_NOT_OK_LOG(s, msg)                          \
    do {                                                   \
        OLAPStatus _s = (s);                               \
        if (OLAP_UNLIKELY(_s != OLAP_SUCCESS)) {           \
            LOG(WARNING) << (msg) << "[res=" << _s << "]"; \
            return _s;                                     \
        }                                                  \
    } while (0);
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
#define BUILD_VERSION "Unknow"
#endif

} // namespace starrocks

#endif // STARROCKS_BE_SRC_OLAP_OLAP_DEFINE_H

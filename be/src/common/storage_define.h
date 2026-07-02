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

#pragma once

#include <cstdint>
#include <cstdlib>
#include <sstream>
#include <string>

#include "common/constexpr.h"
#include "common/storage_path_constants.h"

namespace starrocks {

static const uint64_t OLAP_FIX_HEADER_MAGIC_NUMBER = 0;

// Magic number for shared-data (lake) tablet metadata / txn log files that carry a
// FixedFileHeader with an Adler-32 checksum. It is chosen so that its little-endian first
// on-disk byte is 0xFE, which can never be the first byte of a serialized protobuf message.
//
// Rationale (independent of field ordering): every protobuf message begins with a field
// tag varint, and the wire type is the low 3 bits of that varint, i.e. the low 3 bits of
// the first byte. 0xFE has low 3 bits == 6, and wire type 6 is not a valid/emitted protobuf
// wire type (only 0,1,2,3,4,5 exist; 3/4 are the deprecated groups). So no protobuf
// serializer ever emits a message whose first byte is 0xFE, regardless of which field is
// serialized first. Readers can therefore distinguish the new checksummed format from a
// legacy headerless protobuf by inspecting the first byte, with zero false-positive risk.
// The remaining bytes spell "LAKEMTA". Never reuse OLAP_FIX_HEADER_MAGIC_NUMBER (0) here.
static const uint64_t LAKE_META_HEADER_MAGIC_NUMBER = 0x41544D454B414CFEULL;

// Flag OR'ed into the high bit of the trailing 8-byte size field of a bundle tablet metadata file
// to mark the checksummed footer layout, written when lake_enable_protobuf_file_checksum is on:
//   [bundle_meta][crc32(bundle_meta)][size | LAKE_BUNDLE_META_CHECKSUM_FLAG]
// versus the legacy footer [bundle_meta][size]. This is a *deterministic* discriminator with zero
// collision risk: the bundle metadata size is bounded by protobuf's 2GB message limit and so is
// always far below 2^63, meaning a legacy file can never have this bit set. Readers test the bit to
// pick the layout and recover the real size by masking it off.
static const uint64_t LAKE_BUNDLE_META_CHECKSUM_FLAG = 0x8000000000000000ULL;

uint32_t get_olap_string_max_length();

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
static const std::string TMP_PREFIX = kStorageTmpPrefix;              // NOLINT
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

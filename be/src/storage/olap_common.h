// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/olap_common.h

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

#include <netinet/in.h>

#include <functional>
#include <list>
#include <map>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <typeinfo>
#include <unordered_map>
#include <unordered_set>

#include "gen_cpp/Types_types.h"
#include "storage/delete_condition.h"
#include "storage/olap_define.h"
#include "util/guard.h"
#include "util/hash_util.hpp"
#include "util/uid_util.h"

#define LOW_56_BITS 0x00ffffffffffffff

namespace starrocks {

static const int64_t MAX_ROWSET_ID = 1L << 56;

typedef int32_t SchemaHash;
typedef __int128 int128_t;
typedef unsigned __int128 uint128_t;

typedef UniqueId TabletUid;

enum CompactionType { BASE_COMPACTION = 1, CUMULATIVE_COMPACTION = 2, UPDATE_COMPACTION = 3, INVALID_COMPACTION };

inline std::string to_string(CompactionType type) {
    switch (type) {
    case BASE_COMPACTION:
        return "base";
    case CUMULATIVE_COMPACTION:
        return "cumulative";
    case UPDATE_COMPACTION:
        return "update";
    case INVALID_COMPACTION:
        return "invalid";
    default:
        return "unknown";
    }
}

struct DataDirInfo {
    DataDirInfo() {}

    std::string path;
    size_t path_hash{0};
    int64_t disk_capacity{1}; // actual disk capacity
    int64_t available{0};
    int64_t data_used_capacity{0};
    bool is_used{false};
    TStorageMedium::type storage_medium; // storage medium: SSD|HDD
};

struct TabletInfo {
    TabletInfo(TTabletId in_tablet_id, TSchemaHash in_schema_hash, const UniqueId& in_uid)
            : tablet_id(in_tablet_id), schema_hash(in_schema_hash), tablet_uid(in_uid) {}
    TabletInfo(const TabletInfo& other) = default;

    bool operator<(const TabletInfo& right) const {
        if (tablet_id != right.tablet_id) {
            return tablet_id < right.tablet_id;
        } else if (schema_hash != right.schema_hash) {
            return schema_hash < right.schema_hash;
        } else {
            return tablet_uid < right.tablet_uid;
        }
    }

    std::string to_string() const {
        std::stringstream ss;
        ss << tablet_id << "." << schema_hash << "." << tablet_uid.to_string();
        return ss.str();
    }

    TTabletId tablet_id;
    TSchemaHash schema_hash;
    UniqueId tablet_uid;
};

enum RangeCondition {
    GT = 0, // greater than
    GE = 1, // greater or equal
    LT = 2, // less than
    LE = 3, // less or equal
};

enum MaterializeType {
    OLAP_MATERIALIZE_TYPE_UNKNOWN = 0,
    OLAP_MATERIALIZE_TYPE_PERCENTILE = 1,
    OLAP_MATERIALIZE_TYPE_HLL = 2,
    OLAP_MATERIALIZE_TYPE_BITMAP = 3,
    OLAP_MATERIALIZE_TYPE_COUNT = 4
};

enum FieldType {
    OLAP_FIELD_TYPE_UNKNOWN = 0, // UNKNOW Type
    OLAP_FIELD_TYPE_TINYINT = 1, // MYSQL_TYPE_TINY
    OLAP_FIELD_TYPE_UNSIGNED_TINYINT = 2,
    OLAP_FIELD_TYPE_SMALLINT = 3, // MYSQL_TYPE_SHORT
    OLAP_FIELD_TYPE_UNSIGNED_SMALLINT = 4,
    OLAP_FIELD_TYPE_INT = 5, // MYSQL_TYPE_LONG
    OLAP_FIELD_TYPE_UNSIGNED_INT = 6,
    OLAP_FIELD_TYPE_BIGINT = 7, // MYSQL_TYPE_LONGLONG
    OLAP_FIELD_TYPE_UNSIGNED_BIGINT = 8,
    OLAP_FIELD_TYPE_LARGEINT = 9,
    OLAP_FIELD_TYPE_FLOAT = 10,  // MYSQL_TYPE_FLOAT
    OLAP_FIELD_TYPE_DOUBLE = 11, // MYSQL_TYPE_DOUBLE
    OLAP_FIELD_TYPE_DISCRETE_DOUBLE = 12,
    OLAP_FIELD_TYPE_CHAR = 13,     // MYSQL_TYPE_STRING
    OLAP_FIELD_TYPE_DATE = 14,     // MySQL_TYPE_NEWDATE
    OLAP_FIELD_TYPE_DATETIME = 15, // MySQL_TYPE_DATETIME
    OLAP_FIELD_TYPE_DECIMAL = 16,  // DECIMAL, using different store format against MySQL
    OLAP_FIELD_TYPE_VARCHAR = 17,

    OLAP_FIELD_TYPE_STRUCT = 18, // Struct
    OLAP_FIELD_TYPE_ARRAY = 19,  // ARRAY
    OLAP_FIELD_TYPE_MAP = 20,    // Map
    OLAP_FIELD_TYPE_NONE = 22,
    OLAP_FIELD_TYPE_HLL = 23,
    OLAP_FIELD_TYPE_BOOL = 24,
    OLAP_FIELD_TYPE_OBJECT = 25,

    // Added by StarRocks
    // Reserved some field for commutiy version

    // decimal v3 type
    OLAP_FIELD_TYPE_DECIMAL32 = 47,
    OLAP_FIELD_TYPE_DECIMAL64 = 48,
    OLAP_FIELD_TYPE_DECIMAL128 = 49,
    OLAP_FIELD_TYPE_DATE_V2 = 50,
    OLAP_FIELD_TYPE_TIMESTAMP = 51,
    OLAP_FIELD_TYPE_DECIMAL_V2 = 52,
    OLAP_FIELD_TYPE_PERCENTILE = 53,

    OLAP_FIELD_TYPE_JSON = 54,

    // max value of FieldType, newly-added type should not exceed this value.
    // used to create a fixed-size hash map.
    OLAP_FIELD_TYPE_MAX_VALUE = 55
};

inline const char* field_type_to_string(FieldType type) {
    switch (type) {
    case OLAP_FIELD_TYPE_TINYINT:
        return "TINYINT";
    case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
        return "UNSIGNED TINYINT";
    case OLAP_FIELD_TYPE_SMALLINT:
        return "SMALLINT";
    case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
        return "UNSIGNED SMALLINT";
    case OLAP_FIELD_TYPE_INT:
        return "INT";
    case OLAP_FIELD_TYPE_UNSIGNED_INT:
        return "UNSIGNED INT";
    case OLAP_FIELD_TYPE_BIGINT:
        return "BIGINT";
    case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
        return "UNSIGNED BIGINT";
    case OLAP_FIELD_TYPE_LARGEINT:
        return "LARGEINT";
    case OLAP_FIELD_TYPE_FLOAT:
        return "FLOAT";
    case OLAP_FIELD_TYPE_DOUBLE:
        return "DOUBLE";
    case OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
        return "DISCRETE DOUBLE";
    case OLAP_FIELD_TYPE_CHAR:
        return "CHAR";
    case OLAP_FIELD_TYPE_DATE:
        return "DATE";
    case OLAP_FIELD_TYPE_DATETIME:
        return "DATETIME";
    case OLAP_FIELD_TYPE_DECIMAL:
        return "DECIMAL";
    case OLAP_FIELD_TYPE_VARCHAR:
        return "VARCHAR";
    case OLAP_FIELD_TYPE_STRUCT:
        return "STRUCT";
    case OLAP_FIELD_TYPE_ARRAY:
        return "ARRAY";
    case OLAP_FIELD_TYPE_MAP:
        return "MAP";
    case OLAP_FIELD_TYPE_UNKNOWN:
        return "UNKNOWN";
    case OLAP_FIELD_TYPE_NONE:
        return "NONE";
    case OLAP_FIELD_TYPE_HLL:
        return "HLL";
    case OLAP_FIELD_TYPE_BOOL:
        return "BOOL";
    case OLAP_FIELD_TYPE_OBJECT:
        return "OBJECT";
    case OLAP_FIELD_TYPE_DECIMAL32:
        return "DECIMAL32";
    case OLAP_FIELD_TYPE_DECIMAL64:
        return "DECIMAL64";
    case OLAP_FIELD_TYPE_DECIMAL128:
        return "DECIMAL128";
    case OLAP_FIELD_TYPE_DATE_V2:
        return "DATE V2";
    case OLAP_FIELD_TYPE_TIMESTAMP:
        return "TIMESTAMP";
    case OLAP_FIELD_TYPE_DECIMAL_V2:
        return "DECIMAL V2";
    case OLAP_FIELD_TYPE_PERCENTILE:
        return "PERCENTILE";
    case OLAP_FIELD_TYPE_JSON:
        return "JSON";
    case OLAP_FIELD_TYPE_MAX_VALUE:
        return "MAX VALUE";
    }
    return "";
}

inline std::ostream& operator<<(std::ostream& os, FieldType type) {
    os << field_type_to_string(type);
    return os;
}

enum FieldAggregationMethod {
    OLAP_FIELD_AGGREGATION_NONE = 0,
    OLAP_FIELD_AGGREGATION_SUM = 1,
    OLAP_FIELD_AGGREGATION_MIN = 2,
    OLAP_FIELD_AGGREGATION_MAX = 3,
    OLAP_FIELD_AGGREGATION_REPLACE = 4,
    OLAP_FIELD_AGGREGATION_HLL_UNION = 5,
    OLAP_FIELD_AGGREGATION_UNKNOWN = 6,
    OLAP_FIELD_AGGREGATION_BITMAP_UNION = 7,
    // Replace if and only if added value is not null
    OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL = 8,
    OLAP_FIELD_AGGREGATION_PERCENTILE_UNION = 9
};

inline const char* aggregation_method_to_string(FieldAggregationMethod method) {
    switch (method) {
    case OLAP_FIELD_AGGREGATION_NONE:
        return "NONE";
    case OLAP_FIELD_AGGREGATION_SUM:
        return "SUM";
    case OLAP_FIELD_AGGREGATION_MIN:
        return "MIN";
    case OLAP_FIELD_AGGREGATION_MAX:
        return "MAX";
    case OLAP_FIELD_AGGREGATION_REPLACE:
        return "REPLACE";
    case OLAP_FIELD_AGGREGATION_HLL_UNION:
        return "HLL_UNION";
    case OLAP_FIELD_AGGREGATION_UNKNOWN:
        return "UNKNOWN";
    case OLAP_FIELD_AGGREGATION_BITMAP_UNION:
        return "BITMAP_UNION";
    case OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL:
        return "REPLACE_IF_NOT_NULL";
    case OLAP_FIELD_AGGREGATION_PERCENTILE_UNION:
        return "PERCENTILE_UNION";
    }
    return "";
}

template <FieldType TYPE>
inline constexpr FieldType DelegateType = TYPE;
template <>
inline constexpr FieldType DelegateType<OLAP_FIELD_TYPE_DECIMAL32> = OLAP_FIELD_TYPE_INT;
template <>
inline constexpr FieldType DelegateType<OLAP_FIELD_TYPE_DECIMAL64> = OLAP_FIELD_TYPE_BIGINT;
template <>
inline constexpr FieldType DelegateType<OLAP_FIELD_TYPE_DECIMAL128> = OLAP_FIELD_TYPE_LARGEINT;

inline FieldType delegate_type(FieldType type) {
    switch (type) {
    case OLAP_FIELD_TYPE_DECIMAL32:
        return OLAP_FIELD_TYPE_INT;
    case OLAP_FIELD_TYPE_DECIMAL64:
        return OLAP_FIELD_TYPE_BIGINT;
    case OLAP_FIELD_TYPE_DECIMAL128:
        return OLAP_FIELD_TYPE_LARGEINT;
    default:
        return type;
    }
}

inline bool is_string_type(FieldType type) {
    return type == FieldType::OLAP_FIELD_TYPE_CHAR || type == FieldType::OLAP_FIELD_TYPE_VARCHAR;
}

inline bool is_decimalv3_field_type(FieldType type) {
    return type == OLAP_FIELD_TYPE_DECIMAL32 || type == OLAP_FIELD_TYPE_DECIMAL64 || type == OLAP_FIELD_TYPE_DECIMAL128;
}

inline std::ostream& operator<<(std::ostream& os, FieldAggregationMethod method) {
    os << aggregation_method_to_string(method);
    return os;
}

enum OLAPCompressionType {
    OLAP_COMP_TRANSPORT = 1,
    OLAP_COMP_STORAGE = 2,
    OLAP_COMP_LZ4 = 3,
};

enum PushType {
    PUSH_FOR_DELETE = 2, // for delete
    PUSH_NORMAL_V2 = 4,  // for spark load
};

enum ReaderType {
    READER_QUERY = 0,
    READER_ALTER_TABLE = 1,
    READER_BASE_COMPACTION = 2,
    READER_CUMULATIVE_COMPACTION = 3,
    READER_CHECKSUM = 4,
};

inline bool is_query(ReaderType reader_type) {
    return reader_type == READER_QUERY;
}

inline bool is_compaction(ReaderType reader_type) {
    return reader_type == READER_BASE_COMPACTION || reader_type == READER_CUMULATIVE_COMPACTION;
}

// <start_version_id, end_version_id>, such as <100, 110>
//using Version = std::pair<TupleVersion, TupleVersion>;

struct Version {
    int64_t first{0};
    int64_t second{0};

    Version(int64_t first_, int64_t second_) : first(first_), second(second_) {}
    Version() {}

    Version& operator=(const Version& version) {
        first = version.first;
        second = version.second;
        return *this;
    }

    friend std::ostream& operator<<(std::ostream& os, const Version& version);

    bool operator!=(const Version& rhs) const { return first != rhs.first || second != rhs.second; }

    bool operator==(const Version& rhs) const { return first == rhs.first && second == rhs.second; }

    bool contains(const Version& other) const { return first <= other.first && second >= other.second; }

    bool operator<(const Version& rhs) const {
        if (second < rhs.second) {
            return true;
        } else if (second > rhs.second) {
            return false;
        } else {
            // version with bigger first will be smaller.
            // design this for fast search in _contains_version() in tablet.cpp.
            return first > rhs.first;
        }
    }
};

typedef std::vector<Version> Versions;

inline std::ostream& operator<<(std::ostream& os, const Version& version) {
    return os << "[" << version.first << "-" << version.second << "]";
}

// used for hash-struct of hash_map<Version, Rowset*>.
struct HashOfVersion {
    size_t operator()(const Version& version) const {
        size_t seed = 0;
        seed = HashUtil::hash64(&version.first, sizeof(version.first), seed);
        seed = HashUtil::hash64(&version.second, sizeof(version.second), seed);
        return seed;
    }
};

// ReaderStatistics used to collect statistics when scan data from storage
struct OlapReaderStatistics {
    int64_t create_segment_iter_ns = 0;
    int64_t io_ns = 0;
    int64_t compressed_bytes_read = 0;

    int64_t decompress_ns = 0;
    int64_t uncompressed_bytes_read = 0;

    // total read bytes in memory
    int64_t bytes_read = 0;

    int64_t block_load_ns = 0;
    int64_t blocks_load = 0;
    int64_t block_fetch_ns = 0; // time of rowset reader's `next_batch()` call
    int64_t block_seek_num = 0;
    int64_t block_seek_ns = 0;
    int64_t block_convert_ns = 0;

    int64_t decode_dict_ns = 0;
    int64_t late_materialize_ns = 0;

    int64_t raw_rows_read = 0;

    int64_t rows_vec_cond_filtered = 0;
    int64_t vec_cond_ns = 0;
    int64_t vec_cond_evaluate_ns = 0;
    int64_t vec_cond_chunk_copy_ns = 0;
    int64_t branchless_cond_evaluate_ns = 0;
    int64_t expr_cond_evaluate_ns = 0;

    int64_t get_rowsets_ns = 0;
    int64_t get_delvec_ns = 0;
    int64_t segment_init_ns = 0;
    int64_t segment_create_chunk_ns = 0;

    int64_t segment_stats_filtered = 0;
    int64_t rows_key_range_filtered = 0;
    int64_t rows_stats_filtered = 0;
    int64_t rows_bf_filtered = 0;
    int64_t rows_del_filtered = 0;
    int64_t del_filter_ns = 0;

    int64_t index_load_ns = 0;

    int64_t total_pages_num = 0;
    int64_t cached_pages_num = 0;

    int64_t rows_bitmap_index_filtered = 0;
    int64_t bitmap_index_filter_timer = 0;

    int64_t rows_del_vec_filtered = 0;

    int64_t rowsets_read_count = 0;
    int64_t segments_read_count = 0;
    int64_t total_columns_data_page_count = 0;

    int64_t runtime_stats_filtered = 0;

    int64_t read_pk_index_ns = 0;
};

typedef uint32_t ColumnId;
// Column unique id set
typedef std::set<uint32_t> UniqueIdSet;
// Column unique Id -> column id map
typedef std::map<ColumnId, ColumnId> UniqueIdToColumnIdMap;

// 8 bit rowset id version
// 56 bit, inc number from 1
// 128 bit backend uid, it is a uuid bit, id version
struct RowsetId {
    int8_t version = 0;
    int64_t hi = 0;
    int64_t mi = 0;
    int64_t lo = 0;

    void init(std::string_view rowset_id_str);

    // to compatiable with old version
    void init(int64_t rowset_id) { init(1, rowset_id, 0, 0); }

    void init(int64_t id_version, int64_t high, int64_t middle, int64_t low) {
        version = static_cast<int8_t>(id_version);
        if (UNLIKELY(high >= MAX_ROWSET_ID)) {
            LOG(FATAL) << "inc rowsetid is too large:" << high;
        }
        hi = (id_version << 56) + (high & LOW_56_BITS);
        mi = middle;
        lo = low;
    }

    std::string to_string() const {
        if (version < 2) {
            return std::to_string(hi & LOW_56_BITS);
        } else {
            char buf[48];
            to_hex(hi, buf);
            to_hex(mi, buf + 16);
            to_hex(lo, buf + 32);
            return {buf, 48};
        }
    }

    int64_t id() { return hi & LOW_56_BITS; }

    // std::unordered_map need this api
    bool operator==(const RowsetId& rhs) const { return hi == rhs.hi && mi == rhs.mi && lo == rhs.lo; }

    bool operator!=(const RowsetId& rhs) const { return hi != rhs.hi || mi != rhs.mi || lo != rhs.lo; }

    bool operator<(const RowsetId& rhs) const {
        if (hi != rhs.hi) {
            return hi < rhs.hi;
        } else if (mi != rhs.mi) {
            return mi < rhs.mi;
        } else {
            return lo < rhs.lo;
        }
    }

    friend std::ostream& operator<<(std::ostream& out, const RowsetId& rowset_id) {
        out << rowset_id.to_string();
        return out;
    }
};

struct TabletSegmentId {
    int64_t tablet_id = INT64_MAX;
    uint32_t segment_id = UINT32_MAX;
    bool operator==(const TabletSegmentId& rhs) const {
        return tablet_id == rhs.tablet_id && segment_id == rhs.segment_id;
    }
    std::string to_string() const {
        std::stringstream ss;
        ss << tablet_id << "_" << segment_id;
        return ss.str();
    };
};

} // namespace starrocks

namespace std {
template <>
struct hash<starrocks::TabletSegmentId> {
    std::size_t operator()(const starrocks::TabletSegmentId& rssid) const {
        return starrocks::HashUtil::hash64(&rssid, 12, 0);
    }
};
} // namespace std

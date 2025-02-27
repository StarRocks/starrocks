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

#include <cstdint>
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
    DataDirInfo() = default;

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
    READER_BYPASS_QUERY = 5,
};

inline bool is_query(ReaderType reader_type) {
    return reader_type == READER_QUERY || reader_type == READER_BYPASS_QUERY;
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
    Version() = default;

    Version& operator=(const Version& version) = default;

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

    int64_t decode_dict_ns = 0;
    int64_t late_materialize_ns = 0;

    int64_t raw_rows_read = 0;

    int64_t rows_vec_cond_filtered = 0;
    int64_t vec_cond_ns = 0;
    int64_t vec_cond_evaluate_ns = 0;
    int64_t rf_cond_input_rows = 0;
    int64_t rf_cond_output_rows = 0;
    int64_t rf_cond_evaluate_ns = 0;
    int64_t vec_cond_chunk_copy_ns = 0;
    int64_t branchless_cond_evaluate_ns = 0;
    int64_t expr_cond_evaluate_ns = 0;

    int64_t get_rowsets_ns = 0;
    int64_t get_delvec_ns = 0;
    int64_t get_delta_column_group_ns = 0;
    int64_t segment_init_ns = 0;
    int64_t column_iterator_init_ns = 0;
    int64_t bitmap_index_iterator_init_ns = 0;
    int64_t zone_map_filter_ns = 0;
    int64_t rows_key_range_filter_ns = 0;
    int64_t bf_filter_ns = 0;

    int64_t segment_stats_filtered = 0;
    int64_t rows_key_range_filtered = 0;
    int64_t rows_after_key_range = 0;
    int64_t rows_key_range_num = 0;
    int64_t rows_stats_filtered = 0;
    int64_t rows_vector_index_filtered = 0;
    int64_t rows_bf_filtered = 0;
    int64_t rows_del_filtered = 0;
    int64_t del_filter_ns = 0;

    int64_t total_pages_num = 0;
    int64_t cached_pages_num = 0;

    int64_t rows_bitmap_index_filtered = 0;
    int64_t bitmap_index_filter_timer = 0;
    int64_t get_row_ranges_by_vector_index_timer = 0;
    int64_t vector_search_timer = 0;
    int64_t process_vector_distance_and_id_timer = 0;

    int64_t rows_del_vec_filtered = 0;

    int64_t rows_gin_filtered = 0;
    int64_t gin_index_filter_ns = 0;

    int64_t rowsets_read_count = 0;
    int64_t segments_read_count = 0;
    int64_t total_columns_data_page_count = 0;

    int64_t runtime_stats_filtered = 0;

    int64_t read_pk_index_ns = 0;

    // ------ for lake tablet ------
    int64_t pages_from_local_disk = 0;

    int64_t compressed_bytes_read_local_disk = 0;
    int64_t compressed_bytes_write_local_disk = 0;
    int64_t compressed_bytes_read_remote = 0;
    // bytes read requested from be, same as compressed_bytes_read for local tablet
    int64_t compressed_bytes_read_request = 0;

    int64_t io_count = 0;
    int64_t io_count_local_disk = 0;
    int64_t io_count_remote = 0;
    int64_t io_count_request = 0;

    int64_t io_ns_read_local_disk = 0;
    int64_t io_ns_write_local_disk = 0;
    int64_t io_ns_remote = 0;

    int64_t prefetch_hit_count = 0;
    int64_t prefetch_wait_finish_ns = 0;
    int64_t prefetch_pending_ns = 0;
    // ------ for lake tablet ------

    // ------ for json type, to count flat column ------
    // key: json absolute path, value: count
    int64_t json_flatten_ns = 0;
    int64_t json_cast_ns = 0;
    int64_t json_merge_ns = 0;
    int64_t json_init_ns = 0;
    std::unordered_map<std::string, int64_t> flat_json_hits;
    std::unordered_map<std::string, int64_t> merge_json_hits;
    std::unordered_map<std::string, int64_t> dynamic_json_hits;

    // Counters for data sampling
    int64_t sample_time_ns = 0;               // Records the time to prepare sample, actual IO time is not included
    int64_t sample_size = 0;                  // Records the number of hits in the sample. Granularity can be BLOCK/PAGE
    int64_t sample_population_size = 0;       // Records the total number of samples. Granularity can be BLOCK/PAGE
    int64_t sample_build_histogram_count = 0; // Records the number of histogram built for sampling
    int64_t sample_build_histogram_time_ns = 0; // Records the time to build histogram
};

// OlapWriterStatistics used to collect statistics when write data to storage
struct OlapWriterStatistics {
    int64_t segment_write_ns = 0;
};

const char* const kBytesReadLocalDisk = "bytes_read_local_disk";
const char* const kBytesWriteLocalDisk = "bytes_write_local_disk";
const char* const kBytesReadRemote = "bytes_read_remote";
const char* const kIOCountLocalDisk = "io_count_local_disk";
const char* const kIOCountRemote = "io_count_remote";
const char* const kIONsReadLocalDisk = "io_ns_read_local_disk";
const char* const kIONsWriteLocalDisk = "io_ns_write_local_disk";
const char* const kIONsRemote = "io_ns_remote";
const char* const kPrefetchHitCount = "prefetch_hit_count";
const char* const kPrefetchWaitFinishNs = "prefetch_wait_finish_ns";
const char* const kPrefetchPendingNs = "prefetch_pending_ns";

// The position index of a column in a specific TabletSchema starts from 0.
// The position of the same column in different TabletSchema may be different, which
// means that the same column may have a different ColumnId in different contexts, depending
// on the TabletSchema used.
// TODO: Change the name
typedef uint32_t ColumnId;

// A unique identifier for a column within the Tablet scope, unlike ColumnId, ColumnUID
// is independent of where the column appears in the Tablet schema.
// Since versions 3.2.3, ColumnUID are generated by the FE, which uses the Java int
// type, corresponding to the int32_t.
typedef int32_t ColumnUID;

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

struct HashOfRowsetId {
    size_t operator()(const RowsetId& rowset_id) const {
        size_t seed = 0;
        seed = HashUtil::hash64(&rowset_id.hi, sizeof(rowset_id.hi), seed);
        seed = HashUtil::hash64(&rowset_id.mi, sizeof(rowset_id.mi), seed);
        seed = HashUtil::hash64(&rowset_id.lo, sizeof(rowset_id.lo), seed);
        return seed;
    }
};

struct TabletSegmentId {
    int64_t tablet_id = INT64_MAX;
    uint32_t segment_id = UINT32_MAX;
    TabletSegmentId() {}
    TabletSegmentId(int64_t tid, uint32_t sid) : tablet_id(tid), segment_id(sid) {}
    ~TabletSegmentId() {}
    bool operator==(const TabletSegmentId& rhs) const {
        return tablet_id == rhs.tablet_id && segment_id == rhs.segment_id;
    }
    bool operator<(const TabletSegmentId& rhs) const {
        if (tablet_id < rhs.tablet_id) {
            return true;
        } else if (tablet_id > rhs.tablet_id) {
            return false;
        } else {
            return segment_id < rhs.segment_id;
        }
    }
    std::string to_string() const {
        std::stringstream ss;
        ss << tablet_id << "_" << segment_id;
        return ss.str();
    };
    friend std::ostream& operator<<(std::ostream& out, const TabletSegmentId& tsid) {
        out << tsid.to_string();
        return out;
    }
};

struct TabletSegmentIdRange {
    TabletSegmentId left;
    TabletSegmentId right;
    TabletSegmentIdRange(int64_t left_tid, uint32_t left_sid, int64_t right_tid, uint32_t right_sid)
            : left(left_tid, left_sid), right(right_tid, right_sid) {}
    ~TabletSegmentIdRange() {}
    // make sure left <= right
    bool is_valid() const { return left < right || left == right; }
    std::string to_string() const {
        std::stringstream ss;
        ss << "[" << left.to_string() << "," << right.to_string() << "]";
        return ss.str();
    };
    friend std::ostream& operator<<(std::ostream& out, const TabletSegmentIdRange& tsid_range) {
        out << tsid_range.to_string();
        return out;
    }
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

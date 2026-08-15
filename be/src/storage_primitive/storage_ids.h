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

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>

#include "base/compiler_util.h"
#include "base/hash/hash_std.hpp"
#include "base/logging.h"
#include "base/uid_util.h"
#include "gen_cpp/Types_types.h"

#define LOW_56_BITS 0x00ffffffffffffff

namespace starrocks {

static const int64_t MAX_ROWSET_ID = 1L << 56;

typedef int32_t SchemaHash;
typedef UniqueId TabletUid;

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
    TabletSegmentId() = default;
    TabletSegmentId(int64_t tid, uint32_t sid) : tablet_id(tid), segment_id(sid) {}
    ~TabletSegmentId() = default;
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
    ~TabletSegmentIdRange() = default;
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

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

#include "common/status.h"
#include "storage/olap_common.h"

namespace starrocks {

// `DeltaColumnGroup` is used for record the new update columns data
// It is generated when column mode partial update happen.
// `_column_ids` record the columns which have been updated, and `_column_file` points to the data file
// which contains update columns.
// Column data file end as `.cols`.
class DeltaColumnGroup {
public:
    DeltaColumnGroup() {}
    ~DeltaColumnGroup() {}
    void init(int64_t version, const std::vector<uint32_t>& column_ids, const std::string& column_file);
    Status load(int64_t version, const char* data, size_t length);
    std::string save() const;
    int get_column_idx(uint32_t cid) {
        for (int idx = 0; idx < _column_ids.size(); idx++) {
            if (_column_ids[idx] == cid) {
                return idx;
            }
        }
        return -1;
    }
    std::string column_file() const { return _column_file; }
    std::vector<uint32_t> column_ids() const { return _column_ids; }
    int64_t version() const { return _version; }
    void rename_column_file(const std::string& dir, RowsetId new_rowset_id, int segment_id);

    std::string debug_string() {
        std::stringstream ss;
        ss << "ver:" << _version << ", ";
        ss << "file:" << _column_file << ", ";
        ss << "cids:";
        for (uint32_t cid : _column_ids) {
            ss << cid << "|";
        }
        return ss.str();
    }

    size_t memory_usage() const { return _memory_usage; }

private:
    void _calc_memory_usage();

private:
    int64_t _version = 0;
    std::vector<uint32_t> _column_ids;
    std::string _column_file;
    size_t _memory_usage = 0;
};

using DeltaColumnGroupPtr = std::shared_ptr<DeltaColumnGroup>;
using DeltaColumnGroupList = std::vector<DeltaColumnGroupPtr>;

class DeltaColumnGroupLoader {
public:
    DeltaColumnGroupLoader() = default;
    virtual ~DeltaColumnGroupLoader() = default;
    virtual Status load(const TabletSegmentId& tsid, int64_t version, DeltaColumnGroupList* pdcgs) = 0;
};

class DeltaColumnGroupListSerializer {
public:
    static std::string serialize_delta_column_group_list(const DeltaColumnGroupList& dcgs);
    static Status deserialize_delta_column_group_list(const char* data, size_t length, DeltaColumnGroupList* dcgs);
};

} // namespace starrocks
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

#include <string>
#include <unordered_map>

#include "storage/lake/rowset.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_metadata.h"

namespace starrocks {

namespace lake {

class MetaFileBuilder;

struct PartialUpdateState {
    std::vector<uint64_t> src_rss_rowids;
    std::vector<std::unique_ptr<Column>> write_columns;
};

class RowsetUpdateState {
public:
    using ColumnUniquePtr = std::unique_ptr<Column>;

    RowsetUpdateState();
    ~RowsetUpdateState();

    Status load(const TxnLogPB_OpWrite& op_write, const TabletMetadata& metadata, int64_t base_version, Tablet* tablet,
                const MetaFileBuilder* builder);

    Status rewrite_segment(const TxnLogPB_OpWrite& op_write, const TabletMetadata& metadata, Tablet* tablet);

    const std::vector<ColumnUniquePtr>& upserts() const { return _upserts; }
    const std::vector<ColumnUniquePtr>& deletes() const { return _deletes; }

    std::size_t memory_usage() const { return _memory_usage; }

    std::string to_string() const;

    const std::vector<PartialUpdateState>& parital_update_states() { return _partial_update_states; }

    static void plan_read_by_rssid(const std::vector<uint64_t>& rowids, size_t* num_default,
                                   std::map<uint32_t, std::vector<uint32_t>>* rowids_by_rssid,
                                   std::vector<uint32_t>* idxes);

private:
    Status _do_load(const TxnLogPB_OpWrite& op_write, const TabletMetadata& metadata, Tablet* tablet);

    Status _prepare_partial_update_states(const TxnLogPB_OpWrite& op_write, const TabletMetadata& metadata,
                                          Tablet* tablet, const TabletSchema& tablet_schema);

    std::once_flag _load_once_flag;
    Status _status;
    // one for each segment file
    std::vector<ColumnUniquePtr> _upserts;
    // one for each delete file
    std::vector<ColumnUniquePtr> _deletes;
    size_t _memory_usage = 0;
    int64_t _tablet_id = 0;

    // TODO: dump to disk if memory usage is too large
    std::vector<PartialUpdateState> _partial_update_states;

    int64_t _base_version;
    const MetaFileBuilder* _builder;

    RowsetUpdateState(const RowsetUpdateState&) = delete;
    const RowsetUpdateState& operator=(const RowsetUpdateState&) = delete;
};

inline std::ostream& operator<<(std::ostream& os, const RowsetUpdateState& o) {
    os << o.to_string();
    return os;
}

} // namespace lake

} // namespace starrocks

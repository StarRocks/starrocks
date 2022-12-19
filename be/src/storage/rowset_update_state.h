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

#include "storage/olap_common.h"
#include "storage/primary_index.h"
#include "storage/tablet_updates.h"

namespace starrocks {

class Tablet;

struct PartialUpdateState {
    std::vector<uint64_t> src_rss_rowids;
    std::vector<std::unique_ptr<vectorized::Column>> write_columns;
};

class RowsetUpdateState {
public:
    using ColumnUniquePtr = std::unique_ptr<vectorized::Column>;

    RowsetUpdateState();
    ~RowsetUpdateState();

    Status load(Tablet* tablet, Rowset* rowset);

    Status apply(Tablet* tablet, Rowset* rowset, uint32_t rowset_id, EditVersion latest_applied_version,
                 const PrimaryIndex& index);

    const std::vector<ColumnUniquePtr>& upserts() const { return _upserts; }
    const std::vector<ColumnUniquePtr>& deletes() const { return _deletes; }

    std::size_t memory_usage() const { return _memory_usage; }

    std::string to_string() const;

    const std::vector<PartialUpdateState>& parital_update_states() { return _partial_update_states; }

    // call check conflict directly
    // only use for ut of partial update
    Status test_check_conflict(Tablet* tablet, Rowset* rowset, uint32_t rowset_id, EditVersion latest_applied_version,
                               std::vector<uint32_t>& read_column_ids, const PrimaryIndex& index) {
        return _check_and_resolve_conflict(tablet, rowset, rowset_id, latest_applied_version, read_column_ids, index);
    }

    static void plan_read_by_rssid(const vector<uint64_t>& rowids, size_t* num_default,
                                   std::map<uint32_t, std::vector<uint32_t>>* rowids_by_rssid, vector<uint32_t>* idxes);

    Status load_deletes(Rowset* rowset, uint32_t delete_id);
    Status load_upserts(Rowset* rowset, uint32_t upsert_id);
    void release_upserts(uint32_t idx);
    void release_deletes(uint32_t idx);

private:
    Status _load_deletes(Rowset* rowset, uint32_t delete_id, vectorized::Column* pk_column);
    Status _load_upserts(Rowset* rowset, uint32_t upsert_id, vectorized::Column* pk_column);

    Status _do_load(Tablet* tablet, Rowset* rowset);

    Status _prepare_partial_update_states(Tablet* tablet, Rowset* rowset);

    Status _check_and_resolve_conflict(Tablet* tablet, Rowset* rowset, uint32_t rowset_id,
                                       EditVersion latest_applied_version, std::vector<uint32_t>& read_column_ids,
                                       const PrimaryIndex& index);

    bool _check_partial_update(Rowset* rowset);

    std::once_flag _load_once_flag;
    Status _status;
    // one for each segment file
    std::vector<ColumnUniquePtr> _upserts;
    // one for each delete file
    std::vector<ColumnUniquePtr> _deletes;
    size_t _memory_usage = 0;
    int64_t _tablet_id = 0;

    // states for partial update
    EditVersion _read_version;
    uint32_t _next_rowset_id = 0;

    // TODO: dump to disk if memory usage is too large
    std::vector<PartialUpdateState> _partial_update_states;

    RowsetUpdateState(const RowsetUpdateState&) = delete;
    const RowsetUpdateState& operator=(const RowsetUpdateState&) = delete;
};

inline std::ostream& operator<<(std::ostream& os, const RowsetUpdateState& o) {
    os << o.to_string();
    return os;
}

} // namespace starrocks

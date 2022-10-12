// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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

private:
    Status _do_load(Tablet* tablet, Rowset* rowset);

    Status _prepare_partial_update_states(Tablet* tablet, Rowset* rowset);

    Status _check_and_resolve_conflict(Tablet* tablet, Rowset* rowset, uint32_t rowset_id,
                                       EditVersion latest_applied_version, std::vector<uint32_t>& read_column_ids,
                                       const PrimaryIndex& index);

    Status _update_rowset_meta(Tablet* tablet, Rowset* rowset);

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

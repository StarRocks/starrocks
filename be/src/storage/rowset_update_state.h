// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <string>
#include <unordered_map>

#include "storage/olap_common.h"
#include "storage/primary_index.h"

namespace starrocks {

class Rowset;
using RowsetSharedPtr = std::shared_ptr<Rowset>;
class Tablet;
class TabletMeta;
using TabletSharedPtr = std::shared_ptr<Tablet>;

class RowsetUpdateState {
public:
    using ColumnUniquePtr = std::unique_ptr<vectorized::Column>;

    RowsetUpdateState();
    ~RowsetUpdateState();

    Status load(int64_t tablet_id, Rowset* rowset);

    const std::vector<ColumnUniquePtr>& upserts() const { return _upserts; }
    const std::vector<ColumnUniquePtr>& deletes() const { return _deletes; }

    std::size_t memory_usage() const { return _memory_usage; }

    std::string to_string() const;

private:
    Status _do_load(Rowset* rowset);

    std::once_flag _load_once_flag;
    Status _status;
    // one for each segment file
    std::vector<ColumnUniquePtr> _upserts;
    // one for each delete file
    std::vector<ColumnUniquePtr> _deletes;
    size_t _memory_usage = 0;
    int64_t _tablet_id = 0;

    RowsetUpdateState(const RowsetUpdateState&) = delete;
    const RowsetUpdateState& operator=(const RowsetUpdateState&) = delete;
};

inline std::ostream& operator<<(std::ostream& os, const RowsetUpdateState& o) {
    os << o.to_string();
    return os;
}

} // namespace starrocks

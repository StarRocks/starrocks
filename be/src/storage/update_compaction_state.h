// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <string>
#include <unordered_map>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "storage/olap_common.h"

namespace starrocks {

class Rowset;
using RowsetSharedPtr = std::shared_ptr<Rowset>;

namespace vectorized {

struct CompactionSemgentState {
    ColumnPtr pkeys;
    std::vector<uint32_t> src_rssids;
};

class CompactionState {
public:
    CompactionState();
    ~CompactionState();

    CompactionState(const CompactionState&) = delete;
    CompactionState& operator=(const CompactionState&) = delete;

    Status load(Rowset* rowset);

    size_t memory_usage() const { return _memory_usage; }

    std::vector<CompactionSemgentState> segment_states;

private:
    Status _do_load(Rowset* rowset);

    std::once_flag _load_once_flag;
    Status _status;
    size_t _memory_usage = 0;
};

} // namespace vectorized

} // namespace starrocks

// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <string>

#include "storage/vectorized/compaction.h"

namespace starrocks::vectorized {

class CumulativeCompaction : public Compaction {
public:
    CumulativeCompaction(MemTracker* mem_tracker, TabletSharedPtr tablet);
    ~CumulativeCompaction() override;

    CumulativeCompaction(const CumulativeCompaction&) = delete;
    CumulativeCompaction& operator=(const CumulativeCompaction&) = delete;

    Status compact() override;

protected:
    Status pick_rowsets_to_compact() override;

    // check_version_continuity_with_cumulative_point checks whether the input rowsets is continuous with cumulative point.
    Status check_version_continuity_with_cumulative_point(const std::vector<RowsetSharedPtr>& rowsets);

    std::string compaction_name() const override { return "cumulative compaction"; }

    ReaderType compaction_type() const override { return ReaderType::READER_CUMULATIVE_COMPACTION; }

private:
    int64_t _cumulative_rowset_size_threshold;
};

} // namespace starrocks::vectorized

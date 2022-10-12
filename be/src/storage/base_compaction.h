// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "storage/compaction.h"

namespace starrocks::vectorized {

// BaseCompaction is derived from Compaction.
// BaseCompaction will implements
//   1. its policy to pick rowsests
//   2. do compaction to produce new rowset.

class BaseCompaction : public Compaction {
public:
    explicit BaseCompaction(MemTracker* mem_tracker, TabletSharedPtr tablet);
    ~BaseCompaction() override;

    BaseCompaction(const BaseCompaction&) = delete;
    BaseCompaction& operator=(const BaseCompaction&) = delete;

    Status compact() override;

protected:
    Status pick_rowsets_to_compact() override;
    std::string compaction_name() const override { return "base compaction"; }

    ReaderType compaction_type() const override { return ReaderType::READER_BASE_COMPACTION; }

private:
    // check if all input rowsets are non overlapping among segments.
    // a rowset with overlapping segments should be compacted by cumulative compaction first.
    Status _check_rowset_overlapping(const vector<RowsetSharedPtr>& rowsets);
};

} // namespace starrocks::vectorized

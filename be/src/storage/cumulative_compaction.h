// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <string>

#include "storage/compaction.h"

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

    std::string compaction_name() const override { return "cumulative compaction"; }

    ReaderType compaction_type() const override { return ReaderType::READER_CUMULATIVE_COMPACTION; }
};

} // namespace starrocks::vectorized

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

#include "storage/compaction.h"

namespace starrocks {

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

    bool fit_compaction_condition(const std::vector<RowsetSharedPtr>& rowsets, int64_t compaction_score);

    std::string compaction_name() const override { return "cumulative compaction"; }

    ReaderType compaction_type() const override { return ReaderType::READER_CUMULATIVE_COMPACTION; }
};

} // namespace starrocks

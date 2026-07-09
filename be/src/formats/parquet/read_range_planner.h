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

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "cache/scan/shared_buffered_input_stream.h"
#include "common/global_types.h"
#include "common/status.h"
#include "formats/parquet/utils.h"

namespace starrocks::parquet {

struct GroupReaderParam;
class ColumnReader;

// Plans and coalesces IO ranges for Parquet column readers.
//
// Active ranges are planned and submitted during row-group prepare().
// Lazy ranges are pre-computed but may be submitted on demand (currently
// submitted together with active ranges during prepare; deferred lazy
// submission will be enabled when SharedBufferedInputStream supports
// incremental range addition).
//
// Used by ColumnMaterializer; not called directly by expression evaluation.
class ReadRangePlanner {
public:
    using ColumnReaderMap = std::unordered_map<SlotId, std::unique_ptr<ColumnReader>>;
    using IORange = SharedBufferedInputStream::IORange;

    ReadRangePlanner(const GroupReaderParam& param, ColumnReaderMap* column_readers);

    // Collect IO ranges for a set of column indices into `out`.  `types`
    // controls whether PAGE_INDEX or PAGES ranges are collected.
    // `is_active` tags the collected ranges.
    void collect_ranges(const std::vector<int>& column_indices, bool is_active, std::vector<IORange>* out,
                        int64_t* end_offset, ColumnIOTypeFlags types);

    // Deduplicate exact-duplicate ranges; if two ranges share the same
    // (offset, size), the active flag is OR-ed.
    static void deduplicate(std::vector<IORange>* ranges);

    // Whether active and lazy ranges should be coalesced into a unified
    // stream set (adaptive counter >= 0 or config forces it).
    bool should_coalesce_active_lazy() const;

    // Plan active ranges now (called during prepare).
    // Returns the planned active ranges.
    const std::vector<IORange>& active_ranges() const { return _active_ranges; }

    // Pre-compute lazy ranges (called during prepare).
    // The ranges are stored; actual stream submission is deferred.
    void pre_plan_lazy_ranges(const std::vector<int>& lazy_column_indices, int64_t* end_offset,
                              ColumnIOTypeFlags types);

    // Whether lazy ranges have been planned (non-empty set).
    bool has_lazy_ranges() const { return !_lazy_ranges.empty(); }

    // Retrieve pre-planned lazy ranges for on-demand submission.
    const std::vector<IORange>& lazy_ranges() const { return _lazy_ranges; }

private:
    const GroupReaderParam& _param;
    ColumnReaderMap* _column_readers = nullptr;

    std::vector<IORange> _active_ranges;
    std::vector<IORange> _lazy_ranges;
};

} // namespace starrocks::parquet

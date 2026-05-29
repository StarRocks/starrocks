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

#include <vector>

#include "fs/fs.h"
#include "storage/variant_tuple.h"

namespace starrocks {

class SegmentMetadataPB;

struct SegmentFileInfo : public FileInfo {
    VariantTuple sort_key_min;
    VariantTuple sort_key_max;
    // Equal-row-interval samples of the sort key (NON-DECREASING) collected by
    // SegmentWriter. May be empty for small segments (num_rows <= interval) or
    // segments where sampling was not armed. Always paired with
    // sort_key_sample_row_interval: samples.empty() <=> sort_key_sample_row_interval == 0.
    std::vector<VariantTuple> sort_key_samples;
    int64_t sort_key_sample_row_interval = 0;
    int64_t num_rows = 0;
    // IDs of vector indexes configured for this segment (one .vi file per id).
    std::vector<int64_t> vector_index_ids;

    // Copy sort_key_min, sort_key_max, sort_key_samples, and
    // sort_key_sample_row_interval into a SegmentMetadataPB. Does NOT
    // set num_rows or segment_idx (callers handle those separately).
    void write_sort_key_fields_to(SegmentMetadataPB* segment_meta) const;
};

} // namespace starrocks

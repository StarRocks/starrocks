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
class FileMetaPB;

// Serialize a plain file (a del file, sst, lcrm file, ...) into a FileMetaPB: name, size (when
// known), and encryption_meta (only when non-empty, since an empty encryption_meta means
// "unencrypted", which is the same as absent). Does NOT set `shared` — that flag is not part of
// FileInfo and is assigned separately by the cross-publish paths that need it.
void to_file_meta_pb(const FileInfo& file, FileMetaPB* file_meta);

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

    // Serialize this segment's full per-segment metadata into |segment_meta|: the file attributes
    // (filename, size, encryption_meta, bundle_file_offset), the sort-key fields, num_rows,
    // vector_index_ids, and segment_idx. |segment_idx| is a required input (placed first so callers
    // cannot forget it): a contiguous positional index at write time, or a sparse one after
    // compaction/merge.
    void to_proto(uint32_t segment_idx, SegmentMetadataPB* segment_meta) const;
};

} // namespace starrocks

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

#include <memory>
#include <vector>

#include "common/status.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/lake/tablet_metadata.h"

namespace starrocks::lake {

class PersistentIndexSstable;

struct CompactionCandidateResult {
    std::vector<std::vector<PersistentIndexSstablePB>> candidate_filesets;
    // Whether to merge base level during compaction
    bool merge_base_level = false;
};

struct SizeTieredLevel {
    std::vector<size_t> fileset_indexes; // indexes into filesets vector
    int64_t level_size;
    int64_t total_size;
    double score;

    SizeTieredLevel(std::vector<size_t> indexes, int64_t ls, int64_t ts, double sc)
            : fileset_indexes(std::move(indexes)), level_size(ls), total_size(ts), score(sc) {}
};

struct LevelComparator {
    bool operator()(const SizeTieredLevel* left, const SizeTieredLevel* right) const {
        return left->score > right->score ||
               (left->score == right->score && left->fileset_indexes[0] > right->fileset_indexes[0]);
    }
};

class LakePersistentIndexSizeTieredCompactionStrategy {
public:
    LakePersistentIndexSizeTieredCompactionStrategy() = default;
    ~LakePersistentIndexSizeTieredCompactionStrategy() = default;

    // Pick compaction candidates from tablet metadata.
    // Use size tiered compaction strategy.
    // Parameters:
    //   - sstable_meta: The sstable metadata of the tablet.
    //   - candidates: Output parameter. A vector of compaction candidate groups.
    //       Each group is a vector of sstable metadata (PB) in the same fileset.
    // Returns:
    //   - Status: OK if successful, error status otherwise.
    // Rules:
    //   1. Fileset is the smallest unit for selection — partial selection within a fileset is not allowed.
    //   2. Only consecutive filesets can be compacted together.
    //
    static StatusOr<CompactionCandidateResult> pick_compaction_candidates(
            const PersistentIndexSstableMetaPB& sstable_meta);
};

} // namespace starrocks::lake
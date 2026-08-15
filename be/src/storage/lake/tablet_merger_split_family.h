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
#include <limits>
#include <vector>

#include "common/statusor.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/lake/tablet_metadata.h"

namespace starrocks::lake {

// Split-family inference across the old tablets being merged. Groups old tablets
// that share an sstable filename (Edge 1) or carry the same uid on any rowset
// (Edge 2) — the canonical old tablet of each family is the smallest member
// index. Each family identifies its canonical anchor so downstream code (the
// legacy-shared-sstable rebuild path) can resolve per-family lookup maps.
namespace detail {

// Inferred split families across a set of merge contexts. Each input
// (= one old tablet of the merge) belongs to AT MOST one family (or kNoFamily
// for orphan / standalone old tablets).
struct InferredSplitFamilies {
    static constexpr uint32_t kNoFamily = std::numeric_limits<uint32_t>::max();

    struct Family {
        // member old_tablet_indexes in ascending order. The smallest member is
        // the canonical old tablet for this family.
        std::vector<uint32_t> member_old_tablet_indexes;
        // == member_old_tablet_indexes.front(). Stored explicitly so callers
        // don't have to peek into the vector.
        uint32_t canonical_old_tablet_index = 0;
    };

    // old_tablet_index → family_id (kNoFamily for orphan).
    std::vector<uint32_t> old_tablet_to_family;
    // Indexed by family_id. Emitted in ascending canonical_old_tablet_index
    // order (so the iteration is deterministic and matches the dedup
    // order in merge_sstables).
    std::vector<Family> families;
};

// Infer split families from a vector of merge inputs. Edges:
//   (1) two old tablets share a legacy `shared && !has_shared_rssid` sstable
//       filename (catches the case where rowset duplication is incomplete
//       across old tablets but the shared sstable file is still common);
//   (2) two old tablets carry a rowset with the same uid (universal uid
//       coverage in lake-mode range-distribution tablets means every cross-
//       sibling logical rowset converges on one uid here — split-pruned
//       siblings, shared-ancestor siblings, cross-published siblings alike).
//
// Old tablets with no edges to any other old tablet get kNoFamily. Family ids are
// assigned in ascending canonical_old_tablet_index order; canonical_old_tablet_index
// is always the smallest old_tablet_index in the family.
StatusOr<InferredSplitFamilies> infer_split_families(const TabletMetadataPtrs& old_tablet_metadatas);

} // namespace detail

} // namespace starrocks::lake

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

#include "storage/lake/tablet_merger_split_family.h"

#include <map>
#include <unordered_map>
#include <utility>

#include "base/uid_util.h"
#include "common/status.h"
#include "storage/lake/tablet_reshard_helper.h"

namespace starrocks::lake::detail {

namespace {

// Standard union-find with the smallest member always rising to the root.
// This makes find(x) deterministically return the minimum old_tablet_index in
// x's family, which is exactly what canonical_old_tablet_index needs.
class UnionFind {
public:
    explicit UnionFind(uint32_t n) : _parent(n) {
        for (uint32_t i = 0; i < n; ++i) _parent[i] = i;
    }

    uint32_t find(uint32_t x) {
        while (_parent[x] != x) {
            _parent[x] = _parent[_parent[x]]; // path compression
            x = _parent[x];
        }
        return x;
    }

    void unite(uint32_t x, uint32_t y) {
        const uint32_t root_x = find(x);
        const uint32_t root_y = find(y);
        if (root_x == root_y) return;
        // The smaller root absorbs the larger root, so the canonical
        // (smallest) old tablet stays at the family's root after every union.
        if (root_x < root_y) {
            _parent[root_y] = root_x;
        } else {
            _parent[root_x] = root_y;
        }
    }

private:
    std::vector<uint32_t> _parent;
};

} // namespace

StatusOr<InferredSplitFamilies> infer_split_families(const TabletMetadataPtrs& old_tablet_metadatas) {
    InferredSplitFamilies result;
    const auto old_tablet_count = static_cast<uint32_t>(old_tablet_metadatas.size());
    result.old_tablet_to_family.assign(old_tablet_count, InferredSplitFamilies::kNoFamily);
    if (old_tablet_count == 0) {
        return result;
    }

    UnionFind union_find(old_tablet_count);

    // Edge (1): legacy `shared && !has_shared_rssid` sstable filename.
    // Two old tablets that reference the same legacy file must come from the
    // same SPLIT family (the file is bytes-immutable; SPLIT marks shared
    // copies but doesn't rewrite anything).
    std::unordered_map<std::string, uint32_t> filename_to_first_old_tablet;
    for (uint32_t old_tablet_index = 0; old_tablet_index < old_tablet_count; ++old_tablet_index) {
        const auto& metadata = old_tablet_metadatas[old_tablet_index];
        if (metadata == nullptr || !metadata->has_sstable_meta()) continue;
        for (const auto& sstable : metadata->sstable_meta().sstables()) {
            if (!sstable.shared() || sstable.has_shared_rssid()) continue;
            auto [iter, inserted] = filename_to_first_old_tablet.emplace(sstable.filename(), old_tablet_index);
            if (!inserted) {
                union_find.unite(iter->second, old_tablet_index);
            }
        }
    }

    // Edge (2): uid family edge. Every rowset that should land in the same family
    // carries the same uid: split lineage preserves it via CopyFrom, cross-publish writes
    // mint and propagate it through the txn log, and synthesized rowsets like the
    // column-mode "new rows" derive it deterministically from the source op_write's
    // uid. Range-distribution is a new feature (no migration from existing tables),
    // so every rowset reaching this point carries a valid uid — grouping by uid
    // covers split-pruned siblings (disjoint segments[]) and shared-ancestor
    // siblings (identical segments[]) alike. A locally produced rowset carries a
    // unique uid and matches no sibling.
    std::map<UniqueId, uint32_t> uid_to_first_old_tablet;
    for (uint32_t old_tablet_index = 0; old_tablet_index < old_tablet_count; ++old_tablet_index) {
        const auto& metadata = old_tablet_metadatas[old_tablet_index];
        if (metadata == nullptr) continue;
        for (const auto& rowset : metadata->rowsets()) {
            if (!tablet_reshard_helper::has_valid_uid(rowset)) continue;
            UniqueId uid_key(rowset.uid());
            auto [iter, inserted] = uid_to_first_old_tablet.emplace(uid_key, old_tablet_index);
            if (!inserted && iter->second != old_tablet_index) {
                union_find.unite(iter->second, old_tablet_index);
            }
        }
    }

    // Materialize families. members_by_root[r] ends up holding every old tablet
    // whose union-find root is r. Because UnionFind always promotes the
    // smaller index to the root, members_by_root[r] is non-empty only when
    // r is the smallest member of its family — which is exactly the
    // canonical old tablet.
    std::vector<std::vector<uint32_t>> members_by_root(old_tablet_count);
    for (uint32_t old_tablet_index = 0; old_tablet_index < old_tablet_count; ++old_tablet_index) {
        members_by_root[union_find.find(old_tablet_index)].push_back(old_tablet_index);
    }
    for (uint32_t root_old_tablet_index = 0; root_old_tablet_index < old_tablet_count; ++root_old_tablet_index) {
        auto& members = members_by_root[root_old_tablet_index];
        if (members.size() < 2) {
            // size 0 → not a root (its members were absorbed into a smaller root).
            // size 1 → standalone old tablet with no edges; leave as kNoFamily.
            continue;
        }
        InferredSplitFamilies::Family family;
        family.member_old_tablet_indexes = std::move(members);
        // member_old_tablet_indexes is already in ascending order because we
        // pushed old tablets in iteration order (old_tablet_index 0..N-1).
        family.canonical_old_tablet_index = family.member_old_tablet_indexes.front();
        const auto family_id = static_cast<uint32_t>(result.families.size());
        for (const auto member : family.member_old_tablet_indexes) {
            result.old_tablet_to_family[member] = family_id;
        }
        result.families.push_back(std::move(family));
    }
    return result;
}

} // namespace starrocks::lake::detail

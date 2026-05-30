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

#include <fmt/format.h>

#include <algorithm>
#include <optional>
#include <unordered_map>
#include <utility>

#include "base/hash/hash_util.hpp"
#include "common/logging.h"
#include "common/status.h"
#include "storage/lake/meta_file.h"

namespace starrocks::lake::detail {

namespace {

// Compute a rowset's lifted rssid for a given segment position WITHOUT
// risking uint32_t wraparound that get_rssid()'s native return type would
// silently allow. Returns std::nullopt when rowset.id() + segment_idx
// would overflow uint32; the caller must mark the affected family unsafe
// in that case.
std::optional<uint32_t> safe_lifted_segment_rssid(const RowsetMetadataPB& rowset_meta, int32_t segment_pos) {
    const uint64_t lifted =
            static_cast<uint64_t>(rowset_meta.id()) + static_cast<uint64_t>(get_segment_idx(rowset_meta, segment_pos));
    if (lifted > std::numeric_limits<uint32_t>::max()) {
        return std::nullopt;
    }
    return static_cast<uint32_t>(lifted);
}

template <typename T>
inline void fold_into_hash(size_t& hash, const T& value) {
    HashUtil::hash_combine(hash, value);
}

// Standard union-find with the smallest member always rising to the root.
// This makes find(x) deterministically return the minimum child_index in
// x's family, which is exactly what canonical_child_index needs.
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
        // (smallest) child stays at the family's root after every union.
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

size_t RowsetPhysicalKeyHash::operator()(const RowsetPhysicalKey& key) const noexcept {
    size_t hash = std::hash<int64_t>{}(key.version);
    for (const auto& segment : key.segments) {
        fold_into_hash(hash, segment);
    }
    for (const auto offset : key.bundle_file_offsets) {
        fold_into_hash(hash, offset);
    }
    for (const auto flag : key.shared_segments_flags) {
        // std::hash<bool> exists but is trivial; cast to int8 first so
        // false / true don't collide with adjacent integer fields.
        fold_into_hash(hash, static_cast<int8_t>(flag));
    }
    for (const auto idx : key.segment_idx_layout) {
        fold_into_hash(hash, idx);
    }
    return hash;
}

RowsetPhysicalKey make_rowset_physical_key(const RowsetMetadataPB& rowset_meta) {
    RowsetPhysicalKey key;
    key.version = rowset_meta.version();
    key.segments.assign(rowset_meta.segments().begin(), rowset_meta.segments().end());
    key.shared_segments_flags.assign(rowset_meta.shared_segments().begin(), rowset_meta.shared_segments().end());

    // Normalize bundle_file_offsets and segment_idx_layout to ALWAYS have one
    // entry per segment, falling back to documented defaults when the PB
    // omits them. Without this, two physically-equivalent rowsets where one
    // copy has the field populated (e.g., `bundle_file_offsets = [0, 0]`)
    // and the other has it absent (`bundle_file_offsets = []`) would hash
    // and compare unequal — a false-negative family inference that loses the
    // canonical projection. PB defaults match production read semantics:
    // bundle_file_offsets default to 0 per segment, and segment_idx falls
    // back to positional index per get_segment_idx() in meta_file.cpp.
    const int segments_count = rowset_meta.segments_size();
    key.bundle_file_offsets.reserve(segments_count);
    key.segment_idx_layout.reserve(segments_count);
    for (int segment_pos = 0; segment_pos < segments_count; ++segment_pos) {
        const int64_t bundle_offset =
                segment_pos < rowset_meta.bundle_file_offsets_size() ? rowset_meta.bundle_file_offsets(segment_pos) : 0;
        key.bundle_file_offsets.push_back(bundle_offset);
        key.segment_idx_layout.push_back(static_cast<int32_t>(get_segment_idx(rowset_meta, segment_pos)));
    }
    return key;
}

bool is_shared_ancestor_rowset(const RowsetMetadataPB& rowset_meta) {
    if (rowset_meta.segments_size() == 0) return false;
    if (rowset_meta.shared_segments_size() != rowset_meta.segments_size()) return false;
    return std::all_of(rowset_meta.shared_segments().begin(), rowset_meta.shared_segments().end(),
                       [](bool flag) { return flag; });
}

StatusOr<InferredSplitFamilies> infer_split_families(const std::vector<SplitFamilyInferenceInput>& inputs) {
    InferredSplitFamilies result;
    const auto child_count = static_cast<uint32_t>(inputs.size());
    result.child_to_family.assign(child_count, InferredSplitFamilies::kNoFamily);
    if (child_count == 0) {
        return result;
    }

    UnionFind union_find(child_count);

    // Edge (1): legacy `shared && !has_shared_rssid` sstable filename.
    // Two children that reference the same legacy file must come from the
    // same SPLIT family (the file is bytes-immutable; SPLIT marks shared
    // copies but doesn't rewrite anything).
    std::unordered_map<std::string, uint32_t> filename_to_first_child;
    for (uint32_t child_index = 0; child_index < child_count; ++child_index) {
        const auto& metadata = inputs[child_index].metadata;
        if (metadata == nullptr || !metadata->has_sstable_meta()) continue;
        for (const auto& sstable : metadata->sstable_meta().sstables()) {
            if (!sstable.shared() || sstable.has_shared_rssid()) continue;
            auto [iter, inserted] = filename_to_first_child.emplace(sstable.filename(), child_index);
            if (!inserted) {
                union_find.unite(iter->second, child_index);
            }
        }
    }

    // Edge (2): full physical identity of a shared-ancestor rowset. Both
    // children must satisfy is_shared_ancestor_rowset() — delete-only and
    // child-local rowsets are filtered out so two physically-matching but
    // unrelated rowsets cannot create a false union.
    std::unordered_map<RowsetPhysicalKey, uint32_t, RowsetPhysicalKeyHash> rowset_key_to_first_child;
    for (uint32_t child_index = 0; child_index < child_count; ++child_index) {
        const auto& metadata = inputs[child_index].metadata;
        if (metadata == nullptr) continue;
        for (const auto& rowset : metadata->rowsets()) {
            if (!is_shared_ancestor_rowset(rowset)) continue;
            auto key = make_rowset_physical_key(rowset);
            auto [iter, inserted] = rowset_key_to_first_child.emplace(std::move(key), child_index);
            if (!inserted) {
                union_find.unite(iter->second, child_index);
            }
        }
    }

    // Materialize families. members_by_root[r] ends up holding every child
    // whose union-find root is r. Because UnionFind always promotes the
    // smaller index to the root, members_by_root[r] is non-empty only when
    // r is the smallest member of its family — which is exactly the
    // canonical child.
    std::vector<std::vector<uint32_t>> members_by_root(child_count);
    for (uint32_t child_index = 0; child_index < child_count; ++child_index) {
        members_by_root[union_find.find(child_index)].push_back(child_index);
    }
    for (uint32_t root_child_index = 0; root_child_index < child_count; ++root_child_index) {
        auto& members = members_by_root[root_child_index];
        if (members.size() < 2) {
            // size 0 → not a root (its members were absorbed into a smaller root).
            // size 1 → standalone child with no edges; leave as kNoFamily.
            continue;
        }
        InferredSplitFamilies::Family family;
        family.member_child_indexes = std::move(members);
        // member_child_indexes is already in ascending order because we
        // pushed children in iteration order (child_index 0..N-1).
        family.canonical_child_index = family.member_child_indexes.front();
        family.canonical_rssid_offset = inputs[family.canonical_child_index].rssid_offset;
        const auto family_id = static_cast<uint32_t>(result.families.size());
        for (const auto member : family.member_child_indexes) {
            result.child_to_family[member] = family_id;
        }
        result.families.push_back(std::move(family));
    }
    return result;
}

namespace {

// Lift one occupancy candidate into the plan. On a clash with a prior
// occupant carrying a different physical key, mark both involved families
// unsafe so merge_rowsets + the fast-path fall back to natural-offset
// behavior.
void record_occupancy(uint32_t final_rssid, const RowsetPhysicalKey& key, uint32_t family_id,
                      RssidProjectionPlan& plan) {
    auto [iter, inserted] = plan.occupied_rssids.emplace(final_rssid, RssidProjectionPlan::Occupancy{key, family_id});
    if (inserted) return;
    if (iter->second.key == key) return; // dedup across family members; no-op
    // Two physically-distinct rowsets want the same final rssid. Orphan ctx
    // entries carry kNoFamily and are filtered — orphans use natural offset
    // and don't go through the canonical projection.
    if (family_id != InferredSplitFamilies::kNoFamily) {
        plan.unsafe_families.insert(family_id);
    }
    if (iter->second.family_id != InferredSplitFamilies::kNoFamily) {
        plan.unsafe_families.insert(iter->second.family_id);
    }
}

// Try to project a single source rssid through the plan-building pass.
// Returns false if the projection arithmetic overflows uint32_t (and
// records the family as unsafe), so the outer loop can skip the
// unprojectable entry without poisoning the plan.
bool try_record_projection(uint32_t source_rssid, int64_t offset, const RowsetPhysicalKey& key, uint32_t family_id,
                           RssidProjectionPlan& plan) {
    const int64_t final_rssid = static_cast<int64_t>(source_rssid) + offset;
    if (final_rssid < 0 || final_rssid > std::numeric_limits<uint32_t>::max()) {
        if (family_id != InferredSplitFamilies::kNoFamily) {
            plan.unsafe_families.insert(family_id);
        }
        return false;
    }
    record_occupancy(static_cast<uint32_t>(final_rssid), key, family_id, plan);
    return true;
}

} // namespace

StatusOr<RssidProjectionPlan> build_rssid_projection_plan(const std::vector<SplitFamilyInferenceInput>& inputs,
                                                          const InferredSplitFamilies& families) {
    // The plan must be built from the same (inputs, families) pair: a
    // future caller diverging them would produce out-of-bounds accesses
    // below, so validate in release as well as debug.
    if (families.child_to_family.size() != inputs.size()) {
        return Status::InvalidArgument(
                "child_to_family size mismatches inputs size — InferredSplitFamilies must be built from these inputs");
    }
    for (uint32_t child_index = 0; child_index < families.child_to_family.size(); ++child_index) {
        const uint32_t family_id = families.child_to_family[child_index];
        if (family_id == InferredSplitFamilies::kNoFamily) continue;
        if (family_id >= families.families.size()) {
            return Status::InvalidArgument(fmt::format("child_to_family[{}] = {} but only {} families exist",
                                                       child_index, family_id, families.families.size()));
        }
    }
    for (uint32_t family_id = 0; family_id < families.families.size(); ++family_id) {
        const auto& family = families.families[family_id];
        for (const uint32_t member : family.member_child_indexes) {
            if (member >= inputs.size()) {
                return Status::InvalidArgument(
                        fmt::format("InferredSplitFamilies references child_index {} but only {} inputs were provided",
                                    member, inputs.size()));
            }
            // child_to_family must round-trip: a Family's member must map
            // back to that family_id. Otherwise step 1 (which reads
            // child_to_family) and step 2 (which iterates families) operate
            // on inconsistent assumptions.
            if (families.child_to_family[member] != family_id) {
                return Status::InvalidArgument(
                        fmt::format("InferredSplitFamilies inconsistency: families[{}] lists child {} as member but "
                                    "child_to_family[{}] = {}",
                                    family_id, member, member, families.child_to_family[member]));
            }
        }
    }

    RssidProjectionPlan plan;

    // Step 1: walk every rowset in every ctx, claim its final rssids, and
    // detect cross-family / orphan-vs-family collisions. A rowset's
    // projection offset depends on whether it is a shared-ancestor inside
    // a family (canonical offset) or otherwise (natural offset = the ctx's
    // own rssid_offset, same as v1's add_rowset path).
    for (uint32_t child_index = 0; child_index < inputs.size(); ++child_index) {
        const auto& metadata = inputs[child_index].metadata;
        if (metadata == nullptr) continue;
        const uint32_t family_id = families.child_to_family[child_index];
        DCHECK(family_id == InferredSplitFamilies::kNoFamily || family_id < families.families.size());
        const int64_t natural_offset = inputs[child_index].rssid_offset;
        const int64_t canonical_offset = (family_id != InferredSplitFamilies::kNoFamily)
                                                 ? families.families[family_id].canonical_rssid_offset
                                                 : natural_offset;

        for (const auto& rowset : metadata->rowsets()) {
            const bool use_canonical =
                    family_id != InferredSplitFamilies::kNoFamily && is_shared_ancestor_rowset(rowset);
            const int64_t offset = use_canonical ? canonical_offset : natural_offset;
            const auto key = make_rowset_physical_key(rowset);

            // Always claim rowset.id() — covers add_rowset's first
            // map_rssid call AND rowset-level metadata (delvec keys, DCG
            // keys) that use rowset.id() rather than a segment-position
            // rssid.
            try_record_projection(rowset.id(), offset, key, family_id, plan);

            // Also claim every segment position. safe_lifted_segment_rssid
            // checks for rowset.id() + segment_idx wraparound BEFORE
            // narrowing to uint32_t; on overflow it returns nullopt and
            // we mark the family unsafe (matches the wraparound treatment
            // of the int64 overflow check inside try_record_projection).
            for (int segment_pos = 0; segment_pos < rowset.segments_size(); ++segment_pos) {
                const auto lifted_or = safe_lifted_segment_rssid(rowset, segment_pos);
                if (!lifted_or.has_value()) {
                    if (family_id != InferredSplitFamilies::kNoFamily) {
                        plan.unsafe_families.insert(family_id);
                    }
                    continue;
                }
                try_record_projection(*lifted_or, offset, key, family_id, plan);
            }
        }
    }

    // Step 2: populate explicit_rssid_map and family_legacy_sstable_offset
    // for SAFE families only. For unsafe families the plan stays silent so
    // map_rssid falls through to the natural-offset path automatically.
    for (uint32_t family_id = 0; family_id < families.families.size(); ++family_id) {
        if (plan.unsafe_families.count(family_id) > 0) continue;
        const auto& family = families.families[family_id];
        plan.family_legacy_sstable_offset.emplace(family_id, family.canonical_rssid_offset);

        for (const uint32_t child_index : family.member_child_indexes) {
            const auto& metadata = inputs[child_index].metadata;
            if (metadata == nullptr) continue;
            for (const auto& rowset : metadata->rowsets()) {
                if (!is_shared_ancestor_rowset(rowset)) continue;

                const int64_t rs_id_final = static_cast<int64_t>(rowset.id()) + family.canonical_rssid_offset;
                // Step 1 already would have marked the family unsafe on
                // overflow, but defensively skip the entry here too.
                if (rs_id_final >= 0 && rs_id_final <= std::numeric_limits<uint32_t>::max()) {
                    plan.explicit_rssid_map.emplace(SourceRssidKey{child_index, rowset.id()},
                                                    static_cast<uint32_t>(rs_id_final));
                }
                for (int segment_pos = 0; segment_pos < rowset.segments_size(); ++segment_pos) {
                    const auto lifted_or = safe_lifted_segment_rssid(rowset, segment_pos);
                    if (!lifted_or.has_value()) continue;
                    const int64_t segment_final = static_cast<int64_t>(*lifted_or) + family.canonical_rssid_offset;
                    if (segment_final < 0 || segment_final > std::numeric_limits<uint32_t>::max()) continue;
                    plan.explicit_rssid_map.emplace(SourceRssidKey{child_index, *lifted_or},
                                                    static_cast<uint32_t>(segment_final));
                }
            }
        }
    }

    return plan;
}

} // namespace starrocks::lake::detail

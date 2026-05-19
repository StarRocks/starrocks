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

#include "storage/lake/tablet_merger.h"

#include <bvar/bvar.h>

#include <algorithm>
#include <limits>
#include <map>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "base/hash/crc32c.h"
#include "base/testutil/sync_point.h"
#include "base/uid_util.h"
#include "base/utility/defer_op.h"
#include "column/column_helper.h"
#include "common/config_rowset_fwd.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "gutil/stl_util.h"
#include "storage/chunk_helper.h"
#include "storage/del_vector.h"
#include "storage/delta_column_group.h"
#include "storage/lake/filenames.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/persistent_index_sstable.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_merger_split_family.h"
#include "storage/lake/tablet_range_helper.h"
#include "storage/lake/tablet_reshard_helper.h"
#include "storage/lake/update_manager.h"
#include "storage/lake/utils.h"
#include "storage/olap_common.h"
#include "storage/options.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/sstable/comparator.h"
#include "storage/sstable/iterator.h"
#include "storage/sstable/options.h"
#include "storage/sstable/table_builder.h"
#include "storage/tablet_schema.h"

namespace {

bvar::Adder<int64_t> g_tablet_merge_dcg_rebuild_total("tablet_merge_dcg_rebuild_total");
bvar::Adder<int64_t> g_tablet_merge_dcg_rebuild_fallback_not_supported_total(
        "tablet_merge_dcg_rebuild_fallback_not_supported_total");

bvar::Adder<int64_t> g_tablet_merge_gap_delvec_total("tablet_merge_gap_delvec_total");
bvar::Adder<int64_t> g_tablet_merge_non_pk_skip_dedup_total("tablet_merge_non_pk_skip_dedup_total");
bvar::Adder<int64_t> g_tablet_merge_synthesized_only_delvec_total("tablet_merge_synthesized_only_delvec_total");

bvar::Adder<int64_t> g_tablet_merge_legacy_sstable_rebuild_total("tablet_merge_legacy_sstable_rebuild_total");
bvar::Adder<int64_t> g_tablet_merge_legacy_sstable_rebuild_dropped_entries(
        "tablet_merge_legacy_sstable_rebuild_dropped_entries");
bvar::Adder<int64_t> g_tablet_merge_legacy_sstable_fastpath_total("tablet_merge_legacy_sstable_fastpath_total");
bvar::Adder<int64_t> g_tablet_merge_legacy_sstable_fastpath_fallback_to_rebuild_total(
        "tablet_merge_legacy_sstable_fastpath_fallback_to_rebuild_total");
bvar::Adder<int64_t> g_tablet_merge_legacy_sstable_fastpath_fallback_source_offset_nonzero(
        "tablet_merge_legacy_sstable_fastpath_fallback_source_offset_nonzero");
bvar::Adder<int64_t> g_tablet_merge_legacy_sstable_fastpath_fallback_canonical_offset_nonzero(
        "tablet_merge_legacy_sstable_fastpath_fallback_canonical_offset_nonzero");
bvar::Adder<int64_t> g_tablet_merge_legacy_sstable_fastpath_fallback_invalid_form(
        "tablet_merge_legacy_sstable_fastpath_fallback_invalid_form");
bvar::Adder<int64_t> g_tablet_merge_legacy_sstable_fastpath_fallback_merged_no_range(
        "tablet_merge_legacy_sstable_fastpath_fallback_merged_no_range");
bvar::Adder<int64_t> g_tablet_merge_legacy_sstable_fastpath_fallback_ranged_no_fileset_id(
        "tablet_merge_legacy_sstable_fastpath_fallback_ranged_no_fileset_id");
bvar::Adder<int64_t> g_tablet_merge_legacy_sstable_fastpath_fallback_partial_compaction(
        "tablet_merge_legacy_sstable_fastpath_fallback_partial_compaction");
bvar::Adder<int64_t> g_tablet_merge_legacy_sstable_fastpath_fallback_merged_delvec_nonempty(
        "tablet_merge_legacy_sstable_fastpath_fallback_merged_delvec_nonempty");
bvar::Adder<int64_t> g_tablet_merge_legacy_sstable_fastpath_fallback_coverage_span_invalid(
        "tablet_merge_legacy_sstable_fastpath_fallback_coverage_span_invalid");
bvar::Adder<int64_t> g_tablet_merge_legacy_sstable_fastpath_fallback_no_family(
        "tablet_merge_legacy_sstable_fastpath_fallback_no_family");
bvar::Adder<int64_t> g_tablet_merge_legacy_sstable_fastpath_fallback_unsafe_family(
        "tablet_merge_legacy_sstable_fastpath_fallback_unsafe_family");
bvar::Adder<int64_t> g_tablet_merge_non_shared_sstable_rebuild_total("tablet_merge_non_shared_sstable_rebuild_total");

} // namespace

namespace starrocks::lake {

namespace {

class TabletMergeContext {
public:
    explicit TabletMergeContext(TabletMetadataPtr metadata) : _metadata(std::move(metadata)) {}

    const TabletMetadataPtr& metadata() const { return _metadata; }
    // Reseat the backing metadata pointer. Used by flush_persistent_index to
    // substitute a spliced snapshot (same rowsets, sstable_meta updated to
    // include freshly-flushed PK-index sstables).
    void set_metadata(TabletMetadataPtr metadata) { _metadata = std::move(metadata); }

    int64_t rssid_offset() const { return _rssid_offset; }
    void set_rssid_offset(int64_t offset) { _rssid_offset = offset; }

    uint32_t child_index() const { return _child_index; }
    void set_child_index(uint32_t child_index) { _child_index = child_index; }

    // The plan must outlive this ctx (typically a stack local in
    // merge_tablet). Null on non-PK merges; map_rssid then degenerates to
    // the shared_rssid_map → natural offset path.
    void set_projection_plan(const detail::RssidProjectionPlan* plan) { _projection_plan = plan; }
    const detail::RssidProjectionPlan* projection_plan() const { return _projection_plan; }

    // Maps an original rssid to the final output rssid. Lookup priority:
    //   (1) RssidProjectionPlan.explicit_rssid_map keyed by (child_index,
    //       rssid). Hit only for shared-ancestor rowsets in safe families
    //       (PK + cloud-native gate at plan-build time).
    //   (2) shared_rssid_map for runtime-deduped child-local mappings.
    //   (3) natural offset (rssid + ctx.rssid_offset).
    StatusOr<uint32_t> map_rssid(uint32_t rssid) const {
        if (_projection_plan != nullptr) {
            auto plan_it = _projection_plan->explicit_rssid_map.find({_child_index, rssid});
            if (plan_it != _projection_plan->explicit_rssid_map.end()) {
                return plan_it->second;
            }
        }
        auto it = _shared_rssid_map.find(rssid);
        if (it != _shared_rssid_map.end()) {
            return it->second;
        }
        int64_t mapped = static_cast<int64_t>(rssid) + _rssid_offset;
        if (mapped < 0 || mapped > static_cast<int64_t>(std::numeric_limits<uint32_t>::max())) {
            return Status::InvalidArgument("Segment id overflow during tablet merge");
        }
        return static_cast<uint32_t>(mapped);
    }

    // Precomputes the sorted set of source-effective rssids whose
    // map_rssid would return a value DIFFERENT from
    // rssid + this->_rssid_offset under the plan/shared_rssid priority.
    // merge_sstables calls this once per ctx, then uses
    // mapping_disagrees_with_natural_in_range below to range-test each
    // non-shared sstable in O(log N) instead of re-scanning the plan +
    // shared_rssid_map per sstable.
    //
    // Conservative: arithmetic overflow on natural-offset projection is
    // treated as a disagreement so the rebuild path runs and rejects
    // out-of-range entries explicitly. False-positives only cost an
    // extra rebuild; false-negatives would silently mis-map PK lookups.
    std::vector<uint32_t> compute_disagreement_keys() const {
        auto disagrees = [&](uint32_t key, uint32_t value) -> bool {
            const int64_t natural = static_cast<int64_t>(key) + _rssid_offset;
            if (natural < 0 || natural > static_cast<int64_t>(std::numeric_limits<uint32_t>::max())) {
                return true; // arithmetic overflow → force rebuild
            }
            return value != static_cast<uint32_t>(natural);
        };

        const size_t plan_size = (_projection_plan != nullptr) ? _projection_plan->explicit_rssid_map.size() : 0;
        std::vector<uint32_t> keys;
        keys.reserve(plan_size + _shared_rssid_map.size());

        if (_projection_plan != nullptr) {
            for (const auto& [source_key, final_rssid] : _projection_plan->explicit_rssid_map) {
                if (source_key.child_index != _child_index) continue;
                if (disagrees(source_key.source_rssid, final_rssid)) {
                    keys.push_back(source_key.source_rssid);
                }
            }
        }
        for (const auto& [source_rssid, final_rssid] : _shared_rssid_map) {
            if (disagrees(source_rssid, final_rssid)) {
                keys.push_back(source_rssid);
            }
        }
        STLSortAndRemoveDuplicates(&keys);
        return keys;
    }

    // True iff the precomputed sorted disagreement-keys vector contains
    // any key in [lifted_lower, lifted_upper] (inclusive). O(log N) via
    // binary search; the caller is expected to compute the keys vector
    // once per ctx outside the inner sstable loop.
    static bool mapping_disagrees_with_natural_in_range(const std::vector<uint32_t>& sorted_disagreement_keys,
                                                        uint32_t lifted_lower, uint32_t lifted_upper) {
        auto it = std::lower_bound(sorted_disagreement_keys.begin(), sorted_disagreement_keys.end(), lifted_lower);
        return it != sorted_disagreement_keys.end() && *it <= lifted_upper;
    }

    bool has_shared_rssid_mapping() const { return !_shared_rssid_map.empty(); }

    // Fills shared_rssid_map so that all rssids occupied by |rowset| map to
    // the corresponding rssid in |canonical_rowset|.
    void update_shared_rssid_map(const RowsetMetadataPB& rowset, const RowsetMetadataPB& canonical_rowset) {
        uint32_t canonical_rssid = canonical_rowset.id();
        _shared_rssid_map[rowset.id()] = canonical_rssid;
        for (int segment_pos = 0; segment_pos < rowset.segments_size(); ++segment_pos) {
            uint32_t rssid = get_rssid(rowset, segment_pos);
            uint32_t segment_offset_within_rowset = rssid - rowset.id();
            _shared_rssid_map[rssid] = canonical_rssid + segment_offset_within_rowset;
        }
    }

    bool has_next_rowset() const { return _current_rowset_index < _metadata->rowsets_size(); }
    const RowsetMetadataPB& current_rowset() const { return _metadata->rowsets(_current_rowset_index); }
    void advance_rowset() { ++_current_rowset_index; }

private:
    TabletMetadataPtr _metadata;
    int64_t _rssid_offset = 0;
    int _current_rowset_index = 0;
    uint32_t _child_index = 0;
    // Non-owning. Lifetime guaranteed by merge_tablet's stack frame.
    const detail::RssidProjectionPlan* _projection_plan = nullptr;
    // Dedup-produced rssid remap table.
    // key: original rssid in this child metadata
    // value: canonical rowset's actual rssid in the final output
    // rssid not in this map uses the default offset mapping.
    std::unordered_map<uint32_t, uint32_t> _shared_rssid_map;
};

// PersistentIndexSstablePB::max_rss_rowid encodes (rssid << 32) | rowid.
// Helpers below name that encoding so callers don't open-code the shift
// or mask. The "rss_rowid" naming matches the PB field; "high" / "low"
// disambiguate which 32-bit half is being referenced.
constexpr int kRssRowidHighShift = 32;
constexpr uint64_t kRssRowidLowMask = 0xFFFFFFFFULL;

inline uint32_t extract_rss_rowid_high(uint64_t encoded) {
    return static_cast<uint32_t>(encoded >> kRssRowidHighShift);
}
inline uint64_t extract_rss_rowid_low(uint64_t encoded) {
    return encoded & kRssRowidLowMask;
}
inline uint64_t encode_rss_rowid(uint32_t rssid_high, uint64_t rowid_low) {
    return (static_cast<uint64_t>(rssid_high) << kRssRowidHighShift) | rowid_low;
}

// Tracks the contributing children's child-local ranges per canonical rowset
// in new_metadata. Used by the post-merge_rowsets PK coverage check and by
// gap-delvec synthesis.
//
// Key: index of the canonical rowset in new_metadata->rowsets().
// Value: each entry is the contributing child's effective_child_local_range
// (rowset.range, fallback ctx.metadata.range, fallback unbounded), captured
// BEFORE update_canonical's union_range mutates the canonical's stored range
// — otherwise the convex hull would swallow gaps and the coverage / gap
// detection would fail.
using CanonicalContribMap = std::unordered_map<int, std::vector<TabletRangePB>>;

struct DelvecSourceRef {
    const TabletMergeContext* ctx;
    DelvecPagePB page;
    std::string file_name;
};

struct UnionPageInfo {
    uint64_t offset;
    uint64_t size;
    uint32_t masked_crc32c;
};

struct TargetDelvecState {
    std::optional<DelvecSourceRef> single_source;
    std::unique_ptr<DelVector> merged;
    std::map<std::pair<std::string, uint64_t>, uint64_t> seen_sources;
};

// Union |source| into |target|. If |target| is empty, it becomes a copy of |source|.
void union_delvec(DelVector* target, DelVector& source, int64_t version) {
    Roaring merged_bitmap;
    if (target->roaring()) {
        merged_bitmap = *target->roaring();
    }
    if (source.roaring()) {
        merged_bitmap |= *source.roaring();
    }
    std::vector<uint32_t> all_dels;
    for (auto it = merged_bitmap.begin(); it != merged_bitmap.end(); ++it) {
        all_dels.push_back(*it);
    }
    target->init(version, all_dels.data(), all_dels.size());
}

int64_t compute_rssid_offset(const TabletMetadataPB& base_metadata, const TabletMetadataPB& append_metadata) {
    uint32_t min_id = std::numeric_limits<uint32_t>::max();
    for (const auto& rowset : append_metadata.rowsets()) {
        min_id = std::min(min_id, rowset.id());
    }
    if (min_id == std::numeric_limits<uint32_t>::max()) {
        return 0;
    }
    return static_cast<int64_t>(base_metadata.next_rowset_id()) - min_id;
}

bool is_duplicate_rowset(const RowsetMetadataPB& a, const RowsetMetadataPB& b) {
    // Two predicates at the same version -> duplicate
    if (a.has_delete_predicate() && b.has_delete_predicate()) {
        return true;
    }
    // Has segments: compare first segment's file_name, bundle_file_offset, shared
    if (a.segments_size() > 0 && b.segments_size() > 0) {
        if (a.segments(0) != b.segments(0)) return false;
        int64_t a_off = a.bundle_file_offsets_size() > 0 ? a.bundle_file_offsets(0) : 0;
        int64_t b_off = b.bundle_file_offsets_size() > 0 ? b.bundle_file_offsets(0) : 0;
        if (a_off != b_off) return false;
        bool a_shared = a.shared_segments_size() > 0 && a.shared_segments(0);
        bool b_shared = b.shared_segments_size() > 0 && b.shared_segments(0);
        return a_shared && b_shared;
    } else if (a.del_files_size() > 0 && b.del_files_size() > 0) {
        // Delete-only: compare first del_file's name, shared
        return a.del_files(0).name() == b.del_files(0).name() && a.del_files(0).shared() && b.del_files(0).shared();
    }
    return false;
}

Status add_rowset(TabletMergeContext& ctx, const RowsetMetadataPB& rowset, TabletMetadataPB* new_metadata) {
    auto* new_rowset = new_metadata->add_rowsets();
    new_rowset->CopyFrom(rowset);
    // rssid mapping
    ASSIGN_OR_RETURN(auto new_id, ctx.map_rssid(rowset.id()));
    new_rowset->set_id(new_id);
    if (rowset.has_max_compact_input_rowset_id()) {
        ASSIGN_OR_RETURN(auto new_max_compact, ctx.map_rssid(rowset.max_compact_input_rowset_id()));
        new_rowset->set_max_compact_input_rowset_id(new_max_compact);
    }
    for (auto& del : *new_rowset->mutable_del_files()) {
        ASSIGN_OR_RETURN(auto new_origin, ctx.map_rssid(del.origin_rowset_id()));
        del.set_origin_rowset_id(new_origin);
    }
    // range update
    RETURN_IF_ERROR(tablet_reshard_helper::update_rowset_range(new_rowset, ctx.metadata()->range()));
    // schema mapping
    const auto& rowset_to_schema = ctx.metadata()->rowset_to_schema();
    auto schema_it = rowset_to_schema.find(rowset.id());
    if (schema_it != rowset_to_schema.end()) {
        (*new_metadata->mutable_rowset_to_schema())[new_rowset->id()] = schema_it->second;
    }
    return Status::OK();
}

Status update_canonical(RowsetMetadataPB* canonical_rowset, const TabletRangePB& duplicate_effective_range,
                        const RowsetMetadataPB& duplicate_rowset) {
    // Always extend the canonical range with the duplicate's *effective* range
    // (rowset.range, fallback ctx.metadata.range, fallback unbounded — same chain
    // as Rowset::get_seek_range()). The previous gate of
    // `canonical_rowset->has_range() && duplicate_rowset.has_range()` silently
    // dropped contributors whose rowset.range was unset but whose ctx tablet
    // range filled the slice; the post-dedup canonical.range would then reflect
    // only the first contributor and readers (which prefer rowset.range over
    // tablet.range) would miss rows from later contributors.
    if (canonical_rowset->has_range()) {
        ASSIGN_OR_RETURN(auto merged_range,
                         tablet_reshard_helper::union_range(canonical_rowset->range(), duplicate_effective_range));
        canonical_rowset->mutable_range()->CopyFrom(merged_range);
    } else {
        canonical_rowset->mutable_range()->CopyFrom(duplicate_effective_range);
    }
    // Each merge input carries a proportional num_dels slice written by
    // tablet_splitter.cpp / tablet_reshard_helper.cpp with num_dels <= num_rows.
    // Summation therefore preserves that invariant on the canonical rowset, so no
    // clamp is needed. Tablet merge is greenfield; there is no legacy state with
    // the parent's full num_dels inherited by every child to guard against.
    DCHECK_LE(canonical_rowset->num_dels(), canonical_rowset->num_rows());
    DCHECK_LE(duplicate_rowset.num_dels(), duplicate_rowset.num_rows());
    canonical_rowset->set_num_rows(canonical_rowset->num_rows() + duplicate_rowset.num_rows());
    canonical_rowset->set_data_size(canonical_rowset->data_size() + duplicate_rowset.data_size());
    canonical_rowset->set_num_dels(canonical_rowset->num_dels() + duplicate_rowset.num_dels());
    return Status::OK();
}

Status merge_rowsets(std::vector<TabletMergeContext>& merge_contexts, TabletMetadataPB* new_metadata,
                     CanonicalContribMap* canonical_contribs) {
    const bool is_pk = is_primary_key(*new_metadata);
    int version_start_index = 0;
    int64_t current_version = -1;

    for (;;) {
        // Find child with minimum (version, child_index).
        // Forward iteration with strict < ensures the smallest child_index wins on ties.
        int min_child_index = -1;
        int64_t min_version = std::numeric_limits<int64_t>::max();
        for (int i = 0; i < static_cast<int>(merge_contexts.size()); ++i) {
            if (!merge_contexts[i].has_next_rowset()) continue;
            int64_t version = merge_contexts[i].current_rowset().version();
            if (version < min_version) {
                min_version = version;
                min_child_index = i;
            }
        }
        if (min_child_index < 0) break;

        const auto& rowset = merge_contexts[min_child_index].current_rowset();
        const auto& ctx_meta = *merge_contexts[min_child_index].metadata();

        // Version change: update version_start_index
        if (rowset.version() != current_version) {
            current_version = rowset.version();
            version_start_index = new_metadata->rowsets_size();
        }

        // Search [version_start_index, end) for a dedup candidate. Decision:
        //   - Delete-predicate dups: keep original unconditional skip path.
        //   - Shared-segment / shared-del_file dups (the only other case
        //     is_duplicate_rowset returns true on): PK always dedups; non-PK
        //     dedups only when ranges are contiguous so that the convex-hull
        //     range stored on the canonical does not span a gap left by a
        //     compacted sibling.
        int canonical_index = -1;
        bool non_pk_skip_dedup_fired = false;
        for (int i = version_start_index; i < new_metadata->rowsets_size(); ++i) {
            if (!is_duplicate_rowset(rowset, new_metadata->rowsets(i))) continue;

            if (rowset.has_delete_predicate()) {
                canonical_index = i;
                break;
            }
            if (is_pk) {
                canonical_index = i;
                break;
            }
            const auto& candidate_range = new_metadata->rowsets(i).range();
            const auto& incoming_range = tablet_reshard_helper::effective_child_local_range(rowset, ctx_meta);
            if (tablet_reshard_helper::ranges_are_contiguous(candidate_range, incoming_range)) {
                canonical_index = i;
                break;
            }
            // Non-PK + non-contiguous: keep scanning. If no other candidate
            // matches, fall through to add_rowset and treat as a separate
            // shared rowset — each retains its own contiguous range.
            non_pk_skip_dedup_fired = true;
        }
        if (canonical_index < 0 && non_pk_skip_dedup_fired) {
            // At least one is_duplicate_rowset hit was rejected due to
            // non-contiguous ranges and no later candidate accepted it →
            // the rowset is added as a sibling of an existing shared rowset.
            g_tablet_merge_non_pk_skip_dedup_total << 1;
        }

        if (canonical_index >= 0) {
            // Duplicate: skip output
            const auto& canonical = new_metadata->rowsets(canonical_index);
            DCHECK(rowset.segments_size() == canonical.segments_size() &&
                   rowset.del_files_size() == canonical.del_files_size())
                    << "Shared rowset dedup hit but payload shape differs: segments(" << rowset.segments_size()
                    << " vs " << canonical.segments_size() << "), del_files(" << rowset.del_files_size() << " vs "
                    << canonical.del_files_size() << ")";
            if (!rowset.has_delete_predicate()) {
                merge_contexts[min_child_index].update_shared_rssid_map(rowset, canonical);
                // Capture the duplicate's child-local effective range BEFORE
                // update_canonical mutates canonical.range via union_range. The
                // same effective range is also passed into update_canonical so
                // that a rowset without its own .range() but with a ctx tablet
                // range still extends the canonical range.
                const auto& duplicate_effective_range =
                        tablet_reshard_helper::effective_child_local_range(rowset, ctx_meta);
                if (canonical_contribs != nullptr) {
                    (*canonical_contribs)[canonical_index].push_back(duplicate_effective_range);
                }
                RETURN_IF_ERROR(update_canonical(new_metadata->mutable_rowsets(canonical_index),
                                                 duplicate_effective_range, rowset));
            }
            // predicate: just skip
        } else {
            // First occurrence: output. Capture child-local range first so that
            // we record the pre-clip view (add_rowset clips to ctx tablet range).
            TabletRangePB initial_child_range;
            if (canonical_contribs != nullptr && !rowset.has_delete_predicate()) {
                initial_child_range = tablet_reshard_helper::effective_child_local_range(rowset, ctx_meta);
            }
            const int new_index = new_metadata->rowsets_size();
            RETURN_IF_ERROR(add_rowset(merge_contexts[min_child_index], rowset, new_metadata));
            if (canonical_contribs != nullptr && !rowset.has_delete_predicate()) {
                (*canonical_contribs)[new_index].push_back(std::move(initial_child_range));
            }
        }

        merge_contexts[min_child_index].advance_rowset();
    }

    return Status::OK();
}

Status validate_dcg_shape(const DeltaColumnGroupVerPB& dcg) {
    // Required fields must be equal length
    if (dcg.unique_column_ids_size() != dcg.column_files_size() || dcg.versions_size() != dcg.column_files_size()) {
        return Status::Corruption("DCG shape invalid: column_files/unique_column_ids/versions size mismatch");
    }
    // Optional fields must not exceed column_files length
    if (dcg.encryption_metas_size() > dcg.column_files_size() || dcg.shared_files_size() > dcg.column_files_size()) {
        return Status::Corruption("DCG shape invalid: optional fields exceed column_files size");
    }
    // No duplicate column UIDs across entries
    std::unordered_set<uint32_t> all_cids;
    for (int i = 0; i < dcg.unique_column_ids_size(); ++i) {
        for (auto cid : dcg.unique_column_ids(i).column_ids()) {
            if (!all_cids.insert(cid).second) {
                return Status::Corruption("DCG contains duplicate column UID across entries");
            }
        }
    }
    return Status::OK();
}

void normalize_dcg_optional_fields(DeltaColumnGroupVerPB* dcg) {
    while (dcg->encryption_metas_size() < dcg->column_files_size()) {
        dcg->add_encryption_metas("");
    }
    while (dcg->shared_files_size() < dcg->column_files_size()) {
        dcg->add_shared_files(false);
    }
}

Status verify_dcg_entry_consistency(const DeltaColumnGroupVerPB& existing, int j, const DeltaColumnGroupVerPB& incoming,
                                    int i) {
    // unique_column_ids
    const auto& e_ids = existing.unique_column_ids(j);
    const auto& i_ids = incoming.unique_column_ids(i);
    if (e_ids.column_ids_size() != i_ids.column_ids_size()) {
        return Status::Corruption("DCG same column_file but unique_column_ids differ");
    }
    for (int k = 0; k < e_ids.column_ids_size(); ++k) {
        if (e_ids.column_ids(k) != i_ids.column_ids(k)) {
            return Status::Corruption("DCG same column_file but unique_column_ids differ");
        }
    }
    // versions
    if (existing.versions(j) != incoming.versions(i)) {
        return Status::Corruption("DCG same column_file but versions differ");
    }
    // encryption_metas (normalized)
    if (existing.encryption_metas(j) != incoming.encryption_metas(i)) {
        return Status::Corruption("DCG same column_file but encryption_metas differ");
    }
    // shared_files (normalized)
    if (existing.shared_files(j) != incoming.shared_files(i)) {
        return Status::Corruption("DCG same column_file but shared_files differ");
    }
    return Status::OK();
}

// ---------------------------------------------------------------------------
// merge_dcg_meta: two-pass entry-level merge with per-target .cols rebuild.
//
// Pass 1 collects per-target "surviving" entries (after exact-dedup by .cols
// filename) and per-target source-rowset references (the child rowsets that
// reference target rssid T through get_rssid/map_rssid). Ranges are captured
// from each source-child rowset BEFORE merge_rowsets() has widened shared
// ranges via union_range, so a coverage gap between child ranges cannot be
// masked by the merged rowset's convex hull.
//
// Pass 2 classifies each target's entries: columns claimed by only one entry
// are non-conflicting and pass through unchanged; columns claimed by >= 2
// entries trigger rebuild. Rebuild folds ALL columns of every conflicting
// entry into a single new .cols file so the reader's first-entry-wins rule
// never leaks stale values from a leftover entry that shares any column with
// the rebuilt set.
//
// Per-target rebuild (rebuild_dcg_for_target_segment) implements Steps A-F of the
// design: locate base segment via get_rssid scan, resolve merged schema,
// compute row windows per source-child rowset using the existing range ->
// SeekRange -> rowid-range pipeline, validate coverage, assemble rebuilt
// chunk (donor file per column + updater-child window overrides), write a
// new .cols file, install one entry into new_dcgs[T].

struct DcgSurvivingEntry {
    size_t child_index;
    uint32_t original_segment_id;
    int entry_index;
    // Single-entry normalized copy of the source DCG entry (all 5 fields at
    // index 0 of the resulting PB). Keeping entries in single-entry form keeps
    // bookkeeping and downstream emission uniform.
    DeltaColumnGroupVerPB single_entry;
};

// True iff the entry's column_ids list contains |unique_id|. The
// underlying PB shape is `entry.single_entry.unique_column_ids(0)
// .column_ids()` (a single-entry-normalized DCG keeps all column ids
// at slot 0); this helper hides the indirection.
inline bool entry_claims_column_uid(const DcgSurvivingEntry& entry, uint32_t unique_id) {
    for (auto claimed : entry.single_entry.unique_column_ids(0).column_ids()) {
        if (claimed == unique_id) return true;
    }
    return false;
}

// Mark every DCG entry that shares any column UID with another entry
// in |entries|. Two entries that both claim the same UID conflict —
// merge_dcg_meta routes those through the rebuild path; non-conflicting
// entries can be emitted as-is.
inline std::vector<bool> mark_conflicting_dcg_entries(const std::vector<DcgSurvivingEntry>& entries) {
    std::unordered_map<uint32_t, std::vector<size_t>> entry_indices_by_unique_id;
    for (size_t entry_index = 0; entry_index < entries.size(); ++entry_index) {
        for (auto unique_id : entries[entry_index].single_entry.unique_column_ids(0).column_ids()) {
            entry_indices_by_unique_id[unique_id].push_back(entry_index);
        }
    }
    std::vector<bool> entry_is_conflicting(entries.size(), false);
    for (const auto& [unique_id, entry_indices] : entry_indices_by_unique_id) {
        if (entry_indices.size() > 1) {
            for (size_t entry_index : entry_indices) entry_is_conflicting[entry_index] = true;
        }
    }
    return entry_is_conflicting;
}

struct DcgSourceRowsetReference {
    size_t child_index;
    const RowsetMetadataPB* rowset = nullptr;
    int segment_position = 0;
    const TabletRangePB* effective_range = nullptr; // rowset.range() else ctx tablet range; null = unbounded
};

struct DcgTargetWorkItem {
    std::vector<DcgSurvivingEntry> entries;
    std::vector<DcgSourceRowsetReference> source_refs;
};

DeltaColumnGroupVerPB make_single_entry_dcg(const DeltaColumnGroupVerPB& source, int entry_index) {
    DeltaColumnGroupVerPB out;
    out.add_column_files(source.column_files(entry_index));
    out.add_unique_column_ids()->CopyFrom(source.unique_column_ids(entry_index));
    out.add_versions(source.versions(entry_index));
    out.add_encryption_metas(source.encryption_metas(entry_index));
    out.add_shared_files(source.shared_files(entry_index));
    return out;
}

// Pass 1 — walk each child's dcg_meta and rowsets, dedup by filename across
// children, and accumulate source-rowset refs per target T.
Status dcg_pass1_collect_entries_and_sources(const std::vector<TabletMergeContext>& merge_contexts,
                                             std::map<uint32_t, DcgTargetWorkItem>* work_by_target) {
    // Track which .cols filenames we have already observed per target so that
    // subsequent children with the same filename are deduped (and verified).
    // Store size_t indexes into DcgTargetWorkItem::entries (NOT raw pointers),
    // since push_back can reallocate the vector and invalidate pointers.
    std::map<uint32_t, std::unordered_map<std::string, size_t>> seen_files_by_target;

    for (size_t child_index = 0; child_index < merge_contexts.size(); ++child_index) {
        const auto& context = merge_contexts[child_index];

        // (a) Accumulate source-rowset references from every rowset that
        // references any target via get_rssid -> context.map_rssid.
        for (const auto& rowset : context.metadata()->rowsets()) {
            for (int segment_position = 0; segment_position < rowset.segments_size(); ++segment_position) {
                uint32_t original_rssid = get_rssid(rowset, segment_position);
                auto target_or = context.map_rssid(original_rssid);
                if (!target_or.ok()) continue;
                uint32_t target_rssid = *target_or;
                DcgSourceRowsetReference source_reference;
                source_reference.child_index = child_index;
                source_reference.rowset = &rowset;
                source_reference.segment_position = segment_position;
                if (rowset.has_range()) {
                    source_reference.effective_range = &rowset.range();
                } else if (context.metadata()->has_range()) {
                    source_reference.effective_range = &context.metadata()->range();
                } else {
                    source_reference.effective_range = nullptr; // unbounded: full segment
                }
                (*work_by_target)[target_rssid].source_refs.push_back(std::move(source_reference));
            }
        }

        // (b) Walk dcg_meta: validate, normalize, split into single-entry
        // records, dedup by .cols filename.
        if (!context.metadata()->has_dcg_meta()) continue;
        for (const auto& [segment_id, dcg_value] : context.metadata()->dcg_meta().dcgs()) {
            ASSIGN_OR_RETURN(uint32_t target_rssid, context.map_rssid(segment_id));

            DeltaColumnGroupVerPB normalized = dcg_value;
            RETURN_IF_ERROR(validate_dcg_shape(normalized));
            normalize_dcg_optional_fields(&normalized);

            for (int entry_index = 0; entry_index < normalized.column_files_size(); ++entry_index) {
                const std::string& file_name = normalized.column_files(entry_index);

                auto& target_work = (*work_by_target)[target_rssid];
                auto& seen_files = seen_files_by_target[target_rssid];
                auto seen_iter = seen_files.find(file_name);
                if (seen_iter != seen_files.end()) {
                    // Exact dedup across children: verify entry-level consistency
                    // against the previously stored entry. Index lookup is safe
                    // even if the vector reallocated between insertions.
                    RETURN_IF_ERROR(verify_dcg_entry_consistency(target_work.entries[seen_iter->second].single_entry, 0,
                                                                 normalized, entry_index));
                    continue;
                }

                DcgSurvivingEntry entry;
                entry.child_index = child_index;
                entry.original_segment_id = segment_id;
                entry.entry_index = entry_index;
                entry.single_entry = make_single_entry_dcg(normalized, entry_index);

                const size_t new_entry_index = target_work.entries.size();
                target_work.entries.push_back(std::move(entry));
                seen_files[file_name] = new_entry_index;
            }
        }
    }
    return Status::OK();
}

// For a merged rowset, find the segment position such that
// get_rssid(rowset, position) == target. Returns -1 if not found.
int find_segment_position_in_rowset(const RowsetMetadataPB& rowset, uint32_t target_rssid) {
    for (int segment_position = 0; segment_position < rowset.segments_size(); ++segment_position) {
        if (get_rssid(rowset, segment_position) == target_rssid) {
            return segment_position;
        }
    }
    return -1;
}

// Step A — locate the merged rowset + segment position that owns the target rssid.
StatusOr<std::pair<const RowsetMetadataPB*, int>> locate_target_in_merged_metadata(const TabletMetadataPB& new_metadata,
                                                                                   uint32_t target_rssid) {
    for (const auto& rowset : new_metadata.rowsets()) {
        int segment_position = find_segment_position_in_rowset(rowset, target_rssid);
        if (segment_position >= 0) return std::make_pair(&rowset, segment_position);
    }
    return Status::InternalError(
            fmt::format("DCG rebuild: target rssid {} not found in merged metadata", target_rssid));
}

// Step B — resolve the merged tablet schema PB for a given rowset.
const TabletSchemaPB* resolve_rowset_schema_pb(const TabletMetadataPB& new_metadata, const RowsetMetadataPB& rowset) {
    const auto& rowset_to_schema = new_metadata.rowset_to_schema();
    const auto schema_id_iter = rowset_to_schema.find(rowset.id());
    if (schema_id_iter != rowset_to_schema.end()) {
        const auto& historical_schemas = new_metadata.historical_schemas();
        auto schema_iter = historical_schemas.find(schema_id_iter->second);
        if (schema_iter != historical_schemas.end()) return &schema_iter->second;
    }
    if (new_metadata.has_schema()) return &new_metadata.schema();
    return nullptr;
}

struct DcgRowWindow {
    size_t child_index;
    Range<rowid_t> range;
};

// Step C — compute row windows in the target segment for every source-child
// rowset that references it. Coverage over [0, num_rows_of_target) is validated.
Status compute_row_windows_for_source_rowsets(TabletManager* tablet_manager, int64_t new_tablet_id,
                                              const RowsetMetadataPB& target_rowset, int target_segment_position,
                                              const TabletSchemaCSPtr& full_tablet_schema,
                                              const std::vector<DcgSourceRowsetReference>& source_references,
                                              std::vector<DcgRowWindow>* out_windows) {
    // Open base segment for index lookups.
    FileInfo base_segment_file_info;
    base_segment_file_info.path =
            tablet_manager->segment_location(new_tablet_id, target_rowset.segments(target_segment_position));
    if (target_rowset.segment_size_size() > target_segment_position) {
        base_segment_file_info.size = target_rowset.segment_size(target_segment_position);
    }
    if (target_rowset.bundle_file_offsets_size() > target_segment_position) {
        base_segment_file_info.bundle_file_offset = target_rowset.bundle_file_offsets(target_segment_position);
    }
    if (target_rowset.segment_encryption_metas_size() > target_segment_position) {
        base_segment_file_info.encryption_meta = target_rowset.segment_encryption_metas(target_segment_position);
    }

    ASSIGN_OR_RETURN(auto file_system, FileSystemFactory::CreateSharedFromString(base_segment_file_info.path));
    ASSIGN_OR_RETURN(auto base_segment,
                     Segment::open(file_system, base_segment_file_info, /*segment_id=*/0, full_tablet_schema,
                                   /*footer_length_hint=*/nullptr, /*partial_rowset_footer=*/nullptr,
                                   /*lake_io_opts=*/LakeIOOptions{}, tablet_manager));

    const rowid_t num_rows_in_target = static_cast<rowid_t>(base_segment->num_rows());

    out_windows->clear();
    out_windows->reserve(source_references.size());

    for (const auto& source_reference : source_references) {
        Range<rowid_t> window{0, num_rows_in_target};
        if (source_reference.effective_range != nullptr) {
            ASSIGN_OR_RETURN(auto seek_range, TabletRangeHelper::create_seek_range_from(
                                                      *source_reference.effective_range, full_tablet_schema,
                                                      /*mem_pool=*/nullptr));
            LakeIOOptions lake_io_options{.fill_data_cache = false};
            ASSIGN_OR_RETURN(auto rowid_range_opt,
                             segment_seek_range_to_rowid_range(base_segment, seek_range, lake_io_options));
            if (!rowid_range_opt.has_value()) {
                continue; // empty window
            }
            window = *rowid_range_opt;
            // Clip to [0, num_rows_in_target)
            window = Range<rowid_t>(std::max<rowid_t>(window.begin(), 0),
                                    std::min<rowid_t>(window.end(), num_rows_in_target));
            if (window.begin() >= window.end()) {
                continue;
            }
        }
        out_windows->push_back({source_reference.child_index, window});
    }

    if (out_windows->empty()) {
        return Status::NotSupported(fmt::format(
                "DCG rebuild: no valid row windows computed for target rssid (num_rows={})", num_rows_in_target));
    }

    // Dedup windows that belong to the SAME child AND have the same range
    // (e.g., a child's shared rowset surfaced twice through different scans).
    // Do NOT dedup windows from different children even if the range matches:
    // those represent distinct authoritative updaters for the same rows and
    // must surface as an overlap failure, not be silently collapsed.
    std::sort(out_windows->begin(), out_windows->end(), [](const DcgRowWindow& left, const DcgRowWindow& right) {
        if (left.range.begin() != right.range.begin()) return left.range.begin() < right.range.begin();
        if (left.range.end() != right.range.end()) return left.range.end() < right.range.end();
        return left.child_index < right.child_index;
    });
    std::vector<DcgRowWindow> deduped_windows;
    deduped_windows.reserve(out_windows->size());
    for (auto& window : *out_windows) {
        if (!deduped_windows.empty() && deduped_windows.back().range.begin() == window.range.begin() &&
            deduped_windows.back().range.end() == window.range.end() &&
            deduped_windows.back().child_index == window.child_index) {
            continue; // same child + same range: safe to collapse
        }
        deduped_windows.push_back(window);
    }
    *out_windows = std::move(deduped_windows);

    // Coverage validation: windows must be contiguous and cover [0, num_rows_in_target).
    //
    // Known limitation: the synthesized gap delvec from
    // compute_synthesized_gap_specs masks rowids that no contributing child
    // claims, but DCG rebuild does not yet consult that bitmap. A
    // (compacted-child gap) × (DCG conflict on canonical R0) combination
    // therefore returns NotSupported here. FE scheduling currently avoids
    // the combo by requiring all children compacted before merge for any
    // tablet with active partial-update DCGs.
    if ((*out_windows)[0].range.begin() != 0) {
        return Status::NotSupported(
                fmt::format("DCG rebuild: row window coverage gap at the start (first.begin={}, expect 0)",
                            (*out_windows)[0].range.begin()));
    }
    for (size_t index = 0; index + 1 < out_windows->size(); ++index) {
        if ((*out_windows)[index].range.end() != (*out_windows)[index + 1].range.begin()) {
            return Status::NotSupported(fmt::format("DCG rebuild: row window coverage gap or overlap ({}->{} vs {})",
                                                    (*out_windows)[index].range.begin(),
                                                    (*out_windows)[index].range.end(),
                                                    (*out_windows)[index + 1].range.begin()));
        }
    }
    if (out_windows->back().range.end() != num_rows_in_target) {
        return Status::NotSupported(
                fmt::format("DCG rebuild: row window coverage gap at the end (last.end={}, expect {})",
                            out_windows->back().range.end(), num_rows_in_target));
    }
    return Status::OK();
}

// Helper: open a source .cols file as a Segment (projection = entry's
// unique_column_ids restricted subset of the merged tablet schema).
StatusOr<std::shared_ptr<Segment>> open_source_dcg_segment(TabletManager* tablet_manager, int64_t owner_tablet_id,
                                                           const std::string& relative_path,
                                                           const std::string& encryption_meta,
                                                           const TabletSchemaCSPtr& entry_schema) {
    FileInfo file_info;
    file_info.path = tablet_manager->segment_location(owner_tablet_id, relative_path);
    file_info.encryption_meta = encryption_meta;
    ASSIGN_OR_RETURN(auto file_system, FileSystemFactory::CreateSharedFromString(file_info.path));
    return Segment::open(file_system, file_info, /*segment_id=*/0, entry_schema, /*footer_length_hint=*/nullptr,
                         /*partial_rowset_footer=*/nullptr, /*lake_io_opts=*/LakeIOOptions{}, tablet_manager);
}

// Helper: read [row_begin, row_end) rows of |column_unique_id| from |segment|,
// previously opened with |entry_schema| which must contain that UID. The
// column values are appended to |destination|.
Status read_column_range_from_segment(const std::shared_ptr<Segment>& segment, const TabletSchemaCSPtr& entry_schema,
                                      uint32_t column_unique_id, rowid_t row_begin, rowid_t row_end,
                                      Column* destination) {
    const int32_t column_index = entry_schema->field_index(static_cast<ColumnUID>(column_unique_id));
    if (column_index < 0) {
        return Status::Corruption(
                fmt::format("DCG rebuild: source segment schema is missing column UID {}", column_unique_id));
    }
    const auto& tablet_column = entry_schema->column(column_index);
    OlapReaderStatistics reader_statistics;

    ASSIGN_OR_RETURN(auto column_iterator, segment->new_column_iterator(tablet_column, /*path=*/nullptr));

    // Build a RandomAccessFile for the segment's file (required by
    // ColumnIteratorOptions::read_file). Segment's new_iterator path is too
    // heavy for a single-column read, so we build a dedicated handle here.
    ASSIGN_OR_RETURN(auto file_system, FileSystemFactory::CreateSharedFromString(segment->file_info().path));
    RandomAccessFileOptions random_access_file_options;
    if (!segment->file_info().encryption_meta.empty()) {
        ASSIGN_OR_RETURN(auto encryption_info,
                         KeyCache::instance().unwrap_encryption_meta(segment->file_info().encryption_meta));
        random_access_file_options.encryption_info = std::move(encryption_info);
    }
    ASSIGN_OR_RETURN(auto random_access_file, file_system->new_random_access_file_with_bundling(
                                                      random_access_file_options, segment->file_info()));

    ColumnIteratorOptions column_iterator_options;
    column_iterator_options.read_file = random_access_file.get();
    column_iterator_options.stats = &reader_statistics;
    column_iterator_options.lake_io_opts = LakeIOOptions{.fill_data_cache = false};
    column_iterator_options.chunk_size = std::max<int>(1, static_cast<int>(row_end - row_begin));
    RETURN_IF_ERROR(column_iterator->init(column_iterator_options));
    RETURN_IF_ERROR(column_iterator->seek_to_ordinal(row_begin));

    size_t remaining_rows = row_end - row_begin;
    while (remaining_rows > 0) {
        size_t batch_size = remaining_rows;
        RETURN_IF_ERROR(column_iterator->next_batch(&batch_size, destination));
        if (batch_size == 0) {
            return Status::InternalError("DCG rebuild: column iterator returned 0 rows before exhausting range");
        }
        remaining_rows -= batch_size;
    }
    return Status::OK();
}

// Per-target rebuild — Steps A-F.
// Returns the single-entry PB describing the newly written .cols file.
StatusOr<DeltaColumnGroupVerPB> rebuild_dcg_for_target_segment(
        TabletManager* tablet_manager, const std::vector<TabletMergeContext>& merge_contexts, int64_t new_tablet_id,
        int64_t new_version, int64_t txn_id, const TabletMetadataPB& new_metadata, uint32_t target_rssid,
        const std::vector<uint32_t>& rebuild_columns, const std::vector<const DcgSurvivingEntry*>& conflicting_entries,
        const std::vector<DcgSourceRowsetReference>& source_references) {
    TEST_SYNC_POINT_CALLBACK("merge_dcg_meta:before_rebuild", &target_rssid);

    // Step A — locate merged rowset + segment position for target rssid.
    ASSIGN_OR_RETURN(auto located_pair, locate_target_in_merged_metadata(new_metadata, target_rssid));
    const RowsetMetadataPB& target_rowset = *located_pair.first;
    const int target_segment_position = located_pair.second;

    // Step B — resolve full tablet schema + rebuild schema.
    const TabletSchemaPB* schema_pb = resolve_rowset_schema_pb(new_metadata, target_rowset);
    if (schema_pb == nullptr) {
        return Status::NotSupported(
                fmt::format("DCG rebuild: no tablet schema available for rowset {}", target_rowset.id()));
    }
    TabletSchemaCSPtr full_tablet_schema = TabletSchema::create(*schema_pb);
    if (full_tablet_schema->sort_key_idxes().empty()) {
        return Status::NotSupported("DCG rebuild: tablet schema has no sort key");
    }
    std::vector<ColumnUID> rebuild_unique_ids;
    rebuild_unique_ids.reserve(rebuild_columns.size());
    for (uint32_t unique_id : rebuild_columns) rebuild_unique_ids.push_back(static_cast<ColumnUID>(unique_id));
    TabletSchemaCSPtr rebuild_schema = TabletSchema::create_with_uid(full_tablet_schema, rebuild_unique_ids);
    // create_with_uid silently drops UIDs not found in the base schema. If the
    // merged historical schema is missing any conflict UID, the rebuilt file
    // would otherwise omit that column silently. Fail fast instead.
    if (rebuild_schema->num_columns() != rebuild_columns.size()) {
        return Status::NotSupported(fmt::format(
                "DCG rebuild: merged tablet schema is missing one or more rebuild column UIDs (expected {} columns, "
                "got {}); cannot safely rebuild .cols",
                rebuild_columns.size(), rebuild_schema->num_columns()));
    }

    // Step C — compute row windows.
    std::vector<DcgRowWindow> windows;
    RETURN_IF_ERROR(compute_row_windows_for_source_rowsets(tablet_manager, new_tablet_id, target_rowset,
                                                           target_segment_position, full_tablet_schema,
                                                           source_references, &windows));
    const rowid_t num_rows_in_target = windows.back().range.end();

    // For each rebuild column, pick:
    // - default donor: any conflicting entry that claims the UID (first found).
    // - per-child overrides: the conflicting entry from the child that claims
    //   the UID, to be used for rows in that child's owner window.
    struct ColumnSourceInfo {
        const DcgSurvivingEntry* default_donor = nullptr;
        std::unordered_map<size_t, const DcgSurvivingEntry*> override_by_child_index;
    };
    std::unordered_map<uint32_t, ColumnSourceInfo> column_source_info_by_unique_id;
    for (uint32_t unique_id : rebuild_columns) {
        for (const DcgSurvivingEntry* entry : conflicting_entries) {
            if (!entry_claims_column_uid(*entry, unique_id)) continue;
            auto& info = column_source_info_by_unique_id[unique_id];
            if (info.default_donor == nullptr) info.default_donor = entry;
            info.override_by_child_index[entry->child_index] = entry;
        }
        if (column_source_info_by_unique_id[unique_id].default_donor == nullptr) {
            return Status::InternalError(fmt::format("DCG rebuild: no donor found for column UID {}", unique_id));
        }
    }

    // Open each referenced source .cols segment (cached per entry address).
    std::unordered_map<const DcgSurvivingEntry*, std::shared_ptr<Segment>> opened_source_segments;
    std::unordered_map<const DcgSurvivingEntry*, TabletSchemaCSPtr> entry_schemas;

    auto get_source_segment = [&](const DcgSurvivingEntry* entry) -> StatusOr<std::shared_ptr<Segment>> {
        auto cache_iter = opened_source_segments.find(entry);
        if (cache_iter != opened_source_segments.end()) return cache_iter->second;
        std::vector<ColumnUID> entry_unique_ids;
        for (auto column_id : entry->single_entry.unique_column_ids(0).column_ids()) {
            entry_unique_ids.push_back(static_cast<ColumnUID>(column_id));
        }
        TabletSchemaCSPtr entry_schema = TabletSchema::create_with_uid(full_tablet_schema, entry_unique_ids);
        int64_t owner_tablet_id = merge_contexts[entry->child_index].metadata()->id();
        const std::string& file_name = entry->single_entry.column_files(0);
        const std::string& encryption_meta = entry->single_entry.encryption_metas(0);
        ASSIGN_OR_RETURN(auto segment, open_source_dcg_segment(tablet_manager, owner_tablet_id, file_name,
                                                               encryption_meta, entry_schema));
        entry_schemas[entry] = entry_schema;
        opened_source_segments[entry] = segment;
        return segment;
    };

    TEST_SYNC_POINT_CALLBACK("merge_dcg_meta:after_open_sources", &target_rssid);

    // Step D — assemble columns IN rebuild_schema ORDER. TabletSchema::create_with_uid
    // preserves the base schema's column order, which can differ from the
    // insertion order of |rebuild_columns|. Chunk binds columns positionally,
    // so we must iterate schema positions, not UID insertion order, to avoid
    // writing column data into the wrong (UID, type) slot.
    //
    // Full-column materialization keeps this code path simple and correct;
    // DCG files are already at segment size, so the peak is bounded by a
    // single rebuilt column over the full segment. Row-batch streaming is a
    // future optimization.
    const size_t num_columns = rebuild_schema->num_columns();
    Columns rebuilt_columns(num_columns);
    std::vector<uint32_t> ordered_unique_ids;
    ordered_unique_ids.reserve(num_columns);
    for (size_t column_index = 0; column_index < num_columns; ++column_index) {
        const auto& tablet_column = rebuild_schema->column(column_index);
        const uint32_t unique_id = static_cast<uint32_t>(tablet_column.unique_id());
        ordered_unique_ids.push_back(unique_id);

        auto source_info_iter = column_source_info_by_unique_id.find(unique_id);
        if (source_info_iter == column_source_info_by_unique_id.end()) {
            return Status::InternalError(
                    fmt::format("DCG rebuild: rebuild_schema has UID {} with no source", unique_id));
        }
        const auto& source_info = source_info_iter->second;

        auto field = ChunkHelper::convert_field(column_index, tablet_column);
        MutableColumnPtr output_column = ChunkHelper::column_from_field(field);
        output_column->reserve(num_rows_in_target);

        for (const auto& window : windows) {
            const DcgSurvivingEntry* selected_source = nullptr;
            auto override_iter = source_info.override_by_child_index.find(window.child_index);
            selected_source = (override_iter != source_info.override_by_child_index.end()) ? override_iter->second
                                                                                           : source_info.default_donor;
            ASSIGN_OR_RETURN(auto source_segment, get_source_segment(selected_source));
            RETURN_IF_ERROR(read_column_range_from_segment(source_segment, entry_schemas[selected_source], unique_id,
                                                           window.range.begin(), window.range.end(),
                                                           output_column.get()));
        }

        if (output_column->size() != static_cast<size_t>(num_rows_in_target)) {
            return Status::InternalError(fmt::format("DCG rebuild: column UID {} size {} != num_rows {}", unique_id,
                                                     output_column->size(), num_rows_in_target));
        }
        rebuilt_columns[column_index] = std::move(output_column);
    }

    // Step F — write new .cols file.
    Schema output_schema = ChunkHelper::convert_schema(rebuild_schema);
    auto output_chunk = std::make_shared<Chunk>(std::move(rebuilt_columns), std::make_shared<Schema>(output_schema));

    const std::string new_file_basename = gen_cols_filename(txn_id);
    const std::string new_file_path = tablet_manager->segment_location(new_tablet_id, new_file_basename);
    WritableFileOptions writable_file_options{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    SegmentWriterOptions segment_writer_options;
    if (new_metadata.has_flat_json_config()) {
        segment_writer_options.flat_json_config = std::make_shared<FlatJsonConfig>();
        segment_writer_options.flat_json_config->update(new_metadata.flat_json_config());
    }
    if (config::enable_transparent_data_encryption) {
        ASSIGN_OR_RETURN(auto encryption_meta_pair,
                         KeyCache::instance().create_encryption_meta_pair_using_current_kek());
        writable_file_options.encryption_info = encryption_meta_pair.info;
        segment_writer_options.encryption_meta = std::move(encryption_meta_pair.encryption_meta);
    }
    ASSIGN_OR_RETURN(auto writable_file, fs::new_writable_file(writable_file_options, new_file_path));
    auto segment_writer = std::make_unique<SegmentWriter>(std::move(writable_file), /*segment_id=*/0, rebuild_schema,
                                                          segment_writer_options);
    RETURN_IF_ERROR(segment_writer->init(false));
    RETURN_IF_ERROR(segment_writer->append_chunk(*output_chunk));
    uint64_t written_file_size = 0;
    uint64_t written_index_size = 0;
    uint64_t written_footer_position = 0;
    RETURN_IF_ERROR(segment_writer->finalize(&written_file_size, &written_index_size, &written_footer_position));

    TEST_SYNC_POINT_CALLBACK("merge_dcg_meta:after_write_cols", const_cast<std::string*>(&new_file_basename));

    // Step E — build single-entry PB for the rebuilt file. Emit unique_column_ids
    // in the SAME order as the written chunk's columns (rebuild_schema order).
    // Mismatched order would cause reader schema binding to mismatch the physical
    // column positions in the .cols segment.
    DeltaColumnGroupVerPB rebuilt;
    rebuilt.add_column_files(new_file_basename);
    auto* unique_column_ids_pb = rebuilt.add_unique_column_ids();
    for (uint32_t unique_id : ordered_unique_ids) unique_column_ids_pb->add_column_ids(unique_id);
    rebuilt.add_versions(new_version);
    rebuilt.add_encryption_metas(segment_writer->encryption_meta());
    rebuilt.add_shared_files(false);
    return rebuilt;
}

Status merge_dcg_meta(TabletManager* tablet_manager, const std::vector<TabletMergeContext>& merge_contexts,
                      int64_t new_tablet_id, int64_t new_version, int64_t txn_id, TabletMetadataPB* new_metadata) {
    std::map<uint32_t, DcgTargetWorkItem> work_by_target;
    RETURN_IF_ERROR(dcg_pass1_collect_entries_and_sources(merge_contexts, &work_by_target));

    auto* merged_dcgs = new_metadata->mutable_dcg_meta()->mutable_dcgs();

    // Track full paths of rebuilt .cols files so we can best-effort clean them
    // up if a later target's rebuild fails partway through. Downstream failures
    // (merge_delvecs/merge_sstables/publish) still rely on standard orphan-file
    // vacuum, which matches the pattern used by merge_delvec_files.
    std::vector<std::string> rebuilt_file_paths;
    auto cleanup_on_failure = [&]() {
        for (const auto& path : rebuilt_file_paths) {
            auto status = fs::delete_file(path);
            LOG_IF(WARNING, !status.ok() && !status.is_not_found())
                    << "failed to clean up partial DCG rebuild file " << path << ": " << status;
        }
        rebuilt_file_paths.clear();
    };

    for (auto& [target_rssid, target_work] : work_by_target) {
        if (target_work.entries.empty()) continue;

        // Identify conflicting entries: any entry claiming a UID shared
        // with another entry. Conflict-free entries are emitted as-is;
        // conflicting entries are rebuilt below.
        const std::vector<bool> entry_is_conflicting = mark_conflicting_dcg_entries(target_work.entries);

        DeltaColumnGroupVerPB final_dcg;

        // Emit non-conflicting entries unchanged.
        for (size_t entry_index = 0; entry_index < target_work.entries.size(); ++entry_index) {
            if (entry_is_conflicting[entry_index]) continue;
            const auto& entry = target_work.entries[entry_index];
            final_dcg.add_column_files(entry.single_entry.column_files(0));
            final_dcg.add_unique_column_ids()->CopyFrom(entry.single_entry.unique_column_ids(0));
            final_dcg.add_versions(entry.single_entry.versions(0));
            final_dcg.add_encryption_metas(entry.single_entry.encryption_metas(0));
            final_dcg.add_shared_files(entry.single_entry.shared_files(0));
        }

        bool any_entry_is_conflicting = false;
        for (bool conflicting : entry_is_conflicting) any_entry_is_conflicting |= conflicting;

        if (any_entry_is_conflicting) {
            // Fold ALL columns of every conflicting entry into rebuild_columns
            // so the reader's first-entry-wins rule can't leak stale values.
            std::vector<uint32_t> rebuild_columns;
            std::unordered_set<uint32_t> seen_rebuild_columns;
            std::vector<const DcgSurvivingEntry*> conflicting_entries;
            for (size_t entry_index = 0; entry_index < target_work.entries.size(); ++entry_index) {
                if (!entry_is_conflicting[entry_index]) continue;
                conflicting_entries.push_back(&target_work.entries[entry_index]);
                for (auto unique_id : target_work.entries[entry_index].single_entry.unique_column_ids(0).column_ids()) {
                    if (seen_rebuild_columns.insert(unique_id).second) {
                        rebuild_columns.push_back(unique_id);
                    }
                }
            }

            StatusOr<DeltaColumnGroupVerPB> rebuilt_or_status = rebuild_dcg_for_target_segment(
                    tablet_manager, merge_contexts, new_tablet_id, new_version, txn_id, *new_metadata, target_rssid,
                    rebuild_columns, conflicting_entries, target_work.source_refs);
            if (!rebuilt_or_status.ok()) {
                if (rebuilt_or_status.status().is_not_supported()) {
                    g_tablet_merge_dcg_rebuild_fallback_not_supported_total << 1;
                }
                cleanup_on_failure();
                return rebuilt_or_status.status();
            }
            const auto& rebuilt_entry = *rebuilt_or_status;
            rebuilt_file_paths.push_back(
                    tablet_manager->segment_location(new_tablet_id, rebuilt_entry.column_files(0)));
            final_dcg.add_column_files(rebuilt_entry.column_files(0));
            final_dcg.add_unique_column_ids()->CopyFrom(rebuilt_entry.unique_column_ids(0));
            final_dcg.add_versions(rebuilt_entry.versions(0));
            final_dcg.add_encryption_metas(rebuilt_entry.encryption_metas(0));
            final_dcg.add_shared_files(rebuilt_entry.shared_files(0));
            g_tablet_merge_dcg_rebuild_total << 1;
        }

        if (final_dcg.column_files_size() == 0) continue;
        auto shape_status = validate_dcg_shape(final_dcg);
        if (!shape_status.ok()) {
            cleanup_on_failure();
            return shape_status;
        }
        (*merged_dcgs)[target_rssid] = std::move(final_dcg);
    }

    return Status::OK();
}

// Phase 0 output: per-target rowid bitmaps representing keys in the
// shared physical segment that no contributing child claims. Read-path
// consumers:
//   - canonical R0's segment iterator already filters by canonical.range, so
//     gap rowids outside the convex hull are no-ops for scans.
//   - PersistentIndexSstable::multi_get filters by the projected delvec on
//     the sstable PB regardless of LSM block-sort order, which keeps the
//     first-child-compacts case safe.
struct CanonicalGapSpec {
    uint32_t target_rssid;
    Roaring gap_bits;
};

// Phase 0: for every PK canonical rowset that owns at least one shared
// segment, mask the rowids whose key falls outside ⋃ contributors but inside
// the merged tablet range.
//
// Bound = merged tablet range, not unbounded `(-∞, +∞)`. The plan's original
// motivation for unbounded was to surface keys outside canonical.range (left
// or right edges); but since each child's tablet.range is a sub-range of the
// pre-split tablet, and merged_tablet.range = union of children's tablet
// ranges, the shared physical segment never carries keys outside the merged
// tablet range. The unbounded helper would just generate edge complements
// that seek to empty rowid windows. Bounded-by-merged-tablet-range is both
// correct and lets us skip the segment open entirely when contributors fully
// cover the merged tablet range (the no-compaction common case).
StatusOr<std::vector<CanonicalGapSpec>> compute_synthesized_gap_specs(TabletManager* tablet_manager,
                                                                      const TabletMetadataPB& new_metadata,
                                                                      const CanonicalContribMap& canonical_contribs) {
    std::vector<CanonicalGapSpec> result;
    for (const auto& [canonical_index, contrib] : canonical_contribs) {
        if (canonical_index < 0 || canonical_index >= new_metadata.rowsets_size()) {
            return Status::InternalError(
                    fmt::format("compute_synthesized_gap_specs: invalid canonical_index {}", canonical_index));
        }
        const auto& canonical = new_metadata.rowsets(canonical_index);
        // shared_segments_size() alone is insufficient because metadata
        // transforms can leave an all-false vector behind. Only synthesize
        // gap bits when at least one segment is actually shared.
        bool has_shared = false;
        for (int i = 0; i < canonical.shared_segments_size(); ++i) {
            if (canonical.shared_segments(i)) {
                has_shared = true;
                break;
            }
        }
        if (!has_shared) continue;

        ASSIGN_OR_RETURN(auto sorted_disjoint, tablet_reshard_helper::sort_and_merge_adjacent_ranges(contrib));
        ASSIGN_OR_RETURN(auto non_contributed,
                         tablet_reshard_helper::compute_disjoint_gaps_within(new_metadata.range(), sorted_disjoint));
        if (non_contributed.empty()) continue;

        const TabletSchemaPB* schema_pb = resolve_rowset_schema_pb(new_metadata, canonical);
        if (schema_pb == nullptr) {
            return Status::Corruption("compute_synthesized_gap_specs: schema not found for canonical rowset");
        }
        TabletSchemaCSPtr schema = TabletSchema::create(*schema_pb);

        for (int seg_pos = 0; seg_pos < canonical.shared_segments_size(); ++seg_pos) {
            if (!canonical.shared_segments(seg_pos)) continue;
            uint32_t target_rssid = get_rssid(canonical, seg_pos);

            FileInfo seg_file_info;
            seg_file_info.path = tablet_manager->segment_location(new_metadata.id(), canonical.segments(seg_pos));
            if (canonical.segment_size_size() > seg_pos) {
                seg_file_info.size = canonical.segment_size(seg_pos);
            }
            if (canonical.bundle_file_offsets_size() > seg_pos) {
                seg_file_info.bundle_file_offset = canonical.bundle_file_offsets(seg_pos);
            }
            if (canonical.segment_encryption_metas_size() > seg_pos) {
                seg_file_info.encryption_meta = canonical.segment_encryption_metas(seg_pos);
            }
            ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(seg_file_info.path));
            ASSIGN_OR_RETURN(auto base_segment,
                             Segment::open(fs, seg_file_info, /*segment_id=*/0, schema,
                                           /*footer_length_hint=*/nullptr, /*partial_rowset_footer=*/nullptr,
                                           /*lake_io_opts=*/LakeIOOptions{}, tablet_manager));
            const rowid_t num_rows = static_cast<rowid_t>(base_segment->num_rows());
            if (num_rows == 0) continue;

            Roaring gap_bits;
            for (const auto& gap_range : non_contributed) {
                ASSIGN_OR_RETURN(auto seek_range,
                                 TabletRangeHelper::create_seek_range_from(gap_range, schema, /*mem_pool=*/nullptr));
                LakeIOOptions io_opts{.fill_data_cache = false};
                ASSIGN_OR_RETURN(auto rowid_range_opt,
                                 segment_seek_range_to_rowid_range(base_segment, seek_range, io_opts));
                if (!rowid_range_opt.has_value()) continue;
                rowid_t lo = std::max<rowid_t>(rowid_range_opt->begin(), 0);
                rowid_t hi = std::min<rowid_t>(rowid_range_opt->end(), num_rows);
                if (lo >= hi) continue;
                gap_bits.addRange(static_cast<uint64_t>(lo), static_cast<uint64_t>(hi));
            }
            if (!gap_bits.isEmpty()) {
                result.push_back(CanonicalGapSpec{target_rssid, std::move(gap_bits)});
            }
        }
    }
    return result;
}

// Phase 2.5 of merge_delvecs: union each synthesized gap bitmap into the
// corresponding target state. Mirrors the Phase 2 transitions but the source
// is a synthesized Roaring rather than a child delvec page — so no
// (file_name, offset) entry goes into seen_sources.
//
// Uses DelVector::union_with(Roaring) directly (no rowid-vector round-trip):
// a compacted-away child's gap can span millions of rowids, and the legacy
// path through std::vector<uint32_t> + DelVector::init(uint32_t*, size) +
// union_delvec would re-enumerate every rowid into a vector twice.
Status inject_synthesized_gaps_into_target_states(TabletManager* tablet_manager,
                                                  const std::vector<CanonicalGapSpec>& specs, int64_t new_version,
                                                  std::map<uint32_t, TargetDelvecState>* target_states) {
    for (const auto& spec : specs) {
        if (spec.gap_bits.isEmpty()) continue;
        auto& state = (*target_states)[spec.target_rssid];

        if (state.single_source.has_value() && !state.merged) {
            // Promote single_source → merged: load the source delvec, OR in gap_bits.
            DelVector dv_prev;
            const auto& ref = *state.single_source;
            LakeIOOptions io_opts;
            RETURN_IF_ERROR(get_del_vec(tablet_manager, *ref.ctx->metadata(), ref.page, false, io_opts, &dv_prev));
            auto merged_dv = std::make_unique<DelVector>();
            if (dv_prev.roaring()) {
                merged_dv->union_with(new_version, *dv_prev.roaring());
            }
            merged_dv->union_with(new_version, spec.gap_bits);
            state.merged = std::move(merged_dv);
            state.single_source.reset();
        } else if (state.merged) {
            state.merged->union_with(new_version, spec.gap_bits);
        } else {
            // Empty state: construct merged directly from gap_bits.
            auto merged_dv = std::make_unique<DelVector>();
            merged_dv->union_with(new_version, spec.gap_bits);
            state.merged = std::move(merged_dv);
        }
    }
    return Status::OK();
}

Status merge_delvecs(TabletManager* tablet_manager, const std::vector<TabletMergeContext>& merge_contexts,
                     const CanonicalContribMap& canonical_contribs, int64_t new_version, int64_t txn_id,
                     TabletMetadataPB* new_metadata) {
    // Phase 0: synthesize gap delvec bits from canonical_contribs.
    ASSIGN_OR_RETURN(auto synthesized_gap_specs,
                     compute_synthesized_gap_specs(tablet_manager, *new_metadata, canonical_contribs));
    if (!synthesized_gap_specs.empty()) {
        g_tablet_merge_gap_delvec_total << 1;
    }

    // Phase 1: Collect unique delvec files across all children
    std::vector<DelvecFileInfo> unique_delvec_files;
    std::unordered_map<std::string, size_t> file_name_to_index;

    for (const auto& ctx : merge_contexts) {
        if (!ctx.metadata()->has_delvec_meta()) {
            continue;
        }
        for (const auto& [ver, file] : ctx.metadata()->delvec_meta().version_to_file()) {
            (void)ver;
            auto [it, inserted] = file_name_to_index.emplace(file.name(), unique_delvec_files.size());
            if (inserted) {
                DelvecFileInfo file_info;
                file_info.tablet_id = ctx.metadata()->id();
                file_info.delvec_file = file;
                unique_delvec_files.emplace_back(std::move(file_info));
            } else {
                // Consistency check: same file name must have same size, encryption_meta, shared
                const auto& existing = unique_delvec_files[it->second].delvec_file;
                if (existing.size() != file.size() || existing.encryption_meta() != file.encryption_meta() ||
                    existing.shared() != file.shared()) {
                    return Status::Corruption("Delvec file metadata mismatch for same file name");
                }
            }
        }
    }

    // Early-return only when there is no work at all: no source delvec files
    // AND no synthesized gaps. A clean PK table with no prior deletes can
    // still need a synthesized gap delvec to mask compacted-away child rowids.
    if (unique_delvec_files.empty() && synthesized_gap_specs.empty()) {
        return Status::OK();
    }

    // Phase 2: Scan pages, build TargetDelvecState for each target rssid.
    // File name is resolved inline via each child's version_to_file map.
    std::map<uint32_t, TargetDelvecState> target_states;

    for (const auto& ctx : merge_contexts) {
        if (!ctx.metadata()->has_delvec_meta()) {
            continue;
        }
        for (const auto& [segment_id, page] : ctx.metadata()->delvec_meta().delvecs()) {
            ASSIGN_OR_RETURN(uint32_t target, ctx.map_rssid(segment_id));

            // Resolve file name from page version
            auto file_it = ctx.metadata()->delvec_meta().version_to_file().find(page.version());
            if (file_it == ctx.metadata()->delvec_meta().version_to_file().end()) {
                return Status::InvalidArgument("Delvec file not found for page version");
            }
            const std::string& file_name = file_it->second.name();

            auto& state = target_states[target];
            auto source_key = std::make_pair(file_name, page.offset());

            if (!state.single_source.has_value() && !state.merged) {
                // Empty state: first encounter
                state.single_source = DelvecSourceRef{&ctx, page, file_name};
                state.seen_sources[source_key] = page.size();
            } else if (state.single_source.has_value() && !state.merged) {
                // single_source state
                auto seen_it = state.seen_sources.find(source_key);
                if (seen_it != state.seen_sources.end()) {
                    // Dedup hit: same file_name + offset
                    if (seen_it->second != page.size()) {
                        return Status::Corruption("Delvec page size mismatch for same source");
                    }
                    // Skip (page-ref dedup)
                    continue;
                }
                // Different source: load both and union
                DelVector dv_prev;
                {
                    const auto& ref = *state.single_source;
                    LakeIOOptions io_opts;
                    RETURN_IF_ERROR(
                            get_del_vec(tablet_manager, *ref.ctx->metadata(), ref.page, false, io_opts, &dv_prev));
                }
                DelVector dv_new;
                {
                    LakeIOOptions io_opts;
                    RETURN_IF_ERROR(get_del_vec(tablet_manager, *ctx.metadata(), page, false, io_opts, &dv_new));
                }
                // Union
                auto merged_dv = std::make_unique<DelVector>();
                union_delvec(merged_dv.get(), dv_prev, new_version);
                union_delvec(merged_dv.get(), dv_new, new_version);
                state.merged = std::move(merged_dv);
                state.single_source.reset();
                state.seen_sources[source_key] = page.size();
            } else {
                // merged state
                auto seen_it = state.seen_sources.find(source_key);
                if (seen_it != state.seen_sources.end()) {
                    if (seen_it->second != page.size()) {
                        return Status::Corruption("Delvec page size mismatch for same source in merged state");
                    }
                    // Skip (already merged)
                    continue;
                }
                // New source: load and union into merged
                DelVector dv_new;
                {
                    LakeIOOptions io_opts;
                    RETURN_IF_ERROR(get_del_vec(tablet_manager, *ctx.metadata(), page, false, io_opts, &dv_new));
                }
                union_delvec(state.merged.get(), dv_new, new_version);
                state.seen_sources[source_key] = page.size();
            }
        }
    }

    // Phase 2.5: inject synthesized gap delvecs into target_states. Each spec's
    // bitmap masks rowids in the shared physical segment whose key was
    // contributed by no surviving child (e.g., a child that compacted away
    // its copy of the shared rowset). Promotes any single_source state to
    // merged so the bitmap can be unioned in.
    RETURN_IF_ERROR(inject_synthesized_gaps_into_target_states(tablet_manager, synthesized_gap_specs, new_version,
                                                               &target_states));

    // Phase 3: Serialize union results into union_buffer
    std::string union_buffer;
    std::map<uint32_t, UnionPageInfo> union_page_infos;

    for (auto& [target, state] : target_states) {
        if (state.merged) {
            std::string data = state.merged->save();
            uint32_t masked_crc = crc32c::Mask(crc32c::Value(data.data(), data.size()));
            union_page_infos[target] = {static_cast<uint64_t>(union_buffer.size()), static_cast<uint64_t>(data.size()),
                                        masked_crc};
            union_buffer.append(data);
        }
    }

    // Phase 4: Write one file. Three routes depending on what contributed:
    //   1. only synthesized (no source delvec files) → write_delvec_file_from_buffer
    //      with the union_buffer at offset 0; sidesteps merge_delvec_files's
    //      DCHECK on (empty old_files + non-empty extra_data).
    //   2. only source files (no synthesized + empty union_buffer) → existing
    //      merge_delvec_files with empty extra_data.
    //   3. both → existing merge_delvec_files with extra_data populated.
    FileMetaPB new_delvec_file;
    std::vector<uint64_t> offsets;
    uint64_t union_base_offset = 0;
    if (unique_delvec_files.empty()) {
        // Route 1: synthesized-only. Phase 1's early-return guarantees that
        // synthesized_gap_specs (and therefore union_buffer) is non-empty
        // here.
        DCHECK(!union_buffer.empty()) << "synthesized-only path with empty union_buffer";
        RETURN_IF_ERROR(write_delvec_file_from_buffer(tablet_manager, new_metadata->id(), txn_id, Slice(union_buffer),
                                                      &new_delvec_file));
        union_base_offset = 0;
        g_tablet_merge_synthesized_only_delvec_total << 1;
    } else {
        // Routes 2 & 3.
        RETURN_IF_ERROR(merge_delvec_files(tablet_manager, unique_delvec_files, new_metadata->id(), txn_id,
                                           &new_delvec_file, &offsets, Slice(union_buffer), &union_base_offset));
    }

    // Build base_offset_by_file_name. Empty for synthesized-only route since
    // there are no source files to reference; merged-state targets always go
    // through union_page_infos which is keyed by target rssid, not file name.
    std::unordered_map<std::string, uint64_t> base_offset_by_file_name;
    for (size_t i = 0; i < unique_delvec_files.size(); ++i) {
        base_offset_by_file_name[unique_delvec_files[i].delvec_file.name()] = offsets[i];
    }

    TEST_SYNC_POINT_CALLBACK("merge_delvecs:before_apply_offsets", &base_offset_by_file_name);

    // Phase 5: Build page entries in new_metadata
    auto* delvec_meta = new_metadata->mutable_delvec_meta();
    delvec_meta->Clear();

    for (const auto& [target, state] : target_states) {
        DelvecPagePB new_page;
        new_page.set_version(new_version);
        new_page.set_crc32c_gen_version(new_version);

        if (state.single_source.has_value()) {
            const auto& ref = *state.single_source;
            auto base_it = base_offset_by_file_name.find(ref.file_name);
            if (base_it == base_offset_by_file_name.end()) {
                return Status::InvalidArgument("Delvec file not merged for page version");
            }
            new_page.set_offset(base_it->second + ref.page.offset());
            new_page.set_size(ref.page.size());
            // CRC decision: only reuse if old CRC is trustworthy
            if (ref.page.has_crc32c() && ref.page.crc32c_gen_version() == ref.page.version()) {
                new_page.set_crc32c(ref.page.crc32c());
            }
        } else if (state.merged) {
            auto info_it = union_page_infos.find(target);
            if (info_it == union_page_infos.end()) {
                return Status::Corruption("Union page info not found for merged target");
            }
            new_page.set_offset(union_base_offset + info_it->second.offset);
            new_page.set_size(info_it->second.size);
            new_page.set_crc32c(info_it->second.masked_crc32c);
        } else {
            return Status::Corruption("Delvec target state has neither single_source nor merged");
        }

        (*delvec_meta->mutable_delvecs())[target] = std::move(new_page);
    }

    (*delvec_meta->mutable_version_to_file())[new_version] = std::move(new_delvec_file);
    return Status::OK();
}

// Lookup maps from a child-id-space rssid to the merged tablet's final
// rssid. Built once per rebuild from the merge_contexts, scoped per
// inferred family (or per orphan child for kNoFamily ctxs) so a multi-
// family merge with overlapping source rssids never returns another
// family's mapping. Each PerFamilyMaps half follows first-emitter-wins:
//
//   data_rssid_map      — keyed by per-segment lifted rssid via
//                         get_rssid(rs, seg_pos). Honors sparse segment_idx
//                         (e.g. {0,2} after middle-segment removal). Excludes
//                         delete-only rowsets so a data entry pointing at
//                         their rssid is dropped as a ghost.
//   watermark_rssid_map — superset that ALSO records rs.id() for
//                         delete-only rowsets. Used only for projecting
//                         src_pb.max_rss_rowid (memtable flush sets it to
//                         the current rowset id, which may be delete-only).
//                         Must NOT be used for data-entry remap.
struct LegacyRssidLookupMaps {
    struct PerFamilyMaps {
        std::unordered_map<uint32_t, uint32_t> data_rssid_map;
        std::unordered_map<uint32_t, uint32_t> watermark_rssid_map;
    };

    // family_id → maps populated from that family's member ctxs only.
    std::unordered_map<uint32_t, PerFamilyMaps> per_family;
    // child_index → maps populated from THAT specific orphan ctx only.
    // Each orphan ctx (kNoFamily) gets its own scoped entry so two
    // unrelated orphan children with overlapping source rssids do not
    // pollute each other — orphan ctxs each have an independent
    // child-local id space, so a global orphan map would be susceptible
    // to the same first-emitter-wins pollution that the per-family
    // structure was designed to prevent for family ctxs.
    std::unordered_map<size_t, PerFamilyMaps> orphan_by_child;
};

// Locate the PerFamilyMaps slot for (family_id, child_index): a family ctx
// shares its family map; an orphan ctx (kNoFamily) has its own child-scoped
// map so unrelated orphan children with overlapping source rssids stay
// isolated. Returns nullptr on miss; callers wrap the result in either an
// InternalError or a DCHECK depending on whether they're building or reading.
template <typename Maps>
auto* find_per_family_maps_slot(Maps& maps, uint32_t family_id, size_t child_index) {
    if (family_id != detail::InferredSplitFamilies::kNoFamily) {
        auto iter = maps.per_family.find(family_id);
        return iter == maps.per_family.end() ? nullptr : &iter->second;
    }
    auto iter = maps.orphan_by_child.find(child_index);
    return iter == maps.orphan_by_child.end() ? nullptr : &iter->second;
}

// Read-side accessor: missing entry surfaces as InternalError so the merge
// fails loudly. build_legacy_rssid_lookup_maps pre-creates every entry, so a
// miss is a programmer bug; falling back to an empty map would silently drop
// every PK entry and manifest as data loss.
StatusOr<const LegacyRssidLookupMaps::PerFamilyMaps*> lookup_maps_for_ctx(const LegacyRssidLookupMaps& maps,
                                                                          uint32_t family_id, size_t child_index) {
    if (const auto* slot = find_per_family_maps_slot(maps, family_id, child_index); slot != nullptr) {
        return slot;
    }
    return Status::InternalError(
            fmt::format("LegacyRssidLookupMaps: missing slot (family_id={}, child_index={})", family_id, child_index));
}

// Build-phase accessor: same lookup, but DCHECK on miss since
// build_legacy_rssid_lookup_maps just pre-created the entry it's about to
// populate.
LegacyRssidLookupMaps::PerFamilyMaps& mutable_per_family_maps_for_ctx(LegacyRssidLookupMaps& maps, uint32_t family_id,
                                                                      size_t child_index) {
    auto* slot = find_per_family_maps_slot(maps, family_id, child_index);
    DCHECK(slot != nullptr) << "PerFamilyMaps not pre-created (family_id=" << family_id
                            << ", child_index=" << child_index << ")";
    return *slot;
}

StatusOr<LegacyRssidLookupMaps> build_legacy_rssid_lookup_maps(const std::vector<TabletMergeContext>& merge_contexts,
                                                               const detail::InferredSplitFamilies& families) {
    if (families.child_to_family.size() != merge_contexts.size()) {
        return Status::InvalidArgument(fmt::format(
                "InferredSplitFamilies.child_to_family size {} != merge_contexts size {}; the families must be built "
                "from the same contexts",
                families.child_to_family.size(), merge_contexts.size()));
    }
    // Validate every child_to_family entry is either kNoFamily or a real
    // family id so the population loop's find()-based lookups can rely on
    // pre-created entries (and the lookup helper never silently skips a
    // bad family id by treating it as orphan).
    for (size_t child_index = 0; child_index < families.child_to_family.size(); ++child_index) {
        const uint32_t family_id = families.child_to_family[child_index];
        if (family_id != detail::InferredSplitFamilies::kNoFamily && family_id >= families.families.size()) {
            return Status::InvalidArgument(
                    fmt::format("InferredSplitFamilies.child_to_family[{}]={} is out of range (families.size={})",
                                child_index, family_id, families.families.size()));
        }
    }

    LegacyRssidLookupMaps lookup_maps;
    // Pre-create one PerFamilyMaps per inferred family so consumers can
    // assume the entry exists when they look up a non-orphan family.
    lookup_maps.per_family.reserve(families.families.size());
    for (uint32_t family_id = 0; family_id < families.families.size(); ++family_id) {
        lookup_maps.per_family.emplace(family_id, LegacyRssidLookupMaps::PerFamilyMaps{});
    }
    // Pre-create one PerFamilyMaps per orphan child_index so consumers
    // can assume the entry exists for every kNoFamily ctx.
    for (size_t child_index = 0; child_index < merge_contexts.size(); ++child_index) {
        if (families.child_to_family[child_index] == detail::InferredSplitFamilies::kNoFamily) {
            lookup_maps.orphan_by_child.emplace(child_index, LegacyRssidLookupMaps::PerFamilyMaps{});
        }
    }

    for (size_t child_index = 0; child_index < merge_contexts.size(); ++child_index) {
        const auto& ctx = merge_contexts[child_index];
        const uint32_t family_id = families.child_to_family[child_index];
        auto& target = mutable_per_family_maps_for_ctx(lookup_maps, family_id, child_index);

        for (const auto& rowset : ctx.metadata()->rowsets()) {
            // Watermark map: rowset id (catches delete-only rowsets).
            if (auto [it, inserted] = target.watermark_rssid_map.try_emplace(rowset.id(), 0); inserted) {
                ASSIGN_OR_RETURN(it->second, ctx.map_rssid(rowset.id()));
            }
            // Data + watermark maps: per-segment rssids.
            for (int segment_position = 0; segment_position < rowset.segments_size(); ++segment_position) {
                const uint32_t lifted_rssid = get_rssid(rowset, segment_position);
                auto [it, inserted] = target.data_rssid_map.try_emplace(lifted_rssid, 0);
                if (!inserted) continue;
                ASSIGN_OR_RETURN(it->second, ctx.map_rssid(lifted_rssid));
                // Watermark map: only record if not already present (first
                // emitter wins, matching merge_rowsets canonical-id semantics).
                target.watermark_rssid_map.try_emplace(lifted_rssid, it->second);
            }
        }
    }
    return lookup_maps;
}

// True iff two PB instances of the same-filename shared sstable describe the
// same physical file AND have identical projection-affecting fields. The
// projection fields (rssid_offset, shared_rssid, shared_version) are included
// because they change how stored bytes are interpreted at read time —
// keeping the first sibling's PB on a mismatch would silently mis-map the
// rebuilt or projected output. SPLIT today copies metadata as-is so siblings
// agree on every field; this is defense-in-depth against future MERGE input
// topologies. fileset_id is intentionally excluded: it can be synthesized
// per-load by PersistentIndexSstableFileset::init when the source has none
// (PR #72031).
bool shared_sstable_metadata_matches(const PersistentIndexSstablePB& a, const PersistentIndexSstablePB& b) {
    if (a.filesize() != b.filesize() || a.encryption_meta() != b.encryption_meta()) return false;
    if (a.has_range() != b.has_range()) return false;
    if (a.has_range() &&
        (a.range().start_key() != b.range().start_key() || a.range().end_key() != b.range().end_key())) {
        return false;
    }
    if (a.rssid_offset() != b.rssid_offset()) return false;
    if (a.has_shared_rssid() != b.has_shared_rssid()) return false;
    if (a.has_shared_rssid() && a.shared_rssid() != b.shared_rssid()) return false;
    if (a.has_shared_version() != b.has_shared_version()) return false;
    if (a.has_shared_version() && a.shared_version() != b.shared_version()) return false;
    return true;
}

// Defense-in-depth invariants for the legacy-shared form. The merge_sstables
// caller branch already gates on (shared && !has_shared_rssid), but rebuild
// may be entered through other paths in the future.
Status validate_legacy_shared_sstable_form(const PersistentIndexSstablePB& src_pb) {
    if (src_pb.has_shared_rssid()) {
        return Status::Corruption("rebuild_legacy_shared_sstable called on a non-legacy sstable (has_shared_rssid)");
    }
    if (src_pb.has_shared_version() && src_pb.shared_version() > 0) {
        // multi_get DCHECKs has_shared_rssid when shared_version > 0
        // (persistent_index_sstable.cpp:211). A legacy file with shared_version > 0
        // is malformed and cannot be safely rebuilt without materializing that
        // version into every entry.
        return Status::Corruption("Legacy shared sstable has shared_version > 0 without has_shared_rssid");
    }
    if (src_pb.has_delvec() && src_pb.delvec().size() > 0) {
        return Status::Corruption("Legacy shared sstable carries a delvec; cannot rebuild without shared_rssid");
    }
    return Status::OK();
}

// Project src_pb.max_rss_rowid through the rebuild. Returns the initial
// encoded watermark for the rebuilt sstable, or std::nullopt when projection
// cannot be done (out-of-range lifted high, or watermark map miss). Lets the
// rebuilt file inherit a sensible max even when it ends up tombstone-only or
// when its source max points at a delete-only rowset.
// Convention: PersistentIndexSstablePB.max_rss_rowid.high is the EFFECTIVE
// (post-projection) max rssid in the SOURCE child's id space — i.e. it
// already includes any accumulated src_pb.rssid_offset. Cross-sstable
// invariants in lake_persistent_index.cpp (the I1 ordering check at
// apply_opcompaction line 875-886, the _memtable seed at line 146-160,
// the post-compaction validation at line 686-689) all read max_rss_rowid
// as effective rssid, and project_non_shared_legacy_sstable already shifts
// max_rss_rowid.high by ctx.rssid_offset to maintain that convention.
//
// Therefore the source rssid offset must NOT be added again here; doing
// so would double-shift for any stacked-offset src (anything that has
// gone through one round of project_non_shared). The lookup key is just
// source_max_rssid_high directly — it is the source child's effective
// max rssid, exactly the key shape watermark_rssid_map records.
std::optional<uint64_t> project_source_max_rss_rowid(
        const PersistentIndexSstablePB& src_pb, int32_t /*source_rssid_offset*/,
        const std::unordered_map<uint32_t, uint32_t>& watermark_rssid_map) {
    const uint64_t source_max_rowid_low = extract_rss_rowid_low(src_pb.max_rss_rowid());
    const uint32_t source_max_rssid_high = extract_rss_rowid_high(src_pb.max_rss_rowid());
    auto entry = watermark_rssid_map.find(source_max_rssid_high);
    if (entry == watermark_rssid_map.end()) {
        return std::nullopt;
    }
    return encode_rss_rowid(entry->second, source_max_rowid_low);
}

// Walk |values_pb|'s non-tombstone entries and update |*max_encoded| /
// |*initialized| with the encoded `(rssid<<32)|rowid` if larger.
void update_max_encoded_rss_rowid_from(const IndexValuesWithVerPB& values_pb, uint64_t* max_encoded,
                                       bool* initialized) {
    for (int i = 0; i < values_pb.values_size(); ++i) {
        const auto& index_value = values_pb.values(i);
        if (is_index_tombstone(index_value)) continue;
        const uint64_t encoded = encode_rss_rowid(index_value.rssid(), index_value.rowid());
        if (!*initialized || encoded > *max_encoded) {
            *max_encoded = encoded;
            *initialized = true;
        }
    }
}

// Remap stored rssids on each non-tombstone value via the data map (lifted by
// |source_rssid_offset|) and, on the first non-tombstone, check the rowid
// against the per-final-rssid delvec via |load_del_vector|. The rowid is
// shared across versions within an IndexValuesWithVerPB (same row, multiple
// versions) — see KeyValueMerger and PersistentIndexSstable::multi_get for
// the same invariant.
//
// Tombstones (sentinel rssid/rowid = UINT32_MAX) are preserved as-is.
//
// Returns:
//   true  → keep this entry; rssids in |*values_pb| have been rewritten.
//   false → drop this entry (dead source rowset or rowid in delvec).
//   error → corruption (out-of-range stored rssid) or delvec load failure.
// Owns the mutable resources used while writing a rebuilt PK sstable. The
// caller arms a DeferOp that calls delete_partial_legacy_rebuild_output() on
// any path that doesn't go through finalize_legacy_rebuild_output().
struct LegacyRebuildOutputWriter {
    std::string filename;
    std::string location;
    std::string encryption_meta;
    std::unique_ptr<WritableFile> writable_file;
    std::unique_ptr<sstable::FilterPolicy> filter_policy;
    std::unique_ptr<sstable::TableBuilder> table_builder;
};

StatusOr<LegacyRebuildOutputWriter> open_legacy_rebuild_output(TabletManager* tablet_manager,
                                                               int64_t merged_tablet_id) {
    LegacyRebuildOutputWriter writer;
    writer.filename = gen_sst_filename();
    writer.location = tablet_manager->sst_location(merged_tablet_id, writer.filename);
    WritableFileOptions write_options;
    if (config::enable_transparent_data_encryption) {
        ASSIGN_OR_RETURN(auto encryption_pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
        write_options.encryption_info = encryption_pair.info;
        writer.encryption_meta = std::move(encryption_pair.encryption_meta);
    }
    ASSIGN_OR_RETURN(writer.writable_file, fs::new_writable_file(write_options, writer.location));
    sstable::Options builder_options;
    writer.filter_policy.reset(const_cast<sstable::FilterPolicy*>(sstable::NewBloomFilterPolicy(10)));
    builder_options.filter_policy = writer.filter_policy.get();
    writer.table_builder = std::make_unique<sstable::TableBuilder>(builder_options, writer.writable_file.get());
    return writer;
}

// Finalize the table builder, close the writer, and populate |out_pb| with a
// non-shared sstable PB pointing at the just-written physical file. A fresh
// fileset_id is assigned because PersistentIndexSstableFileset::init(vector)
// DCHECKs has_fileset_id() for any sstable with a range
// (persistent_index_sstable_fileset.cpp:30-31), and the rebuilt PB carries a
// real key range from builder.KeyRange().
Status finalize_legacy_rebuild_output(LegacyRebuildOutputWriter& writer, uint64_t max_rss_rowid,
                                      PersistentIndexSstablePB* out_pb) {
    RETURN_IF_ERROR(writer.table_builder->Finish());
    auto [start_key, end_key] = writer.table_builder->KeyRange();
    const uint64_t filesize = writer.table_builder->FileSize();
    RETURN_IF_ERROR(writer.writable_file->close());

    out_pb->set_filename(writer.filename);
    out_pb->set_filesize(filesize);
    out_pb->set_encryption_meta(writer.encryption_meta);
    out_pb->mutable_range()->set_start_key(start_key.to_string());
    out_pb->mutable_range()->set_end_key(end_key.to_string());
    out_pb->set_shared(false);
    out_pb->set_rssid_offset(0);
    out_pb->set_max_rss_rowid(max_rss_rowid);
    out_pb->mutable_fileset_id()->CopyFrom(UniqueId::gen_uid().to_proto());
    return Status::OK();
}

// Best-effort cleanup of an unfinalized partial output: close any writer
// handle and delete the file at the recorded location. Used by the rebuild
// cleanup guard so failure paths and "every entry dropped" don't leak OSS
// orphans.
void delete_partial_legacy_rebuild_output(LegacyRebuildOutputWriter& writer) {
    if (writer.writable_file) {
        (void)writer.writable_file->close();
    }
    auto filesystem_or = FileSystemFactory::CreateSharedFromString(writer.location);
    if (filesystem_or.ok()) {
        (void)(*filesystem_or)->delete_file(writer.location);
    }
}

StatusOr<bool> remap_legacy_entry_or_drop(IndexValuesWithVerPB* values_pb, int32_t source_rssid_offset,
                                          const std::unordered_map<uint32_t, uint32_t>& data_rssid_map,
                                          const std::function<StatusOr<DelVectorPtr>(uint32_t)>& load_del_vector) {
    bool delvec_already_checked = false;
    for (int i = 0; i < values_pb->values_size(); ++i) {
        auto* index_value = values_pb->mutable_values(i);
        if (is_index_tombstone(*index_value)) continue;
        const int64_t lifted_rssid = static_cast<int64_t>(index_value->rssid()) + source_rssid_offset;
        if (lifted_rssid < 0 || lifted_rssid > std::numeric_limits<uint32_t>::max()) {
            return Status::Corruption(fmt::format(
                    "legacy sstable stored rssid out of range after applying source offset: stored={} offset={}",
                    index_value->rssid(), source_rssid_offset));
        }
        auto mapped_entry = data_rssid_map.find(static_cast<uint32_t>(lifted_rssid));
        if (mapped_entry == data_rssid_map.end()) {
            return false; // dead source rowset
        }
        index_value->set_rssid(mapped_entry->second);
        if (!delvec_already_checked) {
            ASSIGN_OR_RETURN(auto del_vector, load_del_vector(mapped_entry->second));
            if (del_vector && del_vector->roaring() && del_vector->roaring()->contains(index_value->rowid())) {
                return false; // rowid filtered by merged delvec
            }
            delvec_already_checked = true;
        }
    }
    return true;
}

// Try to project an ancestor-inherited shared PK sstable (shared=true,
// !has_shared_rssid) without reading the source file. canonical_offset is
// read from plan.family_legacy_sstable_offset; the dense walk verifies
// data_rssid_map[lifted_rssid] == lifted_rssid + canonical_offset and that
// the merged delvec at the final rssid is empty. Emit accumulates:
//   out_pb.rssid_offset       = src_pb.rssid_offset + canonical_offset
//   out_pb.max_rss_rowid.high = source_lifted_max + canonical_offset
//
// Returns true with out_pb filled when every safety check passes. On any
// miss returns false and the caller falls back to
// rebuild_legacy_shared_sstable; each fallback path increments a dedicated
// counter so production can break down why fast-path missed.
StatusOr<bool> try_fastpath_project_legacy_shared_sstable(const PersistentIndexSstablePB& src_pb, uint32_t family_id,
                                                          const detail::RssidProjectionPlan& plan,
                                                          const TabletMetadataPB& new_metadata,
                                                          const LegacyRssidLookupMaps::PerFamilyMaps& rssid_lookup_maps,
                                                          PersistentIndexSstablePB* out_pb) {
    constexpr uint32_t kFastPathMaxRssidSpan = 8192;

    if (family_id == detail::InferredSplitFamilies::kNoFamily) {
        g_tablet_merge_legacy_sstable_fastpath_fallback_no_family << 1;
        return false;
    }
    if (plan.unsafe_families.count(family_id) > 0) {
        g_tablet_merge_legacy_sstable_fastpath_fallback_unsafe_family << 1;
        return false;
    }
    auto family_offset_it = plan.family_legacy_sstable_offset.find(family_id);
    if (family_offset_it == plan.family_legacy_sstable_offset.end()) {
        g_tablet_merge_legacy_sstable_fastpath_fallback_no_family << 1;
        return false;
    }
    const int64_t canonical_offset = family_offset_it->second;

    // Form / range / fileset_id gates: identical legality contract to v1.
    // Rebuild's PersistentIndexSstableFileset::init(vector) DCHECK requires
    // a fileset_id on any ranged sstable, so the fast-path must too.
    if (!validate_legacy_shared_sstable_form(src_pb).ok()) {
        g_tablet_merge_legacy_sstable_fastpath_fallback_invalid_form << 1;
        return false;
    }
    if (!new_metadata.has_range()) {
        g_tablet_merge_legacy_sstable_fastpath_fallback_merged_no_range << 1;
        return false;
    }
    if (src_pb.has_range() && !src_pb.has_fileset_id()) {
        g_tablet_merge_legacy_sstable_fastpath_fallback_ranged_no_fileset_id << 1;
        return false;
    }

    // Boundary / overflow checks. accumulated_offset must fit in int32 so
    // it can be written back to the PB; new_lifted_max must fit in uint32
    // so the high word is well-defined.
    const int64_t accumulated_offset = static_cast<int64_t>(src_pb.rssid_offset()) + canonical_offset;
    if (accumulated_offset < std::numeric_limits<int32_t>::min() ||
        accumulated_offset > std::numeric_limits<int32_t>::max()) {
        g_tablet_merge_legacy_sstable_fastpath_fallback_coverage_span_invalid << 1;
        return false;
    }
    // max_rss_rowid.high is already in source-effective space (post-PR0).
    const int64_t source_lifted_max = static_cast<int64_t>(extract_rss_rowid_high(src_pb.max_rss_rowid()));
    const int64_t source_lifted_lower = static_cast<int64_t>(src_pb.rssid_offset()) + 1; // skip stored rssid 0
    if (source_lifted_max < source_lifted_lower) {
        g_tablet_merge_legacy_sstable_fastpath_fallback_coverage_span_invalid << 1;
        return false;
    }
    if (source_lifted_max - source_lifted_lower + 1 > kFastPathMaxRssidSpan) {
        g_tablet_merge_legacy_sstable_fastpath_fallback_coverage_span_invalid << 1;
        return false;
    }
    const int64_t new_lifted_max = source_lifted_max + canonical_offset;
    if (new_lifted_max < 0 || new_lifted_max > static_cast<int64_t>(std::numeric_limits<uint32_t>::max())) {
        g_tablet_merge_legacy_sstable_fastpath_fallback_coverage_span_invalid << 1;
        return false;
    }

    // Dense walk of [source_lifted_lower, source_lifted_max] in source-
    // effective (canonical_ctx-local) space. For each lifted_rssid, expect:
    //   data_rssid_map[lifted_rssid] == lifted_rssid + canonical_offset
    // and the merged delvec at the FINAL rssid is empty (fast-path can't
    // physically drop entries).
    const auto& family_data_rssid_map = rssid_lookup_maps.data_rssid_map;
    const auto& merged_delvecs = new_metadata.delvec_meta().delvecs();
    for (int64_t lifted_rssid_64 = source_lifted_lower; lifted_rssid_64 <= source_lifted_max; ++lifted_rssid_64) {
        const auto lifted_rssid = static_cast<uint32_t>(lifted_rssid_64);
        const auto expected_final = static_cast<uint32_t>(lifted_rssid_64 + canonical_offset);
        auto data_entry = family_data_rssid_map.find(lifted_rssid);
        if (data_entry == family_data_rssid_map.end() || data_entry->second != expected_final) {
            g_tablet_merge_legacy_sstable_fastpath_fallback_partial_compaction << 1;
            return false;
        }
        auto delvec_entry = merged_delvecs.find(expected_final);
        if (delvec_entry != merged_delvecs.end() && delvec_entry->second.size() > 0) {
            g_tablet_merge_legacy_sstable_fastpath_fallback_merged_delvec_nonempty << 1;
            return false;
        }
    }

    // Emit projected PB. PB.rssid_offset accumulates; max_rss_rowid.high
    // (in effective space) shifts by canonical_offset. low word and every
    // other field copy through unchanged.
    out_pb->CopyFrom(src_pb);
    out_pb->set_rssid_offset(static_cast<int32_t>(accumulated_offset));
    out_pb->set_max_rss_rowid(
            encode_rss_rowid(static_cast<uint32_t>(new_lifted_max), extract_rss_rowid_low(src_pb.max_rss_rowid())));
    return true;
}

// Rebuilds an ancestor-inherited shared PK sstable (shared=true,
// !has_shared_rssid) by reading every entry, remapping stored rssids to the
// merged tablet's final rssid space via merge_contexts, applying the merged
// tablet's range filter and per-rssid delvec, and writing a fresh non-shared
// sstable.
//
// Why this is needed (run4 ghost-rssid bug): the legacy projection path only
// accumulates a single rssid_offset per PB, but a single PK sstable file holds
// entries for many ancestor rowsets. After multi-cycle split/merge with partial
// child compaction, a surviving rowset can be assigned a NEW id by add_rowset
// in some merge while the inherited sstable still stores the OLD id. PK lookup
// then returns a stored rssid that no longer corresponds to any live rowset
// in the merged tablet — `unexpected segment id` at publish time.
//
// Five filtering layers (all needed to match modern shared_rssid path semantics
// once we emit a non-shared file):
//   (1) Tablet-range filter via TabletRangeHelper::create_sst_seek_range_from
//       — drops ancestor keys outside the merged tablet range. The modern
//       shared path gets this at PK index init via the contain_shared_sstables
//       gate (lake_persistent_index.cpp:624-635). A non-shared rebuilt output
//       must apply it inline.
//   (2) Per-entry rssid remap via the precomputed data_rssid_map (built once
//       by build_legacy_rssid_lookup_maps) — drops entries whose source rowset
//       is dead in every child.
//   (3) Per-entry delvec filter via new_metadata.delvec_meta() — drops entries
//       whose rowid is in the merged delvec, which by Phase 5 of merge_delvecs
//       includes both real deletes and synthesized gap-bits for shared segments
//       not covered by any contributing child's range.
//   (4) Watermark projection via the watermark_rssid_map (superset of the data
//       map; also covers delete-only rowsets) — sets max_rss_rowid even when
//       the file is tombstone-only or its source max points at a delete-only
//       rowset.
//   (5) Tombstones preserved as-is (rssid/rowid sentinel; never remapped or
//       delvec-filtered) — same invariant as multi_get and KeyValueMerger.
//
// On success out_pb is filled with a non-shared sstable PB pointing at a new
// physical file. If every entry was dropped (dead rowset / out-of-range / in
// delvec), out_pb is left empty and the caller should drop the sstable entirely
// from the merged metadata; the cleanup guard deletes the partial output file
// before this function returns so we don't leave OSS orphans for vacuum.
Status rebuild_legacy_shared_sstable(TabletManager* tablet_manager, int64_t merged_tablet_id,
                                     const PersistentIndexSstablePB& src_pb, const TabletMetadataPtr& src_metadata,
                                     const TabletMetadataPB& new_metadata,
                                     const LegacyRssidLookupMaps::PerFamilyMaps& rssid_lookup_maps,
                                     PersistentIndexSstablePB* out_pb) {
    out_pb->Clear();
    RETURN_IF_ERROR(validate_legacy_shared_sstable_form(src_pb));

    // The merged tablet must carry a range for shared-data range distribution.
    // Without it the (1) tablet-range filter below cannot be applied; refuse
    // rather than silently dropping it.
    if (!new_metadata.has_range()) {
        return Status::InternalError("merged tablet has no range; cannot apply range filter to legacy rebuild");
    }

    // Open the source iterator and Seek to the merged tablet range start.
    // (1) Tablet-range filter — mirrors the contain_shared_sstables gate at
    // lake_persistent_index.cpp:624-635. Modern shared sstables get this at PK
    // index init; the rebuilt output is shared=false, so we apply it inline.
    ASSIGN_OR_RETURN(auto source_sstable,
                     PersistentIndexSstable::new_sstable(
                             src_pb, tablet_manager->sst_location(src_metadata->id(), src_pb.filename()),
                             /*cache=*/nullptr, /*need_filter=*/false,
                             /*delvec=*/nullptr, src_metadata, tablet_manager));
    sstable::ReadOptions source_read_options;
    source_read_options.fill_cache = false;
    std::unique_ptr<sstable::Iterator> source_iterator(source_sstable->new_iterator(source_read_options));
    auto merged_tablet_schema = TabletSchema::create(new_metadata.schema());
    ASSIGN_OR_RETURN(auto seek_range,
                     TabletRangeHelper::create_sst_seek_range_from(new_metadata.range(), merged_tablet_schema));
    if (seek_range.seek_key.empty()) {
        source_iterator->SeekToFirst();
    } else {
        source_iterator->Seek(seek_range.seek_key);
    }
    const sstable::Comparator* bytewise_comparator = sstable::BytewiseComparator();

    // Maps are built once per merge_sstables() call and shared with the fast-path
    // helper; both paths consult the same data + watermark mappings, so building
    // them at the merge_sstables top avoids the quadratic walk that would
    // otherwise scan merge_contexts × rowsets × segments per stored entry.

    // Open the output writer and arm a cleanup guard so any failure path —
    // parse error, delvec load, builder Add, builder Finish, close — and the
    // "every entry dropped" path leave no OSS orphan. cancel() is called only
    // after the output PB is fully built and ready to swap into the merged
    // metadata.
    ASSIGN_OR_RETURN(auto output_writer, open_legacy_rebuild_output(tablet_manager, merged_tablet_id));
    CancelableDefer cleanup_partial_output([&] { delete_partial_legacy_rebuild_output(output_writer); });

    // Stored rssids in the file are in pre-projection space. The read path at
    // persistent_index_sstable.cpp:222-225 shifts them by src_pb.rssid_offset()
    // to derive the effective rssid in the current child's id space. Apply the
    // same shift before lookup so a stacked-offset legacy sstable still
    // resolves correctly.
    const int32_t source_rssid_offset = src_pb.rssid_offset();

    // (4) Project src.max_rss_rowid through the rebuild before the scan so
    // tombstone-only files and files whose source max points at a delete-only
    // rowset still get a stable watermark.
    uint64_t max_encoded_rss_rowid = 0;
    bool max_encoded_initialized = false;
    if (auto initial_max =
                project_source_max_rss_rowid(src_pb, source_rssid_offset, rssid_lookup_maps.watermark_rssid_map)) {
        max_encoded_rss_rowid = *initial_max;
        max_encoded_initialized = true;
    }

    // (3) Per-rssid delvec cache. fill_cache=false / fill_data_cache=false so
    // a one-shot bulk merge scan doesn't pollute long-lived block / delvec
    // caches; the local cache here already deduplicates per-rssid loads.
    std::unordered_map<uint32_t, DelVectorPtr> del_vector_cache;
    auto load_del_vector = [&](uint32_t final_rssid) -> StatusOr<DelVectorPtr> {
        auto cached_entry = del_vector_cache.find(final_rssid);
        if (cached_entry != del_vector_cache.end()) return cached_entry->second;
        const auto& delvecs_by_rssid = new_metadata.delvec_meta().delvecs();
        auto delvec_page_entry = delvecs_by_rssid.find(final_rssid);
        if (delvec_page_entry == delvecs_by_rssid.end() || delvec_page_entry->second.size() == 0) {
            del_vector_cache.emplace(final_rssid, DelVectorPtr{});
            return DelVectorPtr{};
        }
        DelVectorPtr del_vector = std::make_shared<DelVector>();
        LakeIOOptions lake_io_options{.fill_data_cache = false, .skip_disk_cache = false};
        RETURN_IF_ERROR(lake::get_del_vec(tablet_manager, new_metadata, delvec_page_entry->second,
                                          /*fill_cache=*/false, lake_io_options, del_vector.get()));
        del_vector_cache.emplace(final_rssid, del_vector);
        return del_vector;
    };

    uint64_t kept_entry_count = 0;
    uint64_t dropped_entry_count = 0;
    for (; source_iterator->Valid(); source_iterator->Next()) {
        const Slice entry_key = source_iterator->key();
        if (!seek_range.stop_key.empty() && bytewise_comparator->Compare(entry_key, Slice(seek_range.stop_key)) >= 0) {
            break; // (1) past merged tablet range upper bound
        }
        const Slice entry_raw_value = source_iterator->value();
        IndexValuesWithVerPB values_pb;
        if (!values_pb.ParseFromArray(entry_raw_value.data, static_cast<int>(entry_raw_value.size))) {
            return Status::InternalError("Failed to parse legacy sstable value during rebuild");
        }
        // (2) + (3) per-entry remap and delvec filter, packed into one helper.
        ASSIGN_OR_RETURN(bool keep_entry,
                         remap_legacy_entry_or_drop(&values_pb, source_rssid_offset, rssid_lookup_maps.data_rssid_map,
                                                    load_del_vector));
        if (!keep_entry) {
            ++dropped_entry_count;
            continue;
        }
        update_max_encoded_rss_rowid_from(values_pb, &max_encoded_rss_rowid, &max_encoded_initialized);
        const std::string serialized_entry = values_pb.SerializeAsString();
        RETURN_IF_ERROR(output_writer.table_builder->Add(entry_key, Slice(serialized_entry)));
        ++kept_entry_count;
    }
    RETURN_IF_ERROR(source_iterator->status());

    if (dropped_entry_count > 0) {
        g_tablet_merge_legacy_sstable_rebuild_dropped_entries << static_cast<int64_t>(dropped_entry_count);
    }

    if (kept_entry_count == 0) {
        // Every entry was dropped (dead rowset / out-of-range / in delvec).
        // The cleanup guard deletes the partial output file; signal to the
        // caller that the sstable should be dropped from the merged metadata
        // entirely.
        return Status::OK();
    }

    RETURN_IF_ERROR(
            finalize_legacy_rebuild_output(output_writer, max_encoded_initialized ? max_encoded_rss_rowid : 0, out_pb));
    cleanup_partial_output.cancel(); // file is now referenced; keep it
    g_tablet_merge_legacy_sstable_rebuild_total << 1;
    return Status::OK();
}

// Emit the projected PB for one ancestor-inherited shared PK sstable into
// |out_pb|. Tries the metadata-only fast-path first; on any safety miss
// falls back to rebuild_legacy_shared_sstable. An empty out_pb after this
// call means rebuild dropped every entry — the caller should NOT emit it
// to the merged sstable_meta.
Status emit_legacy_shared_sstable_via_fastpath_or_rebuild(TabletManager* tablet_manager, int64_t merged_tablet_id,
                                                          const PersistentIndexSstablePB& src_pb,
                                                          const TabletMetadataPtr& src_metadata,
                                                          const TabletMetadataPB& new_metadata, uint32_t family_id,
                                                          const detail::RssidProjectionPlan& plan,
                                                          const LegacyRssidLookupMaps::PerFamilyMaps& rssid_lookup_maps,
                                                          PersistentIndexSstablePB* out_pb) {
    ASSIGN_OR_RETURN(bool fastpath_succeeded,
                     try_fastpath_project_legacy_shared_sstable(src_pb, family_id, plan, new_metadata,
                                                                rssid_lookup_maps, out_pb));
    if (fastpath_succeeded) {
        g_tablet_merge_legacy_sstable_fastpath_total << 1;
        return Status::OK();
    }
    g_tablet_merge_legacy_sstable_fastpath_fallback_to_rebuild_total << 1;
    return rebuild_legacy_shared_sstable(tablet_manager, merged_tablet_id, src_pb, src_metadata, new_metadata,
                                         rssid_lookup_maps, out_pb);
}

// Rebuild a non-shared legacy sstable (shared=false, !has_shared_rssid)
// by reading every entry, remapping stored rssid via ctx.map_rssid (which
// honors the family-canonical projection), and writing a fresh non-shared
// sstable. Used when the source sstable contains entries referencing both
// shared-ancestor and child-local rowsets — typically a post-split PK-index
// compaction output that crossed the shared-ancestor boundary — which a
// single PB-level rssid_offset shift cannot translate uniformly.
//
// Differences from rebuild_legacy_shared_sstable:
//   - SeekToFirst (non-shared sstable was written within this child's
//     range, which is a subset of the merged range; no tablet-range
//     filter needed).
//   - No per-rssid delvec filter (matches v1 project_non_shared_legacy_-
//     sstable; merged delvec is applied by the read path at lookup
//     time, not pre-baked into the projected PB).
//   - Per-entry remap via ctx.map_rssid instead of a family-scoped
//     data_rssid_map (covers both shared-ancestor and child-local
//     rowsets in a single funnel).
//
// On success out_pb carries a non-shared PB pointing at a new file with
// rssid_offset=0 (entries are pre-remapped) and a fresh fileset_id. If
// the source iterator is empty, out_pb stays empty and the caller drops
// the sstable from merged metadata; the cleanup guard removes the
// partial output on every error path.
Status rebuild_non_shared_legacy_sstable(TabletManager* tablet_manager, int64_t merged_tablet_id,
                                         const PersistentIndexSstablePB& src_pb, const TabletMergeContext& ctx,
                                         const TabletMetadataPtr& src_metadata, PersistentIndexSstablePB* out_pb) {
    out_pb->Clear();
    // Mirror project_non_shared_legacy_sstable's existing corruption
    // guard: a non-shared sstable must not carry an embedded delvec —
    // the embedded delvec format only applies to shared_rssid=true PBs.
    if (src_pb.has_delvec() && src_pb.delvec().size() > 0) {
        return Status::Corruption(
                "non-shared sstable has delvec but no shared_rssid, cannot rebuild during tablet merge");
    }

    // Open source iterator FIRST; emptiness must reach the drop-output
    // path without going through map_rssid arithmetic that could error
    // on an unrelated edge case (negative ctx offset + max_rss_rowid==0).
    ASSIGN_OR_RETURN(auto source_sstable,
                     PersistentIndexSstable::new_sstable(
                             src_pb, tablet_manager->sst_location(src_metadata->id(), src_pb.filename()),
                             /*cache=*/nullptr, /*need_filter=*/false,
                             /*delvec=*/nullptr, src_metadata, tablet_manager));
    sstable::ReadOptions source_read_options;
    source_read_options.fill_cache = false;
    std::unique_ptr<sstable::Iterator> source_iterator(source_sstable->new_iterator(source_read_options));
    source_iterator->SeekToFirst();
    if (!source_iterator->Valid()) {
        RETURN_IF_ERROR(source_iterator->status());
        // Zero-entry sstable: leave out_pb empty, caller drops it.
        return Status::OK();
    }

    // Now safe to open the writer + project initial watermark.
    ASSIGN_OR_RETURN(auto output_writer, open_legacy_rebuild_output(tablet_manager, merged_tablet_id));
    CancelableDefer cleanup_partial_output([&] { delete_partial_legacy_rebuild_output(output_writer); });

    const int32_t source_rssid_offset = src_pb.rssid_offset();
    // Initial watermark: project the source max_rss_rowid.high through
    // ctx.map_rssid so tombstone-only files still emit a stable
    // post-merge watermark. Per-entry update_max_encoded_rss_rowid_from
    // only widens this initial value as non-tombstone entries are
    // written.
    const uint32_t source_max_high = extract_rss_rowid_high(src_pb.max_rss_rowid());
    ASSIGN_OR_RETURN(uint32_t projected_max_high, ctx.map_rssid(source_max_high));
    uint64_t max_encoded_rss_rowid =
            encode_rss_rowid(projected_max_high, extract_rss_rowid_low(src_pb.max_rss_rowid()));
    bool max_encoded_initialized = true;

    uint64_t kept_entry_count = 0;
    for (; source_iterator->Valid(); source_iterator->Next()) {
        const Slice entry_key = source_iterator->key();
        const Slice entry_raw_value = source_iterator->value();
        IndexValuesWithVerPB values_pb;
        if (!values_pb.ParseFromArray(entry_raw_value.data, static_cast<int>(entry_raw_value.size))) {
            return Status::InternalError("Failed to parse non-shared sstable value during rebuild");
        }
        for (int j = 0; j < values_pb.values_size(); ++j) {
            auto* index_value = values_pb.mutable_values(j);
            if (is_index_tombstone(*index_value)) continue; // preserve sentinel as-is
            const int64_t lifted_rssid =
                    static_cast<int64_t>(index_value->rssid()) + static_cast<int64_t>(source_rssid_offset);
            if (lifted_rssid < 0 || lifted_rssid > static_cast<int64_t>(std::numeric_limits<uint32_t>::max())) {
                return Status::Corruption(fmt::format(
                        "non-shared rebuild: lifted rssid out of uint32 range: stored={} src_offset={} lifted={}",
                        index_value->rssid(), source_rssid_offset, lifted_rssid));
            }
            ASSIGN_OR_RETURN(uint32_t final_rssid, ctx.map_rssid(static_cast<uint32_t>(lifted_rssid)));
            index_value->set_rssid(final_rssid);
        }
        update_max_encoded_rss_rowid_from(values_pb, &max_encoded_rss_rowid, &max_encoded_initialized);
        const std::string serialized_entry = values_pb.SerializeAsString();
        RETURN_IF_ERROR(output_writer.table_builder->Add(entry_key, Slice(serialized_entry)));
        ++kept_entry_count;
    }
    RETURN_IF_ERROR(source_iterator->status());

    if (kept_entry_count == 0) {
        // Defensive: SeekToFirst returned Valid() above, so this should
        // be unreachable, but covers the case where every Add() somehow
        // got skipped without an error.
        return Status::OK();
    }

    RETURN_IF_ERROR(finalize_legacy_rebuild_output(output_writer, max_encoded_rss_rowid, out_pb));
    cleanup_partial_output.cancel();
    g_tablet_merge_non_shared_sstable_rebuild_total << 1;
    return Status::OK();
}

// Project a sstable that has shared_rssid set (modern shared or
// `ingest_sst()` output). |out| was already CopyFrom'd from |sst|; this
// function rewrites the projection-affected fields in place.
//
// Always re-attaches the merged delvec from |new_metadata->delvec_meta()|
// regardless of whether the source carried one — this is what lets a
// synthesized gap-delvec (created by merge_delvecs Phase 0 for keys covered
// by no contributing child) reach the rebuilt sstable PB. Without it,
// PersistentIndexSstable::multi_get could return stale rssids when the LSM
// block-sort order is inverted.
Status project_modern_shared_rssid_sstable(const PersistentIndexSstablePB& sst, TabletMergeContext& ctx,
                                           const TabletMetadataPB* new_metadata, PersistentIndexSstablePB* out) {
    ASSIGN_OR_RETURN(auto mapped_rssid, ctx.map_rssid(sst.shared_rssid()));
    out->set_shared_rssid(mapped_rssid);
    out->set_rssid_offset(0); // shared_rssid is post-projection; clear to avoid double-transform on read
    out->set_max_rss_rowid(encode_rss_rowid(mapped_rssid, extract_rss_rowid_low(sst.max_rss_rowid())));

    auto delvec_entry = new_metadata->delvec_meta().delvecs().find(mapped_rssid);
    if (delvec_entry != new_metadata->delvec_meta().delvecs().end() && delvec_entry->second.size() > 0) {
        out->mutable_delvec()->CopyFrom(delvec_entry->second);
    } else if (sst.has_delvec() && sst.delvec().size() > 0) {
        return Status::Corruption("Delvec page not found for sstable after merge");
    }
    return Status::OK();
}

// Project a non-shared sstable without shared_rssid: a child-local file
// produced by flush_pk_memtable for THIS merge round, or a rebuilt legacy
// file from a prior merge. Stored rssids already live in the child's id
// space, so a single ctx.rssid_offset() shift suffices. The accumulation
// preserves correctness when the file already carries a non-zero
// rssid_offset (stacked merge case).
Status project_non_shared_legacy_sstable(const PersistentIndexSstablePB& sst, const TabletMergeContext& ctx,
                                         PersistentIndexSstablePB* out) {
    if (sst.has_delvec() && sst.delvec().size() > 0) {
        return Status::Corruption("Sstable has delvec but no shared_rssid, cannot project delvec");
    }
    const int64_t accumulated_offset = static_cast<int64_t>(sst.rssid_offset()) + ctx.rssid_offset();
    if (accumulated_offset < std::numeric_limits<int32_t>::min() ||
        accumulated_offset > std::numeric_limits<int32_t>::max()) {
        return Status::Corruption(
                fmt::format("accumulated rssid_offset exceeds int32 range: sst_offset={} ctx_offset={} sum={}",
                            sst.rssid_offset(), ctx.rssid_offset(), accumulated_offset));
    }
    out->set_rssid_offset(static_cast<int32_t>(accumulated_offset));
    const int64_t high = static_cast<int64_t>(extract_rss_rowid_high(sst.max_rss_rowid()));
    const int64_t new_high = high + ctx.rssid_offset();
    if (new_high < 0 || new_high > std::numeric_limits<uint32_t>::max()) {
        return Status::Corruption(
                fmt::format("rssid high overflow in merge projection: high={} ctx_offset={} new_high={}", high,
                            ctx.rssid_offset(), new_high));
    }
    out->set_max_rss_rowid(
            encode_rss_rowid(static_cast<uint32_t>(new_high), extract_rss_rowid_low(sst.max_rss_rowid())));
    out->clear_delvec();
    return Status::OK();
}

// Two invariants must hold on the emitted sstable_meta after merge_sstables's
// per-ctx projection:
//
//   (I1) Global signed-monotone non-decreasing max_rss_rowid.
//        LakePersistentIndex::commit (lake_persistent_index.cpp:907-911)
//        fetches max_rss_rowid into an int64_t and rejects last > cur with
//        "sstables are not ordered". The signed comparison matters when the
//        encoded (rssid<<32|rowid) sets the high bit (rssid >= 2^31 or
//        memtable_max set to (rowset_id<<32|UINT32_MAX) per
//        persistent_index_memtable.cpp:110) — unsigned ordering would be
//        the reverse of signed against any low-rssid sibling.
//
//   (I2) Same fileset_id sstables are contiguous in metadata order.
//        LakePersistentIndex::init (lake_persistent_index.cpp:131-145) groups
//        sstables into PersistentIndexSstableFileset by consecutive equal
//        fileset_id. lake_persistent_index_size_tiered_compaction_strategy
//        .cpp:83-87 rejects non-contiguous reuse with "inconsistent
//        fileset_id in sstables", and apply_opcompaction's contiguous-range
//        find_if (lake_persistent_index.cpp:865-885) erases the wrong span
//        otherwise.
//
// Source of conflict between I1 and I2: PersistentIndexSstableFileset::
// append() (persistent_index_sstable_fileset.cpp:96-115) lets a freshly-
// flushed sstable inherit an existing fileset's _fileset_id whenever the
// new sstable's key range strictly extends the existing range. In a
// multi-cycle SPLIT/MERGE flow, flush_pk_memtable on each merge context can
// add a per-child sstable that inherits the same source FID-X from a long-
// lived shared sstable. After projection these per-child flushes spread
// across a wide max_rss_rowid range intermixed with other-FID compaction
// outputs, so a single sort by max_rss_rowid necessarily produces multiple
// non-contiguous runs of FID-X.
//
// Resolution: stable_sort by signed max_rss_rowid (satisfies I1), then walk
// the sorted output and detect when a fileset_id reappears after at least
// one other-FID sstable has closed its earlier run. Any such later run
// cannot share a logical fileset with the earlier one — there is at least
// one foreign sstable physically between them in metadata, so init() /
// pick_compaction_candidates / apply_opcompaction would have to treat them
// as separate filesets anyway. Assign a fresh fileset_id (UniqueId::
// gen_uid) to each later run so I2 is satisfied without sacrificing I1.
// Sstables with no fileset_id remain singletons and close any open run;
// they are not grouped with anything.
void reassign_fileset_ids_for_ordered_runs(google::protobuf::RepeatedPtrField<PersistentIndexSstablePB>* dest) {
    std::vector<PersistentIndexSstablePB> sorted;
    sorted.reserve(dest->size());
    for (const auto& sst : *dest) {
        sorted.emplace_back(sst);
    }
    std::stable_sort(sorted.begin(), sorted.end(),
                     [](const PersistentIndexSstablePB& a, const PersistentIndexSstablePB& b) {
                         return static_cast<int64_t>(a.max_rss_rowid()) < static_cast<int64_t>(b.max_rss_rowid());
                     });

    std::unordered_set<UniqueId> closed_orig_fids;
    UniqueId current_run_orig;
    UniqueId current_run_emitted;
    bool has_run = false;
    for (auto& sst : sorted) {
        if (!sst.has_fileset_id()) {
            // No fileset_id (legacy / standalone): cannot be grouped. Close
            // any open run so a same-FID sstable after this one will be
            // treated as a non-contiguous reuse.
            if (has_run) {
                closed_orig_fids.insert(current_run_orig);
                has_run = false;
            }
            continue;
        }
        UniqueId orig(sst.fileset_id());
        if (has_run && orig == current_run_orig) {
            // Continuation of the current run: emit with this run's FID
            // (which may be the original or a freshly-assigned one).
            sst.mutable_fileset_id()->CopyFrom(current_run_emitted.to_proto());
            continue;
        }
        // Run boundary.
        if (has_run) {
            closed_orig_fids.insert(current_run_orig);
        }
        UniqueId emitted = (closed_orig_fids.count(orig) > 0) ? UniqueId::gen_uid() : orig;
        sst.mutable_fileset_id()->CopyFrom(emitted.to_proto());
        current_run_orig = orig;
        current_run_emitted = emitted;
        has_run = true;
    }

    dest->Clear();
    for (auto& sst : sorted) {
        *dest->Add() = std::move(sst);
    }
}

// Source-effective rssid range that a sstable's stored entries could
// reference, clamped to the uint32_t key space the per-ctx
// disagreement-key vector indexes by. Both bounds are inclusive.
//
// The signed arithmetic matters because compute_rssid_offset can
// produce a negative ctx.rssid_offset() when ctx[N>0]'s input rowset
// ids exceed ctx[0]'s next_rowset_id (legal — exercised by
// test_tablet_merging_accumulates_stacked_rssid_offset). Casting a
// negative low to uint32_t directly would wrap to a huge value and
// silently miss in-range disagreement keys.
struct ClampedLiftedRange {
    uint32_t lower = 0;
    uint32_t upper = 0;
    bool valid() const { return lower <= upper; }
};

ClampedLiftedRange clamp_lifted_range_to_uint32(int32_t source_rssid_offset, uint64_t source_max_rss_rowid_high) {
    constexpr int64_t kUint32Max = std::numeric_limits<uint32_t>::max();
    const int64_t low_signed = static_cast<int64_t>(source_rssid_offset) + 1;
    const int64_t high_signed = static_cast<int64_t>(source_max_rss_rowid_high);
    ClampedLiftedRange range;
    range.lower = static_cast<uint32_t>(std::max<int64_t>(0, low_signed));
    range.upper = (high_signed < 0) ? 0u : static_cast<uint32_t>(std::min(high_signed, kUint32Max));
    return range;
}

// Resolve the family-canonical projection plan for a ctx. Non-PK merges
// leave the plan pointer null; the fast-path emit still wants a plan
// reference, so an empty static instance acts as a safe identity.
const detail::RssidProjectionPlan& projection_plan_or_empty(const TabletMergeContext& ctx) {
    static const detail::RssidProjectionPlan kEmptyPlan{};
    const auto* plan = ctx.projection_plan();
    return (plan != nullptr) ? *plan : kEmptyPlan;
}

// Returns the highest (rowset.id + step + ctx.rssid_offset) seen across
// merge_contexts[0..upper_exclusive), or |base_next_rowset_id| if higher.
// Phase 1 in merge_tablet uses this as the watermark for ctx[i]'s
// rssid_offset assignment so each child's projected rowset ids land
// strictly above every earlier child's projected ids.
//
// Each uint32_t addend is widened to int64_t before the addition to avoid
// uint32_t wrap when rowset.id() approaches UINT32_MAX — wrap there would
// under-estimate the watermark and let later children reuse occupied rssids.
uint32_t compute_cumulative_rowset_id_ceiling(const std::vector<TabletMergeContext>& merge_contexts,
                                              size_t upper_exclusive, uint32_t base_next_rowset_id) {
    uint32_t ceiling = base_next_rowset_id;
    for (size_t j = 0; j < upper_exclusive; ++j) {
        for (const auto& rowset : merge_contexts[j].metadata()->rowsets()) {
            const int64_t end_wide = static_cast<int64_t>(rowset.id()) +
                                     static_cast<int64_t>(get_rowset_id_step(rowset)) +
                                     merge_contexts[j].rssid_offset();
            const uint32_t end =
                    static_cast<uint32_t>(std::clamp<int64_t>(end_wide, 0, std::numeric_limits<uint32_t>::max()));
            ceiling = std::max(ceiling, end);
        }
    }
    return ceiling;
}

// Emit one ancestor-inherited shared PK sstable (shared=true,
// !has_shared_rssid) into dest via the fast-path or its rebuild fallback.
// Resolves family_id, the family's PerFamilyMaps, and the projection plan
// from ctx. Drops sstables whose rebuild emitted no entries (matches the
// cleanup contract documented on rebuild_legacy_shared_sstable).
Status emit_legacy_shared_sstable_into_dest(TabletManager* tablet_manager, const TabletMergeContext& ctx,
                                            size_t child_index, const PersistentIndexSstablePB& src_pb,
                                            const TabletMetadataPB& new_metadata,
                                            const detail::InferredSplitFamilies& families,
                                            const LegacyRssidLookupMaps& rssid_lookup_maps,
                                            ::google::protobuf::RepeatedPtrField<PersistentIndexSstablePB>* dest) {
    const uint32_t family_id = families.child_to_family[child_index];
    ASSIGN_OR_RETURN(const auto* family_maps_ptr, lookup_maps_for_ctx(rssid_lookup_maps, family_id, child_index));
    PersistentIndexSstablePB projected_pb;
    RETURN_IF_ERROR(emit_legacy_shared_sstable_via_fastpath_or_rebuild(
            tablet_manager, new_metadata.id(), src_pb, ctx.metadata(), new_metadata, family_id,
            projection_plan_or_empty(ctx), *family_maps_ptr, &projected_pb));
    if (!projected_pb.filename().empty()) {
        dest->Add()->Swap(&projected_pb);
    }
    return Status::OK();
}

// Emit one non-shared legacy PK sstable (shared=false, !has_shared_rssid)
// into dest. Routes through the per-entry rebuild path when the projection
// plan would map some referenced rssid to a value that disagrees with the
// natural ctx.rssid_offset shift; otherwise the metadata-only
// project_non_shared_legacy_sstable path applies.
Status emit_non_shared_legacy_sstable_into_dest(TabletManager* tablet_manager, const TabletMergeContext& ctx,
                                                const PersistentIndexSstablePB& src_pb,
                                                const TabletMetadataPB& new_metadata,
                                                const std::vector<uint32_t>& ctx_disagreement_keys,
                                                ::google::protobuf::RepeatedPtrField<PersistentIndexSstablePB>* dest) {
    const auto lifted_range =
            clamp_lifted_range_to_uint32(src_pb.rssid_offset(), extract_rss_rowid_high(src_pb.max_rss_rowid()));
    const bool needs_rebuild = lifted_range.valid() && !ctx_disagreement_keys.empty() &&
                               TabletMergeContext::mapping_disagrees_with_natural_in_range(
                                       ctx_disagreement_keys, lifted_range.lower, lifted_range.upper);
    if (needs_rebuild) {
        PersistentIndexSstablePB rebuilt_pb;
        RETURN_IF_ERROR(rebuild_non_shared_legacy_sstable(tablet_manager, new_metadata.id(), src_pb, ctx,
                                                          ctx.metadata(), &rebuilt_pb));
        if (!rebuilt_pb.filename().empty()) {
            dest->Add()->Swap(&rebuilt_pb);
        }
        return Status::OK();
    }
    auto* out = dest->Add();
    out->CopyFrom(src_pb);
    return project_non_shared_legacy_sstable(src_pb, ctx, out);
}

Status merge_sstables(TabletManager* tablet_manager, std::vector<TabletMergeContext>& merge_contexts,
                      TabletMetadataPB* new_metadata) {
    auto* dest = new_metadata->mutable_sstable_meta()->mutable_sstables();
    // Tracks shared sstables by filename so subsequent occurrences across child
    // contexts are deduped + consistency-checked against the original source PB.
    // We cache the SOURCE PB (not an index into dest) because the legacy-shared
    // rebuild path can either replace the emitted PB with one that has a
    // different filename/filesize, or drop it from dest entirely — either of
    // which would invalidate an index-based cache.
    std::unordered_map<std::string, PersistentIndexSstablePB> shared_dedup_sources;

    auto* update_manager = tablet_manager->update_mgr();

    // PK-index memtable flush has to run first (below) before the lookup maps
    // can be built — flushed metadata may add new rowsets the legacy path needs
    // to remap into. Build the maps lazily once after the flush phase, and
    // reuse them for both the fast-path and the rebuild fallback.
    bool rssid_lookup_maps_initialized = false;
    LegacyRssidLookupMaps rssid_lookup_maps;
    detail::InferredSplitFamilies inferred_families;
    auto ensure_rssid_lookup_maps = [&]() -> Status {
        if (rssid_lookup_maps_initialized) return Status::OK();
        // Family inference and lookup-map population must consume the same
        // merge_contexts snapshot so per-ctx child_index → family_id mapping
        // remains in lockstep with per-ctx contributions to the per-family
        // PerFamilyMaps.
        std::vector<detail::SplitFamilyInferenceInput> inference_inputs;
        inference_inputs.reserve(merge_contexts.size());
        for (const auto& ctx : merge_contexts) {
            inference_inputs.push_back({ctx.metadata(), ctx.rssid_offset()});
        }
        ASSIGN_OR_RETURN(inferred_families, detail::infer_split_families(inference_inputs));
        ASSIGN_OR_RETURN(rssid_lookup_maps, build_legacy_rssid_lookup_maps(merge_contexts, inferred_families));
        rssid_lookup_maps_initialized = true;
        return Status::OK();
    };
    for (size_t child_index = 0; child_index < merge_contexts.size(); ++child_index) {
        auto& ctx = merge_contexts[child_index];
        // Flush the tablet's PK-index memtable into sstables so that the
        // inherited sstable_meta covers all live data of its rowsets. Covers
        // the case where a child accumulated post-split DML that never
        // reached shared storage before merge; see the symmetric call in
        // split_tablet for the pre-split side of the invariant.
        ASSIGN_OR_RETURN(auto flushed_metadata, update_manager->flush_pk_memtable(ctx.metadata()));
        ctx.set_metadata(std::move(flushed_metadata));
        if (!ctx.metadata()->has_sstable_meta()) continue;

        // Compute the per-ctx disagreement-keys cache ONCE outside the
        // sstable loop. Each non-shared sstable then range-tests this
        // cache via binary search instead of re-walking the projection
        // plan + shared_rssid_map. Empty when ctx has no plan-driven
        // disagreement (= ctx is canonical, ctx is non-PK, or no rowsets
        // disagree with natural offset).
        const std::vector<uint32_t> ctx_disagreement_keys = ctx.compute_disagreement_keys();

        for (const auto& sst : ctx.metadata()->sstable_meta().sstables()) {
            // (1) Dedup: only shared sstables can be duplicates across ctxs.
            // The dedup map caches the SOURCE PB (not a dest index)
            // because the rebuild path may replace or drop the projected
            // PB, which would invalidate an index-based cache.
            if (sst.shared()) {
                auto [it, inserted] = shared_dedup_sources.emplace(sst.filename(), sst);
                if (!inserted) {
                    if (!shared_sstable_metadata_matches(it->second, sst)) {
                        return Status::Corruption("Shared sstable metadata mismatch for same filename");
                    }
                    continue; // duplicate, already projected/rebuilt
                }
            }

            // (2) Ancestor-inherited shared PK sstable (shared=true,
            // !has_shared_rssid): legacy form, may need rebuild after
            // multi-cycle split/merge with partial-child compaction.
            // The current ctx is the dedup-winner by construction:
            // shared_dedup_sources only emplaces on first sighting.
            if (sst.shared() && !sst.has_shared_rssid()) {
                RETURN_IF_ERROR(ensure_rssid_lookup_maps());
                RETURN_IF_ERROR(emit_legacy_shared_sstable_into_dest(tablet_manager, ctx, child_index, sst,
                                                                     *new_metadata, inferred_families,
                                                                     rssid_lookup_maps, dest));
                continue;
            }

            // (3) Modern projection: shared sstable with has_shared_rssid
            // (split-derived single-rowset slice).
            if (sst.has_shared_rssid()) {
                auto* out = dest->Add();
                out->CopyFrom(sst);
                RETURN_IF_ERROR(project_modern_shared_rssid_sstable(sst, ctx, new_metadata, out));
                continue;
            }

            // (4) Non-shared, non-modern: child-local PK sstable.
            RETURN_IF_ERROR(emit_non_shared_legacy_sstable_into_dest(tablet_manager, ctx, sst, *new_metadata,
                                                                     ctx_disagreement_keys, dest));
        }
    }

    // The merge above appended projected sstables in source-child iteration
    // order. Re-sort by signed max_rss_rowid and reassign fileset_ids on
    // non-contiguous reuse so the emitted sstable_meta satisfies both the
    // signed-monotone (I1) and contiguous-fileset (I2) invariants. See the
    // helper for the full reasoning.
    reassign_fileset_ids_for_ordered_runs(dest);
    return Status::OK();
}

void update_next_rowset_id(TabletMetadataPB* metadata) {
    uint32_t max_end = 1; // invariant: next_rowset_id >= 1
    for (const auto& rowset : metadata->rowsets()) {
        max_end = std::max(max_end, rowset.id() + get_rowset_id_step(rowset));
    }
    // Also consider sstable_meta projected max_rss_rowid. merge_sstables advances
    // each shared sstable's high word by ctx.rssid_offset (tablet_merger.cpp ~615),
    // and the legacy non-shared_rssid branch can produce projected high words
    // larger than any surviving rowset.id (e.g. when a delete-only sstable from a
    // child contributes a high rssid that has no corresponding rowset in the merged
    // metadata). If next_rowset_id is set from rowset.id alone, future writes on
    // this tablet — and on SPLIT children that inherit this metadata — will assign
    // rssids smaller than the projected sstable highs, producing sstables whose
    // max_rss_rowid is LESS than existing entries and violating the ascending-order
    // invariant that LakePersistentIndex::commit() (lake_persistent_index.cpp:881)
    // enforces. The downstream symptom is the compaction publish failing with
    // "sstables are not ordered, last_max_rss_rowid=A : max_rss_rowid=B" and the
    // next reshard job parking in PREPARING because visibleVersion never catches
    // up. Bound next_rowset_id by (max projected high word) + 1 so any new rssid
    // is strictly greater than every projected sstable rssid.
    if (metadata->has_sstable_meta()) {
        for (const auto& sst : metadata->sstable_meta().sstables()) {
            const uint32_t projected_high = extract_rss_rowid_high(sst.max_rss_rowid());
            // Saturated UINT32_MAX would already break the rssid encoding, so
            // bumping past it is meaningless — leave max_end alone there.
            if (projected_high < std::numeric_limits<uint32_t>::max()) {
                max_end = std::max(max_end, projected_high + 1);
            }
        }
    }
    metadata->set_next_rowset_id(max_end);
}

void merge_schemas(const std::vector<TabletMergeContext>& merge_contexts, TabletMetadataPB* new_metadata) {
    // Step 1: Collect all historical_schemas from all children (union by schema_id)
    auto* merged_schemas = new_metadata->mutable_historical_schemas();
    merged_schemas->clear();
    for (const auto& ctx : merge_contexts) {
        for (const auto& [schema_id, schema] : ctx.metadata()->historical_schemas()) {
            (*merged_schemas)[schema_id] = schema;
        }
    }

    // Step 2: Prune rowset_to_schema entries for non-existent rowset_ids
    std::unordered_set<uint32_t> rowset_ids;
    rowset_ids.reserve(new_metadata->rowsets_size());
    for (const auto& rowset : new_metadata->rowsets()) {
        rowset_ids.insert(rowset.id());
    }

    auto* rowset_to_schema = new_metadata->mutable_rowset_to_schema();
    for (auto it = rowset_to_schema->begin(); it != rowset_to_schema->end();) {
        if (rowset_ids.count(it->first) == 0) {
            it = rowset_to_schema->erase(it);
        } else {
            ++it;
        }
    }

    // Step 3: Prune historical_schemas not referenced by any rowset_to_schema
    std::unordered_set<int64_t> referenced_schema_ids;
    for (const auto& [rowset_id, schema_id] : *rowset_to_schema) {
        referenced_schema_ids.insert(schema_id);
    }

    for (auto it = merged_schemas->begin(); it != merged_schemas->end();) {
        if (referenced_schema_ids.count(it->first) == 0) {
            it = merged_schemas->erase(it);
        } else {
            ++it;
        }
    }

    // Step 4: Ensure current schema is present (may have been pruned in step 3)
    if (new_metadata->schema().has_id() && merged_schemas->count(new_metadata->schema().id()) == 0) {
        (*merged_schemas)[new_metadata->schema().id()] = new_metadata->schema();
    }
}

} // namespace

StatusOr<MutableTabletMetadataPtr> merge_tablet(TabletManager* tablet_manager,
                                                const std::vector<TabletMetadataPtr>& old_tablet_metadatas,
                                                const MergingTabletInfoPB& merging_tablet, int64_t new_version,
                                                const TxnInfoPB& txn_info) {
    if (old_tablet_metadatas.empty()) {
        return Status::InvalidArgument("No old tablet metadata to merge");
    }

    std::vector<TabletMergeContext> merge_contexts;
    merge_contexts.reserve(old_tablet_metadatas.size());
    for (const auto& old_tablet_metadata : old_tablet_metadatas) {
        if (old_tablet_metadata == nullptr) {
            return Status::InvalidArgument("old tablet metadata is null");
        }
        merge_contexts.emplace_back(old_tablet_metadata);
    }

    auto new_tablet_metadata = std::make_shared<TabletMetadataPB>(*merge_contexts.front().metadata());
    new_tablet_metadata->set_id(merging_tablet.new_tablet_id());
    new_tablet_metadata->set_version(new_version);
    new_tablet_metadata->set_commit_time(txn_info.commit_time());
    new_tablet_metadata->set_gtid(txn_info.gtid());
    new_tablet_metadata->clear_rowsets();
    new_tablet_metadata->clear_delvec_meta();
    new_tablet_metadata->clear_sstable_meta();
    new_tablet_metadata->clear_dcg_meta();
    new_tablet_metadata->clear_rowset_to_schema();
    new_tablet_metadata->clear_compaction_inputs();
    new_tablet_metadata->clear_orphan_files();
    new_tablet_metadata->clear_prev_garbage_version();
    new_tablet_metadata->set_cumulative_point(0);

    // Phase 1: Prepare rssid offsets and merged range. For each ctx[i],
    // set new_tablet_metadata.next_rowset_id to the watermark of all
    // earlier ctxs[0..i-1]'s projected rowset ids; compute_rssid_offset
    // then derives ctx[i].rssid_offset against that watermark so its
    // projected ids land strictly above every earlier ctx's.
    for (size_t i = 1; i < merge_contexts.size(); ++i) {
        const uint32_t cumulative_ceiling = compute_cumulative_rowset_id_ceiling(
                merge_contexts, /*upper_exclusive=*/i, merge_contexts.front().metadata()->next_rowset_id());
        new_tablet_metadata->set_next_rowset_id(cumulative_ceiling);
        merge_contexts[i].set_rssid_offset(compute_rssid_offset(*new_tablet_metadata, *merge_contexts[i].metadata()));
    }

    // Merge tablet-level range via union_range
    TabletRangePB merged_range = merge_contexts.front().metadata()->range();
    for (size_t i = 1; i < merge_contexts.size(); ++i) {
        ASSIGN_OR_RETURN(merged_range,
                         tablet_reshard_helper::union_range(merged_range, merge_contexts[i].metadata()->range()));
    }
    new_tablet_metadata->mutable_range()->CopyFrom(merged_range);

    // Phase 1.5: build the family-canonical projection plan once Phase 1
    // has populated rssid_offset, and wire (_child_index, _projection_plan)
    // onto every TabletMergeContext. The plan drives three consumers:
    //   - TabletMergeContext::map_rssid (priority 1 lookup);
    //   - the legacy-sstable fast-path (plan.family_legacy_sstable_offset
    //     + per-family LegacyRssidLookupMaps);
    //   - merge_sstables's non-shared dispatch via
    //     mapping_disagrees_with_natural_in_range, which routes mixed-
    //     reference sstables to rebuild_non_shared_legacy_sstable.
    //
    // PK gate only — non-PK tables skip the build and leave the plan
    // pointer null; map_rssid then degenerates to the
    // shared_rssid_map → natural offset path.
    detail::InferredSplitFamilies inferred_families;
    detail::RssidProjectionPlan projection_plan;
    if (is_primary_key(*new_tablet_metadata)) {
        std::vector<detail::SplitFamilyInferenceInput> inference_inputs;
        inference_inputs.reserve(merge_contexts.size());
        for (const auto& ctx : merge_contexts) {
            inference_inputs.push_back({ctx.metadata(), ctx.rssid_offset()});
        }
        ASSIGN_OR_RETURN(inferred_families, detail::infer_split_families(inference_inputs));
        ASSIGN_OR_RETURN(projection_plan, detail::build_rssid_projection_plan(inference_inputs, inferred_families));
        for (size_t i = 0; i < merge_contexts.size(); ++i) {
            merge_contexts[i].set_child_index(static_cast<uint32_t>(i));
            merge_contexts[i].set_projection_plan(&projection_plan);
        }
    }

    // Phase 2: Merge rowsets (version-driven k-way merge with dedup).
    // canonical_contribs collects each canonical rowset's contributing
    // children's child-local ranges; consumed by the PK fail-fast coverage
    // check below and by gap-delvec synthesis.
    CanonicalContribMap canonical_contribs;
    RETURN_IF_ERROR(merge_rowsets(merge_contexts, new_tablet_metadata.get(), &canonical_contribs));

    // Phase 2.5: Merge schemas (must run before merge_dcg_meta, which needs
    // historical_schemas to locate rebuild schemas for shared-segment rebuild).
    merge_schemas(merge_contexts, new_tablet_metadata.get());

    // Phase 3: Projections (map_rssid uses shared_rssid_map + rssid_offset)
    RETURN_IF_ERROR(merge_dcg_meta(tablet_manager, merge_contexts, merging_tablet.new_tablet_id(), new_version,
                                   txn_info.txn_id(), new_tablet_metadata.get()));

    if (is_primary_key(*new_tablet_metadata)) {
        RETURN_IF_ERROR(merge_delvecs(tablet_manager, merge_contexts, canonical_contribs, new_version,
                                      txn_info.txn_id(), new_tablet_metadata.get()));
    }

    RETURN_IF_ERROR(merge_sstables(tablet_manager, merge_contexts, new_tablet_metadata.get()));

    // Phase 4: Finalize
    update_next_rowset_id(new_tablet_metadata.get());

    return new_tablet_metadata;
}

} // namespace starrocks::lake

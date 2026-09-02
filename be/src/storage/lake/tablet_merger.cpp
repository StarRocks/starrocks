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
#include <google/protobuf/unknown_field_set.h>

#include <algorithm>
#include <limits>
#include <map>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "base/failpoint/fail_point.h"
#include "base/hash/crc32c.h"
#include "base/testutil/sync_point.h"
#include "base/uid_util.h"
#include "base/utility/defer_op.h"
#include "column/chunk_factory.h"
#include "column/column_helper.h"
#include "common/config_rowset_fwd.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "platform/key_cache.h"
#include "storage/chunk_helper.h"
#include "storage/del_vector.h"
#include "storage/delta_column_group.h"
#include "storage/lake/filenames.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet_manager.h"
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
#include "storage/storage_metrics.h"
#include "storage/tablet_schema.h"
#include "storage_primitive/schema_helper.h"

namespace {

bvar::Adder<int64_t> g_tablet_merge_dcg_rebuild_total("tablet_merge_dcg_rebuild_total");
bvar::Adder<int64_t> g_tablet_merge_dcg_rebuild_fallback_not_supported_total(
        "tablet_merge_dcg_rebuild_fallback_not_supported_total");

bvar::Adder<int64_t> g_tablet_merge_gap_delvec_total("tablet_merge_gap_delvec_total");
bvar::Adder<int64_t> g_tablet_merge_non_pk_skip_dedup_total("tablet_merge_non_pk_skip_dedup_total");
bvar::Adder<int64_t> g_tablet_merge_synthesized_only_delvec_total("tablet_merge_synthesized_only_delvec_total");

bvar::Adder<int64_t> g_tablet_merge_sstable_meta_private_total("tablet_merge_sstable_meta_private_total");
bvar::Adder<int64_t> g_tablet_merge_sstable_meta_identical_total("tablet_merge_sstable_meta_identical_total");
bvar::Adder<int64_t> g_tablet_merge_sstable_meta_lazy_rebuild_total("tablet_merge_sstable_meta_lazy_rebuild_total");
bvar::Adder<int64_t> g_tablet_merge_sstable_fallback_shared_or_mixed_total(
        "tablet_merge_sstable_fallback_shared_or_mixed_total");
bvar::Adder<int64_t> g_tablet_merge_sstable_fallback_nonuniform_mapping_total(
        "tablet_merge_sstable_fallback_nonuniform_mapping_total");
bvar::Adder<int64_t> g_tablet_merge_sstable_fallback_cohort_mismatch_total(
        "tablet_merge_sstable_fallback_cohort_mismatch_total");
bvar::Adder<int64_t> g_tablet_merge_sstable_fallback_rowset_layout_mismatch_total(
        "tablet_merge_sstable_fallback_rowset_layout_mismatch_total");
bvar::Adder<int64_t> g_tablet_merge_sstable_fallback_duplicate_physical_file_total(
        "tablet_merge_sstable_fallback_duplicate_physical_file_total");
bvar::Adder<int64_t> g_tablet_merge_sstable_fallback_unsupported_sst_form_total(
        "tablet_merge_sstable_fallback_unsupported_sst_form_total");
bvar::Adder<int64_t> g_tablet_merge_sstable_fallback_projected_domain_total(
        "tablet_merge_sstable_fallback_projected_domain_total");
bvar::Adder<int64_t> g_tablet_merge_sstable_fallback_embedded_delvec_total(
        "tablet_merge_sstable_fallback_embedded_delvec_total");
bvar::Adder<int64_t> g_tablet_merge_sstable_omitted_file_total("tablet_merge_sstable_omitted_file_total");
bvar::Adder<int64_t> g_tablet_merge_sstable_omitted_bytes_total("tablet_merge_sstable_omitted_bytes_total");

} // namespace

namespace starrocks::lake {

DEFINE_FAIL_POINT(skip_lake_pk_index_merge_source_flush);

namespace {

class TabletMergeContext {
public:
    explicit TabletMergeContext(TabletMetadataPtr metadata) : _metadata(std::move(metadata)) {}

    const TabletMetadataPtr& metadata() const { return _metadata; }
    // Reseat the backing metadata pointer. Used by flush_persistent_index to
    // substitute a spliced snapshot (same rowsets, sstable_meta updated to
    // include freshly-flushed PK-index sstables).
    void set_metadata(TabletMetadataPtr metadata) { _metadata = std::move(metadata); }

private:
    TabletMetadataPtr _metadata;
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

// Tracks the contributing old tablets' old-tablet-local ranges per canonical rowset
// in new_metadata. Used by the post-merge_rowsets PK coverage check and by
// gap-delvec synthesis.
//
// Key: index of the canonical rowset in new_metadata->rowsets().
// Value: each entry is the contributing old tablet's effective_old_tablet_local_range
// (rowset.range, fallback ctx.metadata.range, fallback unbounded), captured
// BEFORE update_canonical's union_range mutates the canonical's stored range
// — otherwise the convex hull would swallow gaps and the coverage / gap
// detection would fail.
using CanonicalContribMap = std::unordered_map<size_t, std::vector<TabletRangePB>>;

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
    const Roaring empty;
    target->union_with(version, source.roaring() != nullptr ? *source.roaring() : empty);
}

// Duplicate detection for merge: two rowsets are the same logical rowset across
// split siblings iff they carry the same global uid. The uid is minted once at
// rowset creation and carried verbatim by CopyFrom across SPLIT and cross-publish,
// so siblings descended from one logical rowset (split-pruned, or a concurrent
// write cross-published to every old tablet) compare equal, while an independently
// produced rowset (e.g. a post-split local compaction output) carries a fresh uid
// and never aliases. A rowset without a valid uid is never treated as a duplicate.
bool is_duplicate_rowset(const RowsetMetadataPB& a, const RowsetMetadataPB& b) {
    return tablet_reshard_helper::same_rowset_uid(a, b);
}

struct RowsetEmissionDecision {
    int canonical_index = -1;
    bool emit = false;
    bool discard = false;
    bool non_pk_skip_dedup_fired = false;
};

using RowsetEmissionPlan = std::vector<std::vector<RowsetEmissionDecision>>;

struct RssidTranslationRun {
    uint64_t source_begin;
    uint64_t source_end;
    uint32_t target_begin;
};

constexpr uint64_t kSourceRssidExclusiveLimit = uint64_t{std::numeric_limits<uint32_t>::max()} + 1;
constexpr uint64_t kTargetRssidExclusiveLimit = static_cast<uint64_t>(std::numeric_limits<int32_t>::max());

class RssidProjection {
public:
    Status build(std::vector<std::pair<uint64_t, uint64_t>> atoms, uint32_t target_begin) {
        _runs.clear();
        _occurrence_aliases.clear();
        _divergent_alias_keys.clear();
        _target_end = target_begin;
        std::sort(atoms.begin(), atoms.end());
        std::vector<std::pair<uint64_t, uint64_t>> merged;
        for (const auto& [begin, end] : atoms) {
            if (begin >= end || end > kSourceRssidExclusiveLimit) {
                return Status::InvalidArgument("tablet merge source RSSID interval is outside the supported domain");
            }
            if (merged.empty() || begin > merged.back().second) {
                merged.emplace_back(begin, end);
            } else {
                merged.back().second = std::max(merged.back().second, end);
            }
        }
        uint64_t cursor = target_begin;
        for (const auto& [begin, end] : merged) {
            const uint64_t length = end - begin;
            if (cursor + length > kTargetRssidExclusiveLimit) {
                return Status::InvalidArgument("tablet merge exhausts the supported rssid allocation domain");
            }
            _runs.emplace_back(RssidTranslationRun{begin, end, static_cast<uint32_t>(cursor)});
            cursor += length;
        }
        _target_end = static_cast<uint32_t>(cursor);
        return Status::OK();
    }

    StatusOr<uint32_t> map_primary_rssid(uint32_t source) const {
        auto iter = std::upper_bound(
                _runs.begin(), _runs.end(), static_cast<uint64_t>(source),
                [](uint64_t value, const RssidTranslationRun& run) { return value < run.source_begin; });
        if (iter == _runs.begin()) {
            return Status::Corruption(fmt::format("tablet merge primary RSSID {} is not planned", source));
        }
        --iter;
        if (source >= iter->source_end) {
            return Status::Corruption(fmt::format("tablet merge primary RSSID {} is not planned", source));
        }
        const uint64_t mapped = static_cast<uint64_t>(iter->target_begin) + source - iter->source_begin;
        if (mapped >= _target_end) {
            return Status::Corruption("tablet merge primary RSSID maps beyond its projection");
        }
        return static_cast<uint32_t>(mapped);
    }

    StatusOr<uint32_t> map_occurrence_rssid(uint32_t source) const {
        auto alias = _occurrence_aliases.find(source);
        if (alias != _occurrence_aliases.end()) return alias->second;
        return map_primary_rssid(source);
    }

    Status add_occurrence_alias(uint32_t source, uint32_t target) {
        auto [iter, inserted] = _occurrence_aliases.try_emplace(source, target);
        if (!inserted && iter->second != target) {
            return Status::Corruption(fmt::format("tablet merge occurrence RSSID {} has conflicting aliases", source));
        }
        return Status::OK();
    }

    void finalize_aliases() {
        _divergent_alias_keys.clear();
        for (const auto& [source, target] : _occurrence_aliases) {
            auto primary = map_primary_rssid(source);
            if (primary.ok() && primary.value() != target) _divergent_alias_keys.emplace_back(source);
        }
    }

    std::optional<int64_t> affine_delta(uint64_t begin, uint64_t end) const {
        if (begin >= end || end > uint64_t{std::numeric_limits<uint32_t>::max()} + 1) {
            return std::optional<int64_t>{};
        }
        auto iter = std::upper_bound(
                _runs.begin(), _runs.end(), begin,
                [](uint64_t value, const RssidTranslationRun& run) { return value < run.source_begin; });
        if (iter == _runs.begin()) return std::optional<int64_t>{};
        --iter;
        TEST_SYNC_POINT_CALLBACK("affine_delta:visited_run", const_cast<RssidTranslationRun*>(&*iter));
        if (begin < iter->source_begin || end > iter->source_end) return std::optional<int64_t>{};
        auto alias = std::lower_bound(_divergent_alias_keys.begin(), _divergent_alias_keys.end(), begin);
        if (alias != _divergent_alias_keys.end() && *alias < end) return std::optional<int64_t>{};
        return std::optional<int64_t>{static_cast<int64_t>(iter->target_begin) -
                                      static_cast<int64_t>(iter->source_begin)};
    }

    bool has_divergent_occurrence_alias(uint32_t source) const {
        return std::binary_search(_divergent_alias_keys.begin(), _divergent_alias_keys.end(), source);
    }

    uint32_t target_end() const { return _target_end; }

private:
    std::vector<RssidTranslationRun> _runs;
    std::map<uint32_t, uint32_t> _occurrence_aliases;
    std::vector<uint32_t> _divergent_alias_keys;
    uint32_t _target_end = 1;
};

struct CanonicalAllocationPlan {
    size_t selected_context_index = 0;
    RowsetMetadataPB source_form_rowset;
    std::optional<int64_t> schema_id;
    std::vector<TabletRangePB> contributor_ranges;
};

struct TabletMergeAllocationPlan {
    std::vector<CanonicalAllocationPlan> canonicals;
    std::vector<RssidProjection> projections;
    uint32_t target_next_rowset_id = 1;
    int64_t non_pk_skip_dedup_count = 0;
};

DEFINE_FAIL_POINT(tablet_merge_before_delete_predicate_range);

struct PlannedCanonicalRowset {
    const RowsetMetadataPB* rowset = nullptr;
    TabletRangePB range;
    int output_index = -1;
};

// Plan the exact rowsets that materialization will emit in (version, old-tablet-index)
// order. This is the single source of truth for duplicate decisions: allocation
// planning reconciles sibling occurrences into canonical atoms and occurrence
// aliases, then materialization uses the recorded canonical index directly.
StatusOr<RowsetEmissionPlan> build_rowset_emission_plan(const std::vector<TabletMergeContext>& merge_contexts,
                                                        bool discard_empty_rowsets) {
    RowsetEmissionPlan plan(merge_contexts.size());
    std::vector<int> current_indices(merge_contexts.size(), 0);
    for (size_t i = 0; i < merge_contexts.size(); ++i) {
        plan[i].resize(merge_contexts[i].metadata()->rowsets_size());
    }

    const bool is_pk = is_primary_key(*merge_contexts.front().metadata());
    int64_t current_version = -1;
    int next_output_index = 0;
    std::vector<PlannedCanonicalRowset> canonicals;

    for (;;) {
        int source_index = -1;
        int64_t min_version = std::numeric_limits<int64_t>::max();
        for (int i = 0; i < static_cast<int>(merge_contexts.size()); ++i) {
            if (current_indices[i] >= merge_contexts[i].metadata()->rowsets_size()) continue;
            const int64_t version = merge_contexts[i].metadata()->rowsets(current_indices[i]).version();
            if (version < min_version) {
                min_version = version;
                source_index = i;
            }
        }
        if (source_index < 0) break;

        const int rowset_index = current_indices[source_index]++;
        const auto& source_metadata = *merge_contexts[source_index].metadata();
        const auto& rowset = source_metadata.rowsets(rowset_index);
        if (discard_empty_rowsets && rowset.segment_metas_size() == 0 && rowset.del_files_size() == 0 &&
            !rowset.has_delete_predicate()) {
            plan[source_index][rowset_index].discard = true;
            continue;
        }
        if (rowset.version() != current_version) {
            current_version = rowset.version();
            canonicals.clear();
        }

        // Search the current version's canonical rowsets. Delete predicates dedup
        // by version, normal rowsets by uid; non-PK same-uid siblings dedup only
        // when their ranges are contiguous, so the canonical range cannot span a
        // gap left by a compacted sibling.
        int canonical_plan_index = -1;
        bool non_pk_skip_dedup_fired = false;
        for (int i = 0; i < static_cast<int>(canonicals.size()); ++i) {
            if (rowset.has_delete_predicate()) {
                if (canonicals[i].rowset->has_delete_predicate()) {
                    canonical_plan_index = i;
                    break;
                }
                continue;
            }
            if (!is_duplicate_rowset(rowset, *canonicals[i].rowset)) continue;

            const auto& incoming_range =
                    tablet_reshard_helper::effective_old_tablet_local_range(rowset, source_metadata);
            if (is_pk || tablet_reshard_helper::ranges_are_contiguous(canonicals[i].range, incoming_range)) {
                canonical_plan_index = i;
                if (!is_pk) {
                    ASSIGN_OR_RETURN(canonicals[i].range,
                                     tablet_reshard_helper::union_range(canonicals[i].range, incoming_range));
                }
                break;
            }
            non_pk_skip_dedup_fired = true;
        }

        auto& decision = plan[source_index][rowset_index];
        if (canonical_plan_index >= 0) {
            decision.canonical_index = canonicals[canonical_plan_index].output_index;
            continue;
        }

        decision.emit = true;
        decision.canonical_index = next_output_index++;
        decision.non_pk_skip_dedup_fired = non_pk_skip_dedup_fired;
        PlannedCanonicalRowset canonical;
        canonical.rowset = &rowset;
        canonical.output_index = decision.canonical_index;
        RowsetMetadataPB canonical_copy;
        canonical_copy.CopyFrom(rowset);
        RETURN_IF_ERROR(tablet_reshard_helper::update_rowset_range(&canonical_copy, source_metadata.range()));
        canonical.range.CopyFrom(canonical_copy.range());
        canonicals.emplace_back(std::move(canonical));
    }

    return plan;
}

std::optional<int64_t> rowset_schema_id(const TabletMergeContext& context, uint32_t rowset_id) {
    const auto& mapping = context.metadata()->rowset_to_schema();
    auto iter = mapping.find(rowset_id);
    if (iter == mapping.end()) return std::nullopt;
    return iter->second;
}

std::string normalized_physical_base_key(const SegmentMetadataPB& segment) {
    SegmentMetadataPB normalized(segment);
    normalized.clear_segment_idx();
    normalized.clear_shared();
    return normalized.SerializeAsString();
}

Status validate_physical_segment_shape(const SegmentMetadataPB& segment) {
    constexpr int64_t kSegmentFooterTrailerSize = 12;
    if (!segment.has_filename() || segment.filename().empty()) {
        return Status::Corruption("tablet merge segment has a missing or empty filename");
    }
    if (segment.has_bundle_file_offset() && segment.bundle_file_offset() < 0) {
        return Status::Corruption(fmt::format("tablet merge segment {} has negative bundle_file_offset {}",
                                              segment.filename(), segment.bundle_file_offset()));
    }
    if (segment.has_size() && segment.size() < 0) {
        return Status::Corruption(
                fmt::format("tablet merge segment {} has negative size {}", segment.filename(), segment.size()));
    }
    if (segment.has_bundle_file_offset() && !segment.has_size()) {
        return Status::Corruption(fmt::format("tablet merge segment {} has bundle_file_offset {} but no size",
                                              segment.filename(), segment.bundle_file_offset()));
    }
    if (segment.has_bundle_file_offset()) {
        if (segment.size() < kSegmentFooterTrailerSize) {
            return Status::Corruption(
                    fmt::format("tablet merge bundled segment {} size {} is smaller than the {}-byte footer trailer",
                                segment.filename(), segment.size(), kSegmentFooterTrailerSize));
        }
        const uint64_t bundle_end =
                static_cast<uint64_t>(segment.bundle_file_offset()) + static_cast<uint64_t>(segment.size());
        if (bundle_end > std::numeric_limits<int64_t>::max()) {
            return Status::Corruption(
                    fmt::format("tablet merge segment {} bundle_file_offset {} plus size {} exceeds int64 range",
                                segment.filename(), segment.bundle_file_offset(), segment.size()));
        }
    }
    return Status::OK();
}

Status validate_physical_rowset_shape(const RowsetMetadataPB& rowset) {
    std::optional<bool> bundled;
    std::map<std::pair<std::string, int64_t>, uint32_t> segment_index_by_physical_slice;
    for (int position = 0; position < rowset.segment_metas_size(); ++position) {
        const auto& segment = rowset.segment_metas(position);
        RETURN_IF_ERROR(validate_physical_segment_shape(segment));
        if (!bundled.has_value()) {
            bundled = segment.has_bundle_file_offset();
        } else if (*bundled != segment.has_bundle_file_offset()) {
            return Status::Corruption(
                    fmt::format("tablet merge rowset {} mixes bundled and standalone segments", rowset.id()));
        }
        const auto slice = std::pair{segment.filename(),
                                     segment.has_bundle_file_offset() ? segment.bundle_file_offset() : int64_t{0}};
        const uint32_t segment_index = get_segment_idx(rowset, position);
        auto [iter, inserted] = segment_index_by_physical_slice.try_emplace(slice, segment_index);
        if (!inserted && iter->second != segment_index) {
            return Status::Corruption(fmt::format(
                    "tablet merge rowset {} references physical segment slice {} at bundle offset {} from multiple "
                    "segment indices",
                    rowset.id(), slice.first, slice.second));
        }
    }
    return Status::OK();
}

Status reconcile_segments(RowsetMetadataPB* canonical, const RowsetMetadataPB* occurrence) {
    const bool overlapped = canonical->overlapped() || (occurrence != nullptr && occurrence->overlapped());
    std::map<uint32_t, SegmentMetadataPB> by_index;
    auto ingest = [&](const RowsetMetadataPB& source) -> Status {
        std::unordered_set<uint32_t> source_indices;
        for (int position = 0; position < source.segment_metas_size(); ++position) {
            const uint32_t index = get_segment_idx(source, position);
            if (!source_indices.insert(index).second) {
                return Status::Corruption(
                        fmt::format("tablet merge source rowset has duplicate effective segment index {}", index));
            }
            SegmentMetadataPB candidate(source.segment_metas(position));
            candidate.set_segment_idx(index);
            const bool candidate_shared = candidate.shared();
            SegmentMetadataPB stable_candidate(candidate);
            stable_candidate.clear_shared();
            auto [iter, inserted] = by_index.try_emplace(index, candidate);
            if (inserted) {
                iter->second.set_shared(candidate_shared);
                continue;
            }
            SegmentMetadataPB stable_existing(iter->second);
            stable_existing.set_segment_idx(index);
            stable_existing.clear_shared();
            if (stable_existing.SerializeAsString() != stable_candidate.SerializeAsString()) {
                return Status::Corruption(
                        fmt::format("tablet merge segment declaration conflict at effective index {}", index));
            }
            iter->second.set_shared(iter->second.shared() || candidate_shared);
        }
        return Status::OK();
    };
    RETURN_IF_ERROR(ingest(*canonical));
    const size_t canonical_segment_count = by_index.size();
    if (occurrence != nullptr) RETURN_IF_ERROR(ingest(*occurrence));
    const bool segment_union_expanded = by_index.size() > canonical_segment_count;
    canonical->clear_segment_metas();
    for (auto& [index, segment] : by_index) {
        (void)index;
        canonical->add_segment_metas()->Swap(&segment);
    }
    if (overlapped || segment_union_expanded) canonical->set_overlapped(true);
    return Status::OK();
}

Status normalize_cross_target_physical_ownership(std::vector<CanonicalAllocationPlan>* canonicals) {
    struct PhysicalSliceDeclaration {
        std::string declaration_key;
        std::vector<SegmentMetadataPB*> references;
    };
    struct PhysicalFilenameDeclaration {
        bool bundled = false;
        std::map<int64_t, int64_t> bundled_intervals;
    };
    std::map<std::string, PhysicalFilenameDeclaration> declarations_by_filename;
    std::map<std::pair<std::string, int64_t>, PhysicalSliceDeclaration> declarations_by_physical_slice;
    for (auto& canonical : *canonicals) {
        RETURN_IF_ERROR(validate_physical_rowset_shape(canonical.source_form_rowset));
        for (auto& segment : *canonical.source_form_rowset.mutable_segment_metas()) {
            auto [file, file_inserted] = declarations_by_filename.try_emplace(
                    segment.filename(), PhysicalFilenameDeclaration{.bundled = segment.has_bundle_file_offset()});
            if (!file_inserted && file->second.bundled != segment.has_bundle_file_offset()) {
                return Status::Corruption(
                        fmt::format("tablet merge physical segment file {} mixes bundled and standalone forms",
                                    segment.filename()));
            }
            if (segment.has_bundle_file_offset()) {
                const int64_t bundle_end = static_cast<int64_t>(static_cast<uint64_t>(segment.bundle_file_offset()) +
                                                                static_cast<uint64_t>(segment.size()));
                file->second.bundled_intervals.try_emplace(segment.bundle_file_offset(), bundle_end);
            }
            const auto slice = std::pair{segment.filename(),
                                         segment.has_bundle_file_offset() ? segment.bundle_file_offset() : int64_t{0}};
            const auto declaration_key = normalized_physical_base_key(segment);
            auto [iter, inserted] = declarations_by_physical_slice.try_emplace(slice);
            if (inserted) {
                iter->second.declaration_key = declaration_key;
            } else if (iter->second.declaration_key != declaration_key) {
                return Status::Corruption(
                        fmt::format("tablet merge physical segment slice {} at bundle offset {} has conflicting "
                                    "declarations",
                                    slice.first, slice.second));
            }
            iter->second.references.emplace_back(&segment);
        }
    }
    for (const auto& [filename, declaration] : declarations_by_filename) {
        std::optional<std::pair<int64_t, int64_t>> previous;
        for (const auto& [begin, end] : declaration.bundled_intervals) {
            if (previous.has_value() && begin < previous->second) {
                return Status::Corruption(fmt::format(
                        "tablet merge physical segment file {} has overlapping bundled slices [{}, {}) and [{}, {})",
                        filename, previous->first, previous->second, begin, end));
            }
            previous = std::pair{begin, end};
        }
    }
    for (auto& [slice, declaration] : declarations_by_physical_slice) {
        (void)slice;
        if (declaration.references.size() < 2) continue;
        for (auto* segment : declaration.references) segment->set_shared(true);
    }
    return Status::OK();
}

Status validate_del_replay_span(uint32_t origin, uint32_t offset) {
    const uint64_t end = static_cast<uint64_t>(origin) + static_cast<uint64_t>(offset) + 1;
    if (end > kSourceRssidExclusiveLimit) {
        return Status::InvalidArgument("tablet merge delete replay span exceeds the supported RSSID domain");
    }
    return Status::OK();
}

Status reconcile_duplicate_dels(CanonicalAllocationPlan* canonical, const RowsetMetadataPB& occurrence) {
    auto* selected = &canonical->source_form_rowset;
    if (selected->del_files_size() != occurrence.del_files_size()) {
        return Status::Corruption("tablet merge duplicate rowset del-file count differs");
    }
    const uint32_t current_max = get_max_segment_idx(occurrence);
    for (int del_index = 0; del_index < selected->del_files_size(); ++del_index) {
        auto* selected_del = selected->mutable_del_files(del_index);
        const auto& occurrence_del = occurrence.del_files(del_index);
        const bool selected_self = selected_del->origin_rowset_id() == selected->id();
        const bool occurrence_self = occurrence_del.origin_rowset_id() == occurrence.id();
        if (selected_self != occurrence_self) {
            return Status::Corruption("tablet merge duplicate del files mix self and inherited origins");
        }
        DelfileWithRowsetId stable_selected(*selected_del);
        DelfileWithRowsetId stable_occurrence(occurrence_del);
        stable_selected.clear_origin_rowset_id();
        stable_occurrence.clear_origin_rowset_id();
        stable_selected.clear_shared();
        stable_occurrence.clear_shared();
        if (stable_selected.SerializeAsString() != stable_occurrence.SerializeAsString()) {
            return Status::Corruption("tablet merge duplicate del-file declaration conflict");
        }
        selected_del->set_shared(selected_del->shared() || occurrence_del.shared());
        const uint32_t local_offset = occurrence_del.has_op_offset() ? occurrence_del.op_offset() : current_max;
        RETURN_IF_ERROR(validate_del_replay_span(occurrence_del.origin_rowset_id(), local_offset));
    }
    return Status::OK();
}

Status union_canonical_range(CanonicalAllocationPlan* canonical, const TabletRangePB& occurrence_range) {
    if (!canonical->source_form_rowset.has_range()) {
        canonical->source_form_rowset.mutable_range()->CopyFrom(occurrence_range);
        return Status::OK();
    }
    ASSIGN_OR_RETURN(auto united,
                     tablet_reshard_helper::union_range(canonical->source_form_rowset.range(), occurrence_range));
    canonical->source_form_rowset.mutable_range()->CopyFrom(united);
    return Status::OK();
}

Status validate_primary_affine_span(const RssidProjection& projection, uint64_t begin, uint64_t end,
                                    std::string_view description) {
    auto delta = projection.affine_delta(begin, end);
    if (!delta.has_value()) {
        return Status::Corruption(fmt::format("tablet merge {} is not primary-affine", description));
    }
    return Status::OK();
}

StatusOr<TabletMergeAllocationPlan> build_tablet_merge_allocation_plan(const std::vector<TabletMergeContext>& contexts,
                                                                       bool discard_empty_rowsets) {
    bool force_context_span_projection = false;
    TEST_SYNC_POINT_CALLBACK("tablet_merge_test:force_context_span_projection", &force_context_span_projection);
    bool drop_primary_atom = false;
    TEST_SYNC_POINT_CALLBACK("tablet_merge_test:drop_primary_atom", &drop_primary_atom);

    TabletMergeAllocationPlan result;
    ASSIGN_OR_RETURN(auto emission, build_rowset_emission_plan(contexts, discard_empty_rowsets));
    int canonical_count = 0;
    for (const auto& decisions : emission) {
        for (const auto& decision : decisions) {
            canonical_count = std::max(canonical_count, decision.canonical_index + 1);
            result.non_pk_skip_dedup_count += decision.emit && decision.non_pk_skip_dedup_fired;
        }
    }
    result.canonicals.resize(canonical_count);

    for (size_t context_index = 0; context_index < contexts.size(); ++context_index) {
        const auto& metadata = *contexts[context_index].metadata();
        for (int rowset_index = 0; rowset_index < metadata.rowsets_size(); ++rowset_index) {
            const auto& decision = emission[context_index][rowset_index];
            if (!decision.emit) continue;
            const auto& rowset = metadata.rowsets(rowset_index);
            DCHECK(tablet_reshard_helper::has_valid_uid(rowset))
                    << "rowset reaching reshard merge must carry a valid uid: rowset_id=" << rowset.id()
                    << " version=" << rowset.version();
            if (!tablet_reshard_helper::has_valid_uid(rowset)) {
                return Status::InternalError("rowset reaching reshard merge has no uid");
            }
            auto& canonical = result.canonicals[decision.canonical_index];
            canonical.selected_context_index = context_index;
            canonical.source_form_rowset.CopyFrom(rowset);
            RETURN_IF_ERROR(
                    tablet_reshard_helper::update_rowset_range(&canonical.source_form_rowset, metadata.range()));
            RETURN_IF_ERROR(reconcile_segments(&canonical.source_form_rowset, nullptr));
            canonical.schema_id = rowset_schema_id(contexts[context_index], rowset.id());
            if (!rowset.has_delete_predicate()) {
                canonical.contributor_ranges.emplace_back(
                        tablet_reshard_helper::effective_old_tablet_local_range(rowset, metadata));
            }
        }
    }

    for (size_t context_index = 0; context_index < contexts.size(); ++context_index) {
        const auto& metadata = *contexts[context_index].metadata();
        for (int rowset_index = 0; rowset_index < metadata.rowsets_size(); ++rowset_index) {
            const auto& decision = emission[context_index][rowset_index];
            if (decision.discard || decision.emit) continue;
            const auto& occurrence = metadata.rowsets(rowset_index);
            DCHECK(tablet_reshard_helper::has_valid_uid(occurrence))
                    << "rowset reaching reshard merge must carry a valid uid: rowset_id=" << occurrence.id()
                    << " version=" << occurrence.version();
            if (!tablet_reshard_helper::has_valid_uid(occurrence)) {
                return Status::InternalError("rowset reaching reshard merge has no uid");
            }
            auto& canonical = result.canonicals[decision.canonical_index];
            const auto schema_id = rowset_schema_id(contexts[context_index], occurrence.id());
            if (canonical.schema_id != schema_id) {
                return Status::Corruption("tablet merge duplicate rowset schema mapping conflict");
            }
            const auto& occurrence_range =
                    tablet_reshard_helper::effective_old_tablet_local_range(occurrence, metadata);
            if (occurrence.has_delete_predicate()) {
                if (!canonical.source_form_rowset.has_delete_predicate() ||
                    canonical.source_form_rowset.delete_predicate().SerializeAsString() !=
                            occurrence.delete_predicate().SerializeAsString()) {
                    return Status::Corruption("tablet merge duplicate delete-predicate declaration conflict");
                }
                RETURN_IF_ERROR(union_canonical_range(&canonical, occurrence_range));
                continue;
            }
            canonical.contributor_ranges.emplace_back(occurrence_range);
            RETURN_IF_ERROR(union_canonical_range(&canonical, occurrence_range));
            canonical.source_form_rowset.set_num_rows(canonical.source_form_rowset.num_rows() + occurrence.num_rows());
            canonical.source_form_rowset.set_data_size(canonical.source_form_rowset.data_size() +
                                                       occurrence.data_size());
            canonical.source_form_rowset.set_num_dels(canonical.source_form_rowset.num_dels() + occurrence.num_dels());
            RETURN_IF_ERROR(reconcile_segments(&canonical.source_form_rowset, &occurrence));
            RETURN_IF_ERROR(reconcile_duplicate_dels(&canonical, occurrence));
        }
    }

    RETURN_IF_ERROR(normalize_cross_target_physical_ownership(&result.canonicals));

    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> atoms(contexts.size());
    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> extents(contexts.size());
    for (const auto& canonical : result.canonicals) {
        const auto& rowset = canonical.source_form_rowset;
        const uint32_t final_max = get_max_segment_idx(rowset);
        const uint64_t segment_span = rowset.segment_metas_size() == 0 ? 1 : uint64_t{final_max} + 1;
        const uint64_t extent_end = uint64_t{rowset.id()} + segment_span;
        if (extent_end > kSourceRssidExclusiveLimit) {
            return Status::InvalidArgument("tablet merge rowset extent exceeds the supported RSSID domain");
        }
        auto& context_atoms = atoms[canonical.selected_context_index];
        context_atoms.emplace_back(rowset.id(), extent_end);
        extents[canonical.selected_context_index].emplace_back(rowset.id(), extent_end);
        if (rowset.has_max_compact_input_rowset_id() && !drop_primary_atom) {
            context_atoms.emplace_back(rowset.max_compact_input_rowset_id(),
                                       uint64_t{rowset.max_compact_input_rowset_id()} + 1);
        }
        for (const auto& del : rowset.del_files()) {
            const uint32_t offset = del.has_op_offset() ? del.op_offset() : final_max;
            RETURN_IF_ERROR(validate_del_replay_span(del.origin_rowset_id(), offset));
            context_atoms.emplace_back(del.origin_rowset_id(), uint64_t{del.origin_rowset_id()} + uint64_t{offset} + 1);
        }
    }
    for (auto& context_extents : extents) {
        std::sort(context_extents.begin(), context_extents.end());
        for (size_t i = 1; i < context_extents.size(); ++i) {
            if (context_extents[i].first < context_extents[i - 1].second) {
                return Status::Corruption("tablet merge emitted rowset extents overlap");
            }
        }
    }

    result.projections.resize(contexts.size());
    uint32_t cursor = 1;
    for (size_t context_index = 0; context_index < contexts.size(); ++context_index) {
        if (force_context_span_projection && !atoms[context_index].empty()) {
            atoms[context_index] = {{0, contexts[context_index].metadata()->next_rowset_id()}};
        }
        RETURN_IF_ERROR(result.projections[context_index].build(std::move(atoms[context_index]), cursor));
        cursor = result.projections[context_index].target_end();
    }

    for (size_t context_index = 0; context_index < contexts.size(); ++context_index) {
        const auto& metadata = *contexts[context_index].metadata();
        for (int rowset_index = 0; rowset_index < metadata.rowsets_size(); ++rowset_index) {
            const auto& decision = emission[context_index][rowset_index];
            const auto& occurrence = metadata.rowsets(rowset_index);
            if (decision.discard || decision.emit || occurrence.has_delete_predicate()) continue;
            const auto& canonical = result.canonicals[decision.canonical_index];
            const auto& canonical_rowset = canonical.source_form_rowset;
            ASSIGN_OR_RETURN(
                    uint32_t canonical_target,
                    result.projections[canonical.selected_context_index].map_primary_rssid(canonical_rowset.id()));
            const uint64_t span = canonical_rowset.segment_metas_size() == 0
                                          ? 1
                                          : uint64_t{get_max_segment_idx(canonical_rowset)} + 1;
            const uint64_t canonical_target_end = uint64_t{canonical_target} + span;
            auto add_alias = [&](uint32_t source, uint64_t target) -> Status {
                if (target < canonical_target || target >= canonical_target_end || target >= cursor) {
                    return Status::Corruption("alias target lies outside planned canonical extent");
                }
                return result.projections[context_index].add_occurrence_alias(source, static_cast<uint32_t>(target));
            };
            RETURN_IF_ERROR(add_alias(occurrence.id(), canonical_target));
            for (int position = 0; position < occurrence.segment_metas_size(); ++position) {
                const uint32_t index = get_segment_idx(occurrence, position);
                const uint64_t source = uint64_t{occurrence.id()} + index;
                if (source > std::numeric_limits<uint32_t>::max()) {
                    return Status::InvalidArgument("tablet merge occurrence RSSID exceeds uint32");
                }
                RETURN_IF_ERROR(add_alias(static_cast<uint32_t>(source), uint64_t{canonical_target} + index));
            }
        }
        result.projections[context_index].finalize_aliases();
    }

    for (const auto& canonical : result.canonicals) {
        const auto& rowset = canonical.source_form_rowset;
        const auto& projection = result.projections[canonical.selected_context_index];
        const uint32_t final_max = get_max_segment_idx(rowset);
        const uint64_t extent_end =
                uint64_t{rowset.id()} + (rowset.segment_metas_size() == 0 ? 1 : uint64_t{final_max} + 1);
        RETURN_IF_ERROR(validate_primary_affine_span(projection, rowset.id(), extent_end, "canonical extent"));
        ASSIGN_OR_RETURN(auto mapped_id, projection.map_primary_rssid(rowset.id()));
        if (mapped_id >= cursor) return Status::Corruption("tablet merge canonical target is outside cursor");
        if (rowset.has_max_compact_input_rowset_id()) {
            ASSIGN_OR_RETURN(auto mapped, projection.map_primary_rssid(rowset.max_compact_input_rowset_id()));
            (void)mapped;
        }
        for (const auto& del : rowset.del_files()) {
            const uint32_t offset = del.has_op_offset() ? del.op_offset() : final_max;
            RETURN_IF_ERROR(validate_primary_affine_span(projection, del.origin_rowset_id(),
                                                         uint64_t{del.origin_rowset_id()} + uint64_t{offset} + 1,
                                                         "selected delete span"));
        }
    }

    std::optional<uint32_t> previous_recovery_target;
    for (size_t context_index = 0; context_index < contexts.size(); ++context_index) {
        std::map<uint32_t, uint32_t> groups;
        for (const auto& canonical : result.canonicals) {
            if (canonical.selected_context_index != context_index) continue;
            const auto& rowset = canonical.source_form_rowset;
            const uint32_t raw_key =
                    rowset.has_max_compact_input_rowset_id() ? rowset.max_compact_input_rowset_id() : rowset.id();
            ASSIGN_OR_RETURN(uint32_t target_key, result.projections[context_index].map_primary_rssid(raw_key));
            if (result.projections[context_index].has_divergent_occurrence_alias(raw_key)) {
                return Status::Corruption("tablet merge recovery key has a divergent occurrence alias");
            }
            auto [iter, inserted] = groups.try_emplace(raw_key, target_key);
            if (!inserted && iter->second != target_key) {
                return Status::Corruption("tablet merge equal recovery keys do not remain equivalent");
            }
        }
        for (const auto& [raw_key, target_key] : groups) {
            (void)raw_key;
            if (previous_recovery_target.has_value() && target_key <= *previous_recovery_target) {
                return Status::Corruption("tablet merge recovery order is not strictly increasing");
            }
            previous_recovery_target = target_key;
        }
    }

    result.target_next_rowset_id = cursor;
    return result;
}

Status materialize_planned_rowsets(const TabletMergeAllocationPlan& plan, TabletMetadataPB* target,
                                   CanonicalContribMap* canonical_contribs) {
    TEST_SYNC_POINT_CALLBACK("materialize_planned_rowsets:entry", nullptr);
    g_tablet_merge_non_pk_skip_dedup_total << plan.non_pk_skip_dedup_count;
    for (size_t canonical_index = 0; canonical_index < plan.canonicals.size(); ++canonical_index) {
        const auto& canonical = plan.canonicals[canonical_index];
        if (canonical_index != static_cast<size_t>(target->rowsets_size())) {
            return Status::InternalError("tablet merge canonical plan is out of materialization order");
        }
        RowsetMetadataPB output(canonical.source_form_rowset);
        const auto& projection = plan.projections[canonical.selected_context_index];
        ASSIGN_OR_RETURN(auto mapped_id, projection.map_primary_rssid(output.id()));
        output.set_id(mapped_id);
        if (output.has_max_compact_input_rowset_id()) {
            ASSIGN_OR_RETURN(auto mapped, projection.map_primary_rssid(output.max_compact_input_rowset_id()));
            output.set_max_compact_input_rowset_id(mapped);
        }
        for (auto& del : *output.mutable_del_files()) {
            ASSIGN_OR_RETURN(auto mapped, projection.map_primary_rssid(del.origin_rowset_id()));
            del.set_origin_rowset_id(mapped);
        }
        if (output.has_delete_predicate()) FAIL_POINT_TRIGGER_RETURN_ERROR(tablet_merge_before_delete_predicate_range);
        target->add_rowsets()->Swap(&output);
        if (canonical.schema_id.has_value()) {
            (*target->mutable_rowset_to_schema())[mapped_id] = *canonical.schema_id;
        }
        if (canonical_contribs != nullptr && !canonical.source_form_rowset.has_delete_predicate()) {
            (*canonical_contribs)[canonical_index] = canonical.contributor_ranges;
        }
    }
    return Status::OK();
}

// Per-entry kind, with the legacy hinge: an absent file_kinds array (or an index past its end)
// means DENSE_COLS. Mirrors DeltaColumnGroup::file_kind(idx).
static DeltaColumnFileKindPB dcg_entry_kind(const DeltaColumnGroupVerPB& dcg, int idx) {
    return idx < dcg.file_kinds_size() ? dcg.file_kinds(idx) : DENSE_COLS;
}

Status validate_dcg_shape(const DeltaColumnGroupVerPB& dcg) {
    if (dcg.GetReflection()->GetUnknownFields(dcg).field_count() != 0) {
        return Status::Corruption("DCG message has unknown fields that cannot be attributed to an entry");
    }
    // Required fields must be equal length
    if (dcg.unique_column_ids_size() != dcg.column_files_size() || dcg.versions_size() != dcg.column_files_size()) {
        return Status::Corruption("DCG shape invalid: column_files/unique_column_ids/versions size mismatch");
    }
    // Optional fields must not exceed column_files length
    if (dcg.encryption_metas_size() > dcg.column_files_size() || dcg.shared_files_size() > dcg.column_files_size() ||
        dcg.column_file_sizes_size() > dcg.column_files_size()) {
        return Status::Corruption("DCG shape invalid: optional fields exceed column_files size");
    }
    // SDCG parallel arrays must be either empty (legacy: all DENSE / unknown K) or exactly 1:1 with
    // column_files. Anything in between would break positional indexing.
    if (dcg.file_kinds_size() != 0 && dcg.file_kinds_size() != dcg.column_files_size()) {
        return Status::Corruption("DCG shape invalid: file_kinds size neither 0 nor column_files size");
    }
    if (dcg.sparse_row_counts_size() != 0 && dcg.sparse_row_counts_size() != dcg.column_files_size()) {
        return Status::Corruption("DCG shape invalid: sparse_row_counts size neither 0 nor column_files size");
    }
    if (dcg.presences_size() != 0 && dcg.presences_size() != dcg.column_files_size()) {
        return Status::Corruption("DCG shape invalid: presences size neither 0 nor column_files size");
    }
    // Per-column presence lists (packed flexible files): empty (legacy / no packed file) or 1:1.
    if (dcg.column_presence_lists_size() != 0 && dcg.column_presence_lists_size() != dcg.column_files_size()) {
        return Status::Corruption("DCG shape invalid: column_presence_lists size neither 0 nor column_files size");
    }
    // Duplicate column UID across entries: legal for sparse chains (col_a sparse at v2, v3, ...), where
    // at least one involved entry's file (for that uid) is SPARSE. Two DENSE entries sharing a uid is a
    // genuine conflict (two independent full rewrites of the same column) and stays Corruption.
    std::unordered_map<uint32_t, bool> uid_has_dense_entry; // uid -> seen a DENSE entry claiming it
    for (int idx = 0; idx < dcg.unique_column_ids_size(); ++idx) {
        const bool entry_is_dense = dcg_entry_kind(dcg, idx) == DENSE_COLS;
        for (auto cid : dcg.unique_column_ids(idx).column_ids()) {
            auto [it, inserted] = uid_has_dense_entry.try_emplace(cid, entry_is_dense);
            if (!inserted) {
                if (it->second && entry_is_dense) {
                    return Status::Corruption("DCG contains duplicate column UID across two DENSE entries");
                }
                it->second = it->second || entry_is_dense;
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
    // Pad with 0 (unknown) so column_file_sizes stays 1:1 with column_files; readers
    // treat 0 as "size unknown" and fall back to stat/HeadObject.
    while (dcg->column_file_sizes_size() < dcg->column_files_size()) {
        dcg->add_column_file_sizes(0);
    }
    // SDCG: pad file_kinds to DENSE_COLS and sparse_row_counts to 0 so downstream index access is
    // uniform. Legacy metas (no kinds) normalize to all-DENSE, preserving today's semantics.
    while (dcg->file_kinds_size() < dcg->column_files_size()) {
        dcg->add_file_kinds(DENSE_COLS);
    }
    while (dcg->sparse_row_counts_size() < dcg->column_files_size()) {
        dcg->add_sparse_row_counts(0);
    }
    // Pad presences with empty (all-unknown) summaries so the array stays 1:1 with column_files.
    while (dcg->presences_size() < dcg->column_files_size()) {
        dcg->add_presences();
    }
    // SDCG per-column presence lists: only normalized to 1:1 when the entry already carries ANY (a packed
    // flexible file is present). When absent (homogeneous / legacy), leave it empty so byte-identical metas
    // stay byte-identical and the reader falls back to file-level presence for every entry.
    if (dcg->column_presence_lists_size() > 0) {
        while (dcg->column_presence_lists_size() < dcg->column_files_size()) {
            dcg->add_column_presence_lists();
        }
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
    // SDCG: the same physical file must have the same kind and (for sparse) the same K across metas.
    // Use the legacy hinge so a normalized side and a not-yet-normalized side still compare correctly.
    if (dcg_entry_kind(existing, j) != dcg_entry_kind(incoming, i)) {
        return Status::Corruption("DCG same column_file but file_kinds differ");
    }
    const int64_t e_k = j < existing.sparse_row_counts_size() ? existing.sparse_row_counts(j) : 0;
    const int64_t i_k = i < incoming.sparse_row_counts_size() ? incoming.sparse_row_counts(i) : 0;
    if (e_k != i_k) {
        return Status::Corruption("DCG same column_file but sparse_row_counts differ");
    }
    // SDCG: the same physical file must carry the same presence summary. Compare with the legacy hinge so
    // a side that predates the presences field (absent slot == empty/unknown) still matches an empty slot.
    const SparsePresencePB e_p = j < existing.presences_size() ? existing.presences(j) : SparsePresencePB();
    const SparsePresencePB i_p = i < incoming.presences_size() ? incoming.presences(i) : SparsePresencePB();
    if (e_p.min_source_rowid() != i_p.min_source_rowid() || e_p.max_source_rowid() != i_p.max_source_rowid() ||
        e_p.row_count() != i_p.row_count() || e_p.has_min_source_rowid() != i_p.has_min_source_rowid() ||
        e_p.has_max_source_rowid() != i_p.has_max_source_rowid() || e_p.has_row_count() != i_p.has_row_count()) {
        return Status::Corruption("DCG same column_file but presences differ");
    }
    // SDCG: the same physical packed file must carry the same per-column presence list. Compare with the
    // legacy hinge (absent slot == empty list) by serialized bytes (deterministic for the same content).
    const ColumnPresenceListPB e_cpl =
            j < existing.column_presence_lists_size() ? existing.column_presence_lists(j) : ColumnPresenceListPB();
    const ColumnPresenceListPB i_cpl =
            i < incoming.column_presence_lists_size() ? incoming.column_presence_lists(i) : ColumnPresenceListPB();
    if (e_cpl.SerializeAsString() != i_cpl.SerializeAsString()) {
        return Status::Corruption("DCG same column_file but column_presence_lists differ");
    }
    return Status::OK();
}

// ---------------------------------------------------------------------------
// merge_dcg_meta: two-pass entry-level merge with per-target .cols rebuild.
//
// Pass 1 collects per-target "surviving" entries (after exact-dedup by .cols
// filename) and per-target source-rowset references (the old tablet rowsets that
// reference target rssid T through get_rssid/map_rssid). Ranges are captured
// from each source old tablet rowset BEFORE canonical materialization has widened shared
// ranges via union_range, so a coverage gap between old tablet ranges cannot be
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
// compute row windows per source old tablet rowset using the existing range ->
// SeekRange -> rowid-range pipeline, validate coverage, assemble rebuilt
// chunk (donor file per column + updater old tablet window overrides), write a
// new .cols file, install one entry into new_dcgs[T].

struct DcgSurvivingEntry {
    size_t old_tablet_index;
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
    size_t old_tablet_index;
    const TabletRangePB* effective_range = nullptr; // rowset.range() else ctx tablet range; null = unbounded
};

struct DcgTargetWorkItem {
    std::vector<DcgSurvivingEntry> entries;
    std::vector<DcgSourceRowsetReference> source_refs;
};

struct TargetSegmentDeclaration {
    std::string physical_base_key;
    bool shared = false;
};

DeltaColumnGroupVerPB make_single_entry_dcg(const DeltaColumnGroupVerPB& source, int entry_index) {
    DeltaColumnGroupVerPB out;
    out.add_column_files(source.column_files(entry_index));
    out.add_unique_column_ids()->CopyFrom(source.unique_column_ids(entry_index));
    out.add_versions(source.versions(entry_index));
    out.add_encryption_metas(source.encryption_metas(entry_index));
    out.add_shared_files(source.shared_files(entry_index));
    // source is normalized before this call, so column_file_sizes / file_kinds / sparse_row_counts
    // are all 1:1 with column_files. Carry the SDCG arrays so the single-entry copy keeps its kind/K.
    out.add_column_file_sizes(source.column_file_sizes(entry_index));
    out.add_file_kinds(source.file_kinds(entry_index));
    out.add_sparse_row_counts(source.sparse_row_counts(entry_index));
    // source is normalized => presences is 1:1 with column_files; carry the per-file summary.
    out.add_presences()->CopyFrom(source.presences(entry_index));
    // Per-column presence list (packed flexible files). Only 1:1 when the source carries any; carry the
    // matching entry so the single-entry copy keeps its per-column apply gate. Absent => leave empty.
    if (entry_index < source.column_presence_lists_size()) {
        out.add_column_presence_lists()->CopyFrom(source.column_presence_lists(entry_index));
    }
    // source_segment_num_rows is a per-segment scalar; preserve it on the single-entry copy so the
    // merge can reconcile it across siblings of the same target rssid.
    if (source.has_source_segment_num_rows()) {
        out.set_source_segment_num_rows(source.source_segment_num_rows());
    }
    return out;
}

// True iff this surviving entry's single file is a sparse `.spcols` overlay.
inline bool entry_is_sparse(const DcgSurvivingEntry& entry) {
    const auto& e = entry.single_entry;
    return e.file_kinds_size() > 0 && e.file_kinds(0) == SPARSE_PERCOL;
}

std::unordered_map<uint32_t, bool> collect_target_rssid_ownership(const TabletMetadataPB& metadata) {
    std::unordered_map<uint32_t, bool> result;
    for (const auto& rowset : metadata.rowsets()) {
        for (int position = 0; position < rowset.segment_metas_size(); ++position) {
            result[get_rssid(rowset, position)] = rowset.segment_metas(position).shared();
        }
    }
    return result;
}

std::unordered_map<uint32_t, TargetSegmentDeclaration> collect_target_segment_declarations(
        const TabletMetadataPB& metadata) {
    std::unordered_map<uint32_t, TargetSegmentDeclaration> result;
    for (const auto& rowset : metadata.rowsets()) {
        for (int position = 0; position < rowset.segment_metas_size(); ++position) {
            const auto& segment = rowset.segment_metas(position);
            result[get_rssid(rowset, position)] =
                    TargetSegmentDeclaration{normalized_physical_base_key(segment), segment.shared()};
        }
    }
    return result;
}

// Pass 1 — walk each old tablet's dcg_meta and rowsets, dedup by filename across
// old tablets, and accumulate source-rowset refs per target T.
Status dcg_pass1_collect_entries_and_sources(const std::vector<TabletMergeContext>& merge_contexts,
                                             const TabletMergeAllocationPlan& allocation_plan,
                                             const std::unordered_map<uint32_t, bool>& target_rssid_ownership,
                                             std::map<uint32_t, DcgTargetWorkItem>* work_by_target) {
    // Track which .cols filenames we have already observed per target so that
    // subsequent old tablets with the same filename are deduped (and verified).
    // Store size_t indexes into DcgTargetWorkItem::entries (NOT raw pointers),
    // since push_back can reallocate the vector and invalidate pointers.
    std::map<uint32_t, std::unordered_map<std::string, size_t>> seen_files_by_target;

    for (size_t old_tablet_index = 0; old_tablet_index < merge_contexts.size(); ++old_tablet_index) {
        const auto& context = merge_contexts[old_tablet_index];

        // (a) Accumulate source-rowset references from every rowset that
        // references any target via get_rssid -> context.map_rssid.
        for (const auto& rowset : context.metadata()->rowsets()) {
            for (int segment_position = 0; segment_position < rowset.segment_metas_size(); ++segment_position) {
                uint32_t original_rssid = get_rssid(rowset, segment_position);
                auto target_or = allocation_plan.projections[old_tablet_index].map_occurrence_rssid(original_rssid);
                if (!target_or.ok()) {
                    return Status::Corruption(
                            fmt::format("tablet merge source-live DCG RSSID {} has no target", original_rssid));
                }
                uint32_t target_rssid = *target_or;
                bool force_target_omission = false;
                TEST_SYNC_POINT_CALLBACK("tablet_merge_test:force_dcg_target_omission", &force_target_omission);
                if (force_target_omission) {
                    return Status::Corruption(
                            fmt::format("tablet merge source-live DCG RSSID {} target was omitted", original_rssid));
                }
                DcgSourceRowsetReference source_reference;
                source_reference.old_tablet_index = old_tablet_index;
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
            ASSIGN_OR_RETURN(uint32_t target_rssid,
                             allocation_plan.projections[old_tablet_index].map_occurrence_rssid(segment_id));
            auto target_ownership = target_rssid_ownership.find(target_rssid);
            if (target_ownership == target_rssid_ownership.end()) {
                return Status::Corruption(fmt::format("tablet merge source-live DCG RSSID {} maps to missing target {}",
                                                      segment_id, target_rssid));
            }

            DeltaColumnGroupVerPB normalized = dcg_value;
            RETURN_IF_ERROR(validate_dcg_shape(normalized));
            normalize_dcg_optional_fields(&normalized);

            for (int entry_index = 0; entry_index < normalized.column_files_size(); ++entry_index) {
                normalized.set_shared_files(entry_index, target_ownership->second);
                const std::string& file_name = normalized.column_files(entry_index);

                auto& target_work = (*work_by_target)[target_rssid];
                auto& seen_files = seen_files_by_target[target_rssid];
                auto seen_iter = seen_files.find(file_name);
                if (seen_iter != seen_files.end()) {
                    // Exact dedup across old tablets: verify entry-level consistency
                    // against the previously stored entry. Index lookup is safe
                    // even if the vector reallocated between insertions.
                    RETURN_IF_ERROR(verify_dcg_entry_consistency(target_work.entries[seen_iter->second].single_entry, 0,
                                                                 normalized, entry_index));
                    continue;
                }

                DcgSurvivingEntry entry;
                entry.old_tablet_index = old_tablet_index;
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
    for (int segment_position = 0; segment_position < rowset.segment_metas_size(); ++segment_position) {
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

using tablet_reshard_helper::DcgRowWindow;

// Step C — compute row windows in the target segment for every source old tablet
// rowset that references it. Holes left by a compacted-away old tablet are
// accepted as is_gap windows when they fall within |gap_bits| (the same
// synthesized bitmap merge_delvecs masks); a nullptr |gap_bits| keeps the strict
// contiguous-coverage requirement. The opened base segment and its row count are
// returned so the caller can fill gap rows from it.
Status compute_row_windows_for_source_rowsets(TabletManager* tablet_manager, int64_t new_tablet_id,
                                              const RowsetMetadataPB& target_rowset, int target_segment_position,
                                              const TabletSchemaCSPtr& full_tablet_schema,
                                              const TabletSchemaCSPtr& current_schema,
                                              const std::vector<DcgSourceRowsetReference>& source_references,
                                              const roaring::Roaring* gap_bits,
                                              std::shared_ptr<Segment>* out_base_segment,
                                              std::vector<DcgRowWindow>* out_windows) {
    // Open base segment for index lookups.
    FileInfo base_segment_file_info;
    const auto& target_seg_meta = target_rowset.segment_metas(target_segment_position);
    base_segment_file_info.path = tablet_manager->segment_location(new_tablet_id, target_seg_meta.filename());
    if (target_seg_meta.has_size()) {
        base_segment_file_info.size = target_seg_meta.size();
    }
    if (target_seg_meta.has_bundle_file_offset()) {
        base_segment_file_info.bundle_file_offset = target_seg_meta.bundle_file_offset();
    }
    if (target_seg_meta.has_encryption_meta()) {
        base_segment_file_info.encryption_meta = target_seg_meta.encryption_meta();
    }

    ASSIGN_OR_RETURN(auto file_system, FileSystemFactory::CreateSharedFromString(base_segment_file_info.path));
    ASSIGN_OR_RETURN(auto base_segment,
                     Segment::open(file_system, base_segment_file_info, /*segment_id=*/0, full_tablet_schema,
                                   /*footer_length_hint=*/nullptr, /*partial_rowset_footer=*/nullptr,
                                   /*lake_io_opts=*/LakeIOOptions{}, tablet_manager));

    const rowid_t num_rows_in_target = static_cast<rowid_t>(base_segment->num_rows());
    *out_base_segment = base_segment;

    out_windows->clear();
    out_windows->reserve(source_references.size());

    for (const auto& source_reference : source_references) {
        Range<rowid_t> window{0, num_rows_in_target};
        if (source_reference.effective_range != nullptr) {
            // Decode with full_tablet_schema -- the schema the base segment is opened with above -- so the
            // SeekRange's positional field ids align with that segment. A reshard that ran after a
            // metadata-only trailing key add can stamp a per-rowset range at a larger sort-key arity than
            // this rowset's schema; create_seek_range_from then projects the wider bound onto this segment's
            // sort key, comparing the added columns' defaults (from current_schema) against the dropped
            // trailing bound values so a boundary-prefix row routes exactly as under the full-arity range.
            ASSIGN_OR_RETURN(auto seek_range, TabletRangeHelper::create_seek_range_from(
                                                      *source_reference.effective_range, full_tablet_schema,
                                                      /*mem_pool=*/nullptr, current_schema));
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
        out_windows->push_back({source_reference.old_tablet_index, window});
    }

    // Dedup windows that belong to the SAME old tablet AND have the same range
    // (e.g., an old tablet's shared rowset surfaced twice through different scans).
    // Do NOT dedup windows from different old tablets even if the range matches:
    // those represent distinct authoritative updaters for the same rows and
    // must surface as an overlap failure, not be silently collapsed.
    std::sort(out_windows->begin(), out_windows->end(), [](const DcgRowWindow& left, const DcgRowWindow& right) {
        if (left.range.begin() != right.range.begin()) return left.range.begin() < right.range.begin();
        if (left.range.end() != right.range.end()) return left.range.end() < right.range.end();
        return left.old_tablet_index < right.old_tablet_index;
    });
    std::vector<DcgRowWindow> deduped_windows;
    deduped_windows.reserve(out_windows->size());
    for (auto& window : *out_windows) {
        if (!deduped_windows.empty() && deduped_windows.back().range.begin() == window.range.begin() &&
            deduped_windows.back().range.end() == window.range.end() &&
            deduped_windows.back().old_tablet_index == window.old_tablet_index) {
            continue; // same old tablet + same range: safe to collapse
        }
        deduped_windows.push_back(window);
    }
    *out_windows = std::move(deduped_windows);

    // Reconcile the contributor windows with the synthesized gap bitmap: a hole
    // left by a compacted-away old tablet becomes an is_gap window when it falls
    // entirely within |gap_bits| (those rowids are masked by merge_delvecs and
    // never returned). Unmasked holes, distinct-owner overlaps, and zero rows
    // surface as errors. With |gap_bits|==nullptr any hole fails, which is the
    // original strict contiguous-coverage behavior.
    std::vector<DcgRowWindow> reconciled_windows;
    RETURN_IF_ERROR(tablet_reshard_helper::reconcile_windows_with_gap(*out_windows, gap_bits, num_rows_in_target,
                                                                      &reconciled_windows));
    *out_windows = std::move(reconciled_windows);
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
DEFINE_FAIL_POINT(tablet_merge_after_write_dcg_cols);
StatusOr<DeltaColumnGroupVerPB> rebuild_dcg_for_target_segment(
        TabletManager* tablet_manager, const std::vector<TabletMergeContext>& merge_contexts, int64_t new_tablet_id,
        int64_t new_version, int64_t txn_id, const TabletMetadataPB& new_metadata, uint32_t target_rssid,
        const std::vector<uint32_t>& rebuild_columns, const std::vector<const DcgSurvivingEntry*>& conflicting_entries,
        const std::vector<DcgSourceRowsetReference>& source_references, const roaring::Roaring* gap_bits) {
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

    // Step C — compute row windows. `current_schema` (the current tablet schema, which contains any
    // later-added trailing key columns) lets a source range written at a wider sort-key arity than
    // full_tablet_schema be projected onto the base segment's sort key using those columns' defaults.
    TabletSchemaCSPtr current_schema =
            new_metadata.has_schema() ? TabletSchema::create(new_metadata.schema()) : full_tablet_schema;
    std::vector<DcgRowWindow> windows;
    std::shared_ptr<Segment> base_segment;
    RETURN_IF_ERROR(compute_row_windows_for_source_rowsets(tablet_manager, new_tablet_id, target_rowset,
                                                           target_segment_position, full_tablet_schema, current_schema,
                                                           source_references, gap_bits, &base_segment, &windows));
    const rowid_t num_rows_in_target = static_cast<rowid_t>(base_segment->num_rows());

    // For each rebuild column, pick:
    // - default donor: any conflicting entry that claims the UID (first found).
    // - per-old-tablet overrides: the conflicting entry from the old tablet that claims
    //   the UID, to be used for rows in that old tablet's owner window.
    struct ColumnSourceInfo {
        const DcgSurvivingEntry* default_donor = nullptr;
        std::unordered_map<size_t, const DcgSurvivingEntry*> override_by_old_tablet_index;
    };
    std::unordered_map<uint32_t, ColumnSourceInfo> column_source_info_by_unique_id;
    for (uint32_t unique_id : rebuild_columns) {
        for (const DcgSurvivingEntry* entry : conflicting_entries) {
            if (!entry_claims_column_uid(*entry, unique_id)) continue;
            auto& info = column_source_info_by_unique_id[unique_id];
            if (info.default_donor == nullptr) info.default_donor = entry;
            info.override_by_old_tablet_index[entry->old_tablet_index] = entry;
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
        int64_t owner_tablet_id = merge_contexts[entry->old_tablet_index].metadata()->id();
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

        auto field = StorageSchemaHelper::convert_field(column_index, tablet_column);
        MutableColumnPtr output_column = ChunkFactory::column_from_field(field);
        output_column->reserve(num_rows_in_target);

        for (const auto& window : windows) {
            if (window.is_gap) {
                // No source DCG entry claims these rows (their old tablet was
                // compacted away). They are masked by the merge gap delvec and
                // never returned, so fill from the canonical base segment: real,
                // already-indexed values keep the rebuilt .cols encodings and
                // secondary indexes valid.
                RETURN_IF_ERROR(read_column_range_from_segment(base_segment, full_tablet_schema, unique_id,
                                                               window.range.begin(), window.range.end(),
                                                               output_column.get()));
                continue;
            }
            const DcgSurvivingEntry* selected_source = nullptr;
            auto override_iter = source_info.override_by_old_tablet_index.find(window.old_tablet_index);
            selected_source = (override_iter != source_info.override_by_old_tablet_index.end())
                                      ? override_iter->second
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

    // The rebuilt .cols segment is now durable but nothing references it yet: the orphan-file window.
    //
    // Note the caller does NOT clean this particular file up. merge_dcg_meta appends the rebuilt path
    // to rebuilt_file_paths only AFTER this function returns OK, so an error injected here returns
    // before the caller learns the filename and cleanup_on_failure() cannot delete it -- the file is
    // left for ordinary orphan-file vacuum. That is not a defect to engineer around: producing an
    // unreferenced file is exactly what this hook exists to let a test observe. It does mean a
    // garbage-file check run immediately after an armed merge sees this file until vacuum runs.
    FAIL_POINT_TRIGGER_RETURN_ERROR(tablet_merge_after_write_dcg_cols);

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
    // Record the size of the freshly written .cols file so readers can skip a stat/HeadObject.
    rebuilt.add_column_file_sizes(static_cast<int64_t>(written_file_size));
    return rebuilt;
}

// Phase 0 output: per-target rowid bitmaps representing keys in the
// shared physical segment that no contributing old tablet claims. Read-path
// consumers:
//   - canonical R0's segment iterator already filters by canonical.range, so
//     gap rowids outside the convex hull are no-ops for scans.
//   - PersistentIndexSstable::multi_get filters by the projected delvec on
//     the sstable PB regardless of LSM block-sort order, which keeps the
//     first-old-tablet-compacts case safe.
//   - DCG rebuild (merge_dcg_meta) consults the same bitmap to accept gap holes
//     in row-window coverage and fill those rows from the base segment.
struct CanonicalGapSpec {
    uint32_t target_rssid;
    Roaring gap_bits;
};

Status merge_dcg_meta(TabletManager* tablet_manager, const std::vector<TabletMergeContext>& merge_contexts,
                      const TabletMergeAllocationPlan& allocation_plan, int64_t new_tablet_id, int64_t new_version,
                      int64_t txn_id, const std::vector<CanonicalGapSpec>& gap_specs, TabletMetadataPB* new_metadata) {
    std::map<uint32_t, DcgTargetWorkItem> work_by_target;
    const auto target_rssid_ownership = collect_target_rssid_ownership(*new_metadata);
    RETURN_IF_ERROR(dcg_pass1_collect_entries_and_sources(merge_contexts, allocation_plan, target_rssid_ownership,
                                                          &work_by_target));

    // Index synthesized gap bitmaps by target rssid so a rebuild can short-circuit
    // its coverage check for rowids that merge_delvecs masks. Empty for non-PK
    // tables (no gap synthesis) => every lookup misses => strict coverage.
    std::unordered_map<uint32_t, const Roaring*> gap_bits_by_target;
    gap_bits_by_target.reserve(gap_specs.size());
    for (const auto& spec : gap_specs) {
        gap_bits_by_target.emplace(spec.target_rssid, &spec.gap_bits);
    }

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
        // Accumulate SDCG kinds/counts in emission order; attach to final_dcg only if this target carries
        // sparse content (zero-regression: dense-only split merge keeps absent arrays => byte-identical).
        std::vector<DeltaColumnFileKindPB> emitted_kinds;
        std::vector<int64_t> emitted_sparse_counts;
        // Presence summary per emitted entry, kept in lockstep with emitted_kinds.
        std::vector<SparsePresencePB> emitted_presences;
        // Per-column presence list per emitted entry (packed flexible files), in lockstep with emitted_kinds.
        std::vector<ColumnPresenceListPB> emitted_column_presence_lists;
        bool any_column_presence = false;
        bool sdcg_active = false;
        int64_t target_source_num_rows = 0;

        // Emit non-conflicting entries unchanged.
        for (size_t entry_index = 0; entry_index < target_work.entries.size(); ++entry_index) {
            if (entry_is_conflicting[entry_index]) continue;
            const auto& entry = target_work.entries[entry_index];
            final_dcg.add_column_files(entry.single_entry.column_files(0));
            final_dcg.add_unique_column_ids()->CopyFrom(entry.single_entry.unique_column_ids(0));
            final_dcg.add_versions(entry.single_entry.versions(0));
            final_dcg.add_encryption_metas(entry.single_entry.encryption_metas(0));
            final_dcg.add_shared_files(entry.single_entry.shared_files(0));
            final_dcg.add_column_file_sizes(entry.single_entry.column_file_sizes(0));
            // SDCG: carry kind/K so a non-conflicting `.spcols` keeps its sparse identity through the
            // merge. make_single_entry_dcg populated these 1:1, so index 0 is always present here.
            const DeltaColumnFileKindPB kind = entry.single_entry.file_kinds(0);
            emitted_kinds.push_back(kind);
            emitted_sparse_counts.push_back(entry.single_entry.sparse_row_counts(0));
            // Carry the per-file presence summary; make_single_entry_dcg populated index 0.
            emitted_presences.push_back(entry.single_entry.presences(0));
            // Carry the per-column presence list (packed flexible file) if present; else pad empty.
            ColumnPresenceListPB cpl = entry.single_entry.column_presence_lists_size() > 0
                                               ? entry.single_entry.column_presence_lists(0)
                                               : ColumnPresenceListPB();
            if (cpl.entries_size() > 0) any_column_presence = true;
            emitted_column_presence_lists.push_back(std::move(cpl));
            if (kind == SPARSE_PERCOL) {
                sdcg_active = true;
            }
            // Reconcile the per-segment base row count across siblings of the same target rssid.
            if (entry.single_entry.has_source_segment_num_rows()) {
                target_source_num_rows = entry.single_entry.source_segment_num_rows();
                sdcg_active = true;
            }
        }

        bool any_entry_is_conflicting = false;
        for (bool conflicting : entry_is_conflicting) any_entry_is_conflicting |= conflicting;

        if (any_entry_is_conflicting) {
            // SDCG PoC limitation: the rebuild path folds conflicting entries into one dense `.cols`
            // by reading donor files positionally (base rowid == ordinal). That contract holds only for
            // dense files; a `.spcols` overlay stores values at local ordinals 0..K-1 keyed by a
            // source_rowid column, so folding it positionally would silently corrupt data. When any
            // conflicting (overlapping-uid) entry is sparse, refuse to chain-merge in this split path.
            for (size_t entry_index = 0; entry_index < target_work.entries.size(); ++entry_index) {
                if (entry_is_conflicting[entry_index] && entry_is_sparse(target_work.entries[entry_index])) {
                    cleanup_on_failure();
                    return Status::NotSupported(
                            "tablet split merge of overlapping sparse delta column group (.spcols) layers is "
                            "not supported in this build; this is a known limitation of the enable_sparse_dcg "
                            "PoC path");
                }
            }
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

            const Roaring* target_gap_bits = nullptr;
            if (auto gap_iter = gap_bits_by_target.find(target_rssid); gap_iter != gap_bits_by_target.end()) {
                target_gap_bits = gap_iter->second;
            }
            StatusOr<DeltaColumnGroupVerPB> rebuilt_or_status = rebuild_dcg_for_target_segment(
                    tablet_manager, merge_contexts, new_tablet_id, new_version, txn_id, *new_metadata, target_rssid,
                    rebuild_columns, conflicting_entries, target_work.source_refs, target_gap_bits);
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
            final_dcg.add_column_file_sizes(rebuilt_entry.column_file_sizes(0));
            // The rebuild materializes a brand-new row-complete dense `.cols` file (sparse inputs were
            // already rejected above), so it is always DENSE with no sparse row count / no presence.
            emitted_kinds.push_back(DENSE_COLS);
            emitted_sparse_counts.push_back(0);
            emitted_presences.push_back(SparsePresencePB());
            // Rebuilt dense file has no per-column presence (it is row-complete); pad empty.
            emitted_column_presence_lists.push_back(ColumnPresenceListPB());
            g_tablet_merge_dcg_rebuild_total << 1;
        }

        // Skip targets that ended up empty (no files).
        if (final_dcg.column_files_size() == 0) continue;
        // Attach SDCG arrays only when sparse content is present; keep dense-only merges byte-identical.
        if (sdcg_active) {
            DCHECK_EQ(static_cast<int>(emitted_kinds.size()), final_dcg.column_files_size());
            DCHECK_EQ(emitted_presences.size(), emitted_kinds.size());
            DCHECK_EQ(emitted_column_presence_lists.size(), emitted_kinds.size());
            for (size_t i = 0; i < emitted_kinds.size(); ++i) {
                final_dcg.add_file_kinds(emitted_kinds[i]);
                final_dcg.add_sparse_row_counts(emitted_sparse_counts[i]);
                final_dcg.add_presences()->CopyFrom(emitted_presences[i]);
            }
            // Per-column presence lists emitted 1:1 ONLY when at least one packed file carries one.
            if (any_column_presence) {
                for (size_t i = 0; i < emitted_column_presence_lists.size(); ++i) {
                    final_dcg.add_column_presence_lists()->CopyFrom(emitted_column_presence_lists[i]);
                }
            }
            if (target_source_num_rows > 0) {
                final_dcg.set_source_segment_num_rows(target_source_num_rows);
            }
        }
        auto shape_status = validate_dcg_shape(final_dcg);
        if (!shape_status.ok()) {
            cleanup_on_failure();
            return shape_status;
        }
        (*merged_dcgs)[target_rssid] = std::move(final_dcg);
    }

    return Status::OK();
}

// Pack (col_uid, index_type) into a 64-bit key, mirroring index_delta_group_loader.cpp,
// so tombstone-set membership can be tested cheaply.
inline uint64_t idg_pack_key(int32_t col_uid, IndexType type) {
    return (static_cast<uint64_t>(static_cast<uint32_t>(col_uid)) << 32) | static_cast<uint32_t>(type);
}

// Union |from|'s dropped_keys tombstones into |into| (dedup by packed key). Used when the
// same physical .idx appears under one target from multiple split-family siblings whose
// DROP INDEX history diverged: a key dropped in ANY sibling must stay dropped (DROP INDEX
// is table-wide and monotonic), so first-wins cannot be allowed to resurrect it.
void union_idg_dropped_keys(IndexDeltaGroupEntryPB* into, const IndexDeltaGroupEntryPB& from) {
    std::unordered_set<uint64_t> present;
    for (const auto& dk : into->dropped_keys()) present.insert(idg_pack_key(dk.col_unique_id(), dk.index_type()));
    for (const auto& dk : from.dropped_keys()) {
        if (present.insert(idg_pack_key(dk.col_unique_id(), dk.index_type())).second) {
            *into->add_dropped_keys() = dk;
        }
    }
}

// True iff |e| still has at least one active (non-tombstoned) key. Mirrors the read-path
// loader's active-key computation (index_delta_group_loader.cpp). A fully-tombstoned entry
// is logically dead: the loader skips it, AND vacuum relies on the invariant that idg_meta
// never holds a fully-tombstoned entry (vacuum.cpp: normally apply_drop_index moves such
// .idx to orphan_files). Merge must not install one or the .idx would look live forever.
bool idg_entry_has_active_key(const IndexDeltaGroupEntryPB& e) {
    std::unordered_set<uint64_t> dropped;
    for (const auto& dk : e.dropped_keys()) dropped.insert(idg_pack_key(dk.col_unique_id(), dk.index_type()));
    for (const auto& k : e.keys()) {
        if (dropped.find(idg_pack_key(k.col_unique_id(), k.index_type())) == dropped.end()) return true;
    }
    return false;
}

std::unordered_set<uint32_t> collect_live_rssids(const TabletMetadataPB& metadata) {
    std::unordered_set<uint32_t> live_rssids;
    for (const auto& rowset : metadata.rowsets()) {
        for (int segment_pos = 0; segment_pos < rowset.segment_metas_size(); ++segment_pos) {
            live_rssids.insert(get_rssid(rowset, segment_pos));
        }
    }
    return live_rssids;
}

// merge_idg_meta: remap each source tablet's IDG (.idx) entries into the merged tablet's
// rssid space, dedup by .idx filename per target segment, and keep them newest-version
// first. Unlike merge_dcg_meta there is no rebuild-on-conflict path: an .idx indexes
// unchanging shared segment data, so every source's entry for a given target rssid is built
// over the same physical segment and is interchangeable; the read-path loader picks the
// newest visible version per key and honors tombstones.
//
// An entry is kept only if BOTH its segment_id is a live segment in its OWN source tablet
// (source-live) AND its remapped target is a live segment in the merged tablet (target-live).
// Occurrence projection alone is not a "segment survived" test: a stale idg entry (segment pruned or
// compacted out of that source but the entry not cleaned) could otherwise remap -- e.g. the
// canonical child's stale rssid R maps via R+0 onto a target R that is live only because a
// sibling supplies that segment -- and mis-apply / dangle a .idx that source no longer owns.
// Stale entries are dropped; their .idx becomes an orphan reclaimed by the shared-file
// cleanup once the source tablets are dropped. Writes no new files, so there is no
// partial-write cleanup to do.
Status merge_idg_meta(const std::vector<TabletMergeContext>& merge_contexts,
                      const TabletMergeAllocationPlan& allocation_plan, TabletMetadataPB* new_metadata) {
    // Every segment present after merge_rowsets, mapped to its physical-base declaration. An IDG entry is
    // kept only if its remapped target is here (target-live), and its .idx shared_file is
    // DERIVED from the target segment's shared flag (see the emit loop) rather than preserved
    // from the source: an .idx is shared iff the segment it indexes is shared, so the two must
    // agree. Preserving a source flag would carry a stale value (e.g. a tablet split before
    // this fix has segment.shared=true but idg.shared_file=false, because the old split marked
    // segments shared but not idg) and later mis-route the .idx at vacuum time.
    const auto target_segments = collect_target_segment_declarations(*new_metadata);

    struct TargetWorkItem {
        const TargetSegmentDeclaration* declaration = nullptr;
        std::vector<IndexDeltaGroupEntryPB> entries;
        std::unordered_map<std::string, size_t> seen_files;
    };
    std::map<uint32_t, TargetWorkItem> work_by_target;

    for (size_t context_index = 0; context_index < merge_contexts.size(); ++context_index) {
        const auto& context = merge_contexts[context_index];
        if (!context.metadata()->has_idg_meta()) continue;
        const auto source_live_rssids = collect_live_rssids(*context.metadata());
        for (const auto& [segment_id, idg_ver] : context.metadata()->idg_meta().idgs()) {
            if (source_live_rssids.find(segment_id) == source_live_rssids.end()) {
                continue; // stale idg entry: segment no longer in this source's rowsets
            }
            ASSIGN_OR_RETURN(uint32_t target_rssid,
                             allocation_plan.projections[context_index].map_occurrence_rssid(segment_id));
            bool force_target_omission = false;
            TEST_SYNC_POINT_CALLBACK("tablet_merge_test:force_idg_target_omission", &force_target_omission);
            if (force_target_omission) {
                return Status::Corruption(
                        fmt::format("tablet merge source-live IDG RSSID {} target was omitted", segment_id));
            }
            auto target_segment = target_segments.find(target_rssid);
            if (target_segment == target_segments.end()) {
                return Status::Corruption(fmt::format("tablet merge source-live IDG RSSID {} maps to missing target {}",
                                                      segment_id, target_rssid));
            }
            auto target_work_it =
                    work_by_target.try_emplace(target_rssid, TargetWorkItem{.declaration = &target_segment->second})
                            .first;
            auto& target_work = target_work_it->second;
            DCHECK(target_work.declaration == &target_segment->second);
            auto& entries = target_work.entries;
            for (const auto& entry : idg_ver.entries()) {
                if (!entry.has_index_file() || entry.index_file().empty()) continue;
                auto seen_it = target_work.seen_files.find(entry.index_file());
                if (seen_it != target_work.seen_files.end()) {
                    union_idg_dropped_keys(&entries[seen_it->second], entry); // same physical .idx
                    continue;
                }
                // shared_file is derived from the target segment's ownership in the emit loop.
                target_work.seen_files[entry.index_file()] = entries.size();
                entries.push_back(entry);
            }
        }
    }

    if (work_by_target.empty()) return Status::OK();

    // DROP INDEX is table-wide monotonic. Co-referenced target RSSIDs that resolve to the same normalized physical base
    // and .idx file must therefore see the union of every tombstone before active/dead and orphan decisions.
    std::map<std::pair<std::string, std::string>, IndexDeltaGroupEntryPB> global_tombstones;
    for (const auto& [target_rssid, target_work] : work_by_target) {
        (void)target_rssid;
        DCHECK(target_work.declaration != nullptr);
        for (const auto& entry : target_work.entries) {
            union_idg_dropped_keys(&global_tombstones[{target_work.declaration->physical_base_key, entry.index_file()}],
                                   entry);
        }
    }
    for (auto& [target_rssid, target_work] : work_by_target) {
        (void)target_rssid;
        DCHECK(target_work.declaration != nullptr);
        for (auto& entry : target_work.entries) {
            union_idg_dropped_keys(
                    &entry, global_tombstones.at({target_work.declaration->physical_base_key, entry.index_file()}));
        }
    }

    auto* merged_idgs = new_metadata->mutable_idg_meta()->mutable_idgs();
    // Fully-tombstoned entries' .idx files are orphan candidates. Collect them first and only
    // orphan a file that NO surviving entry (under ANY target) still references: vacuum deletes
    // orphan_files without consulting live idg_meta, so orphaning a still-referenced file would
    // delete an active index. (.idx names are uuid-unique so a name maps to one target in
    // practice, but resolving against the full surviving set keeps the orphan set provably safe.)
    std::unordered_set<std::string> surviving_files;
    std::map<std::string, FileMetaPB> orphan_candidates;
    for (auto& [target_rssid, target_work] : work_by_target) {
        // Derive the .idx shared flag from the merged segment's ownership: an .idx is shared
        // iff the segment it indexes is shared. This corrects a stale source flag (e.g. from a
        // tablet split before this fix) instead of preserving it, and matches how vacuum treats
        // the segment/.cols for the same rssid.
        DCHECK(target_work.declaration != nullptr);
        const bool tgt_shared = target_work.declaration->shared;
        auto& entries = target_work.entries;
        std::sort(entries.begin(), entries.end(), [](const IndexDeltaGroupEntryPB& a, const IndexDeltaGroupEntryPB& b) {
            return a.version() > b.version();
        });
        // Drop fully-tombstoned entries (all keys dropped after union): keeping one would
        // violate vacuum's no-fully-tombstoned-entry invariant.
        IndexDeltaGroupVerPB ver;
        for (auto& e : entries) {
            if (idg_entry_has_active_key(e)) {
                e.set_shared_file(tgt_shared);
                surviving_files.insert(e.index_file());
                *ver.add_entries() = std::move(e);
            } else if (e.has_index_file() && !e.index_file().empty()) {
                auto& fm = orphan_candidates[e.index_file()];
                fm.set_name(e.index_file());
                if (e.has_file_size()) fm.set_size(e.file_size());
                if (tgt_shared) fm.set_shared(true);
                fm.set_version(e.version());
            }
        }
        if (ver.entries_size() == 0) continue;
        (*merged_idgs)[target_rssid] = std::move(ver);
    }
    // Orphan each dead .idx so vacuum reclaims it, mirroring MetaFileBuilder::apply_drop_index.
    for (auto& [file, fm] : orphan_candidates) {
        if (surviving_files.find(file) == surviving_files.end()) {
            *new_metadata->add_orphan_files() = std::move(fm);
        }
    }
    return Status::OK();
}

// Phase 0: for every PK canonical rowset that owns at least one shared
// segment, mask the rowids whose key falls outside ⋃ contributors but inside
// the merged tablet range.
//
// Bound = merged tablet range, not unbounded `(-∞, +∞)`. The plan's original
// motivation for unbounded was to surface keys outside canonical.range (left
// or right edges); but since each old tablet's tablet.range is a sub-range of the
// pre-split tablet, and merged_tablet.range = union of old tablets' tablet
// ranges, the shared physical segment never carries keys outside the merged
// tablet range. The unbounded helper would just generate edge complements
// that seek to empty rowid windows. Bounded-by-merged-tablet-range is both
// correct and lets us skip the segment open entirely when contributors fully
// cover the merged tablet range (the no-compaction common case).
StatusOr<std::vector<CanonicalGapSpec>> compute_synthesized_gap_specs(TabletManager* tablet_manager,
                                                                      const TabletMetadataPB& new_metadata,
                                                                      const CanonicalContribMap& canonical_contribs) {
    std::vector<CanonicalGapSpec> result;
    // The current tablet schema, used to project a gap range whose bound arity is wider than a canonical
    // rowset's (archived) sort key. This path is primary-key-only and the metadata-only trailing sort-key
    // add excludes primary-key tables, so a wider bound does not arise here today; passing current_schema
    // keeps create_seek_range_from correct and consistent with the other call sites regardless.
    const TabletSchemaCSPtr current_schema =
            new_metadata.has_schema() ? TabletSchema::create(new_metadata.schema()) : nullptr;
    for (const auto& [canonical_index, contrib] : canonical_contribs) {
        if (canonical_index >= static_cast<size_t>(new_metadata.rowsets_size())) {
            return Status::InternalError(
                    fmt::format("compute_synthesized_gap_specs: invalid canonical_index {}", canonical_index));
        }
        const auto& canonical = new_metadata.rowsets(static_cast<int>(canonical_index));
        // segment_metas_size() alone is insufficient because a rowset can own only
        // non-shared segments. Only synthesize gap bits when at least one segment is
        // actually shared.
        bool has_shared = false;
        for (const auto& segment_meta : canonical.segment_metas()) {
            if (segment_meta.shared()) {
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

        for (int seg_pos = 0; seg_pos < canonical.segment_metas_size(); ++seg_pos) {
            const auto& segment_meta = canonical.segment_metas(seg_pos);
            if (!segment_meta.shared()) continue;
            uint32_t target_rssid = get_rssid(canonical, seg_pos);

            FileInfo seg_file_info;
            seg_file_info.path = tablet_manager->segment_location(new_metadata.id(), segment_meta.filename());
            if (segment_meta.has_size()) {
                seg_file_info.size = segment_meta.size();
            }
            if (segment_meta.has_bundle_file_offset()) {
                seg_file_info.bundle_file_offset = segment_meta.bundle_file_offset();
            }
            if (segment_meta.has_encryption_meta()) {
                seg_file_info.encryption_meta = segment_meta.encryption_meta();
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
                ASSIGN_OR_RETURN(auto seek_range, TabletRangeHelper::create_seek_range_from(
                                                          gap_range, schema, /*mem_pool=*/nullptr, current_schema));
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
// is a synthesized Roaring rather than an old tablet delvec page — so no
// (file_name, offset) entry goes into seen_sources.
//
// Uses DelVector::union_with(Roaring) directly (no rowid-vector round-trip):
// a compacted-away old tablet's gap can span millions of rowids, and the legacy
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

bool delvec_file_metadata_matches(const FileMetaPB& left, const FileMetaPB& right) {
    return left.has_size() == right.has_size() && (!left.has_size() || left.size() == right.size()) &&
           left.has_shared() == right.has_shared() && (!left.has_shared() || left.shared() == right.shared());
}

DEFINE_FAIL_POINT(tablet_merge_after_write_delvec);
Status merge_delvecs(TabletManager* tablet_manager, const std::vector<TabletMergeContext>& merge_contexts,
                     const TabletMergeAllocationPlan& allocation_plan,
                     const std::vector<CanonicalGapSpec>& synthesized_gap_specs, int64_t new_version, int64_t txn_id,
                     TabletMetadataPB* new_metadata) {
    // Phase 0 gap bitmaps are synthesized once by the caller (merge_tablet) and
    // shared with merge_dcg_meta so the two paths cannot diverge.

    // Phase 1: Scan pages, build TargetDelvecState for each target rssid.
    // File name is resolved inline via each old tablet's version_to_file map.
    std::map<uint32_t, TargetDelvecState> target_states;
    std::unordered_map<std::string, DelvecFileInfo> actual_page_source_files;
    const auto target_live_rssids = collect_live_rssids(*new_metadata);

    for (size_t context_index = 0; context_index < merge_contexts.size(); ++context_index) {
        const auto& ctx = merge_contexts[context_index];
        if (!ctx.metadata()->has_delvec_meta()) {
            continue;
        }
        const auto source_live_rssids = collect_live_rssids(*ctx.metadata());
        for (const auto& [segment_id, page] : ctx.metadata()->delvec_meta().delvecs()) {
            if (!source_live_rssids.contains(segment_id)) {
                continue;
            }
            ASSIGN_OR_RETURN(uint32_t target,
                             allocation_plan.projections[context_index].map_occurrence_rssid(segment_id));
            bool force_target_omission = false;
            TEST_SYNC_POINT_CALLBACK("tablet_merge_test:force_delvec_target_omission", &force_target_omission);
            if (force_target_omission) {
                return Status::Corruption(
                        fmt::format("tablet merge source-live delvec RSSID {} target was omitted", segment_id));
            }
            if (!target_live_rssids.contains(target)) {
                return Status::Corruption(fmt::format(
                        "tablet merge source-live delvec RSSID {} maps to missing target {}", segment_id, target));
            }

            // Resolve file name from page version
            auto file_it = ctx.metadata()->delvec_meta().version_to_file().find(page.version());
            if (file_it == ctx.metadata()->delvec_meta().version_to_file().end()) {
                return Status::InvalidArgument("Delvec file not found for page version");
            }
            const std::string& file_name = file_it->second.name();
            auto [canonical_file_it, inserted] =
                    actual_page_source_files.emplace(file_name, DelvecFileInfo{ctx.metadata()->id(), file_it->second});
            if (!inserted) {
                const auto& canonical_file = canonical_file_it->second.delvec_file;
                const auto& incoming_file = file_it->second;
                if (!delvec_file_metadata_matches(canonical_file, incoming_file)) {
                    return Status::Corruption(
                            fmt::format("Delvec actual page source metadata mismatch for file {} between tablets {} "
                                        "and {}",
                                        file_name, canonical_file_it->second.tablet_id, ctx.metadata()->id()));
                }
            }
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
                    // Dedup hit: same file_name + offset. File metadata was
                    // already validated through actual_page_source_files.
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
    // Phase 1.5: inject synthesized gap delvecs into target_states. Each spec's
    // bitmap masks rowids in the shared physical segment whose key was
    // contributed by no surviving old tablet (e.g., an old tablet that compacted away
    // its copy of the shared rowset). Promotes any single_source state to
    // merged so the bitmap can be unioned in.
    RETURN_IF_ERROR(inject_synthesized_gaps_into_target_states(tablet_manager, synthesized_gap_specs, new_version,
                                                               &target_states));

    // Stale version_to_file records are not consumers. With no page or
    // synthesized gap there is nothing to publish and no output file to write.
    if (target_states.empty()) {
        return Status::OK();
    }

    // Phase 2: Serialize union results into union_buffer.
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

    // Phase 3: Resolve files only for final single-source consumers. A merged
    // state has already been decoded into union_buffer and does not need its
    // immutable source file copied into the output.
    std::vector<DelvecFileInfo> unique_delvec_files;
    std::unordered_set<std::string> selected_source_filenames;
    for (const auto& [target, state] : target_states) {
        (void)target;
        if (!state.single_source.has_value()) {
            continue;
        }
        const auto& ref = *state.single_source;
        auto file_it = ref.ctx->metadata()->delvec_meta().version_to_file().find(ref.page.version());
        if (file_it == ref.ctx->metadata()->delvec_meta().version_to_file().end()) {
            return Status::InvalidArgument("Delvec file not found for final single-source page version");
        }
        const auto& file = file_it->second;
        if (file.name() != ref.file_name) {
            return Status::Corruption("Delvec final single-source file name changed during merge");
        }
        if (selected_source_filenames.emplace(file.name()).second) {
            unique_delvec_files.push_back(DelvecFileInfo{ref.ctx->metadata()->id(), file});
        }
    }
    TEST_SYNC_POINT_CALLBACK("merge_delvecs:selected_source_files", &unique_delvec_files);

    if (unique_delvec_files.empty() && union_buffer.empty()) {
        return Status::Corruption("Delvec targets produced neither source files nor serialized union pages");
    }

    // Phase 4: Write one file. With no final single-source consumer, the
    // serialized union is the complete output; otherwise concatenate only the
    // selected immutable files and append the union buffer.
    FileMetaPB new_delvec_file;
    std::vector<uint64_t> offsets;
    uint64_t union_base_offset = 0;
    int writer_invocations = 0;
    if (unique_delvec_files.empty()) {
        DCHECK(!union_buffer.empty()) << "buffer-only path with empty union_buffer";
        ++writer_invocations;
        TEST_SYNC_POINT_CALLBACK("merge_delvecs:writer_invocations", &writer_invocations);
        RETURN_IF_ERROR(write_delvec_file_from_buffer(tablet_manager, new_metadata->id(), txn_id, Slice(union_buffer),
                                                      &new_delvec_file));
        union_base_offset = 0;
        if (actual_page_source_files.empty()) {
            g_tablet_merge_synthesized_only_delvec_total << 1;
        }
    } else {
        ++writer_invocations;
        TEST_SYNC_POINT_CALLBACK("merge_delvecs:writer_invocations", &writer_invocations);
        RETURN_IF_ERROR(merge_delvec_files(tablet_manager, unique_delvec_files, new_metadata->id(), txn_id,
                                           &new_delvec_file, &offsets, Slice(union_buffer), &union_base_offset));
    }

    // The merged delvec file is written; new_metadata does not point at it until Phase 5 below.
    // Orphan-file window, and like the .cols hook -- not like the sstable one -- nothing cleans this
    // file up: neither this function nor merge_tablet arms a cleanup guard over it, so an error
    // injected here leaves the file for ordinary orphan-file vacuum.
    FAIL_POINT_TRIGGER_RETURN_ERROR(tablet_merge_after_write_delvec);

    // Build base_offset_by_file_name. Empty for synthesized-only route since
    // there are no source files to reference; merged-state targets always go
    // through union_page_infos which is keyed by target rssid, not file name.
    std::unordered_map<std::string, uint64_t> base_offset_by_file_name;
    for (size_t i = 0; i < unique_delvec_files.size(); ++i) {
        base_offset_by_file_name[unique_delvec_files[i].delvec_file.name()] = offsets[i];
    }

    TEST_SYNC_POINT_CALLBACK("merge_delvecs:before_apply_offsets", &base_offset_by_file_name);

    // Phase 5: Build metadata locally. Install it only after every page offset
    // has been validated, so writer and mapping failures cannot partially
    // publish a destination delvec_meta.
    DelvecMetadataPB new_delvec_meta;

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

        (*new_delvec_meta.mutable_delvecs())[target] = std::move(new_page);
    }

    new_delvec_file.clear_encryption_meta();
    (*new_delvec_meta.mutable_version_to_file())[new_version] = std::move(new_delvec_file);
    new_metadata->mutable_delvec_meta()->Swap(&new_delvec_meta);
    return Status::OK();
}

Status validate_non_shared_legacy_sstable_form(const PersistentIndexSstablePB& src_pb) {
    if (src_pb.has_shared_version() && src_pb.shared_version() > 0 && !src_pb.has_shared_rssid()) {
        return Status::Corruption("non-shared legacy sstable has shared_version without shared_rssid");
    }
    if (src_pb.has_delvec() && src_pb.delvec().size() > 0) {
        return Status::Corruption("non-shared sstable has delvec but no shared_rssid");
    }
    return Status::OK();
}

bool has_valid_modern_sstable_shared_version(const PersistentIndexSstablePB& sstable) {
    return sstable.has_shared_version() && sstable.shared_version() > 0;
}

// Project a sstable that has shared_rssid set (modern shared or
// `ingest_sst()` output). |out| was already CopyFrom'd from |sst|; this
// function rewrites the projection-affected fields in place.
//
// Always re-attaches the merged delvec from |new_metadata->delvec_meta()|
// regardless of whether the source carried one — this is what lets a
// synthesized gap-delvec (created by merge_delvecs Phase 0 for keys covered
// by no contributing old tablet) reach the rebuilt sstable PB. Without it,
// PersistentIndexSstable::multi_get could return stale rssids when the LSM
// block-sort order is inverted.
StatusOr<uint32_t> effective_shared_rssid(const std::string& filename, uint32_t shared_rssid, int32_t rssid_offset) {
    const int64_t effective = static_cast<int64_t>(shared_rssid) + rssid_offset;
    if (effective < 0 || effective > std::numeric_limits<uint32_t>::max()) {
        return Status::Corruption(
                fmt::format("Shared sstable {} effective rssid is out of uint32 range: shared_rssid={} "
                            "rssid_offset={} effective={}",
                            filename, shared_rssid, rssid_offset, effective));
    }
    return static_cast<uint32_t>(effective);
}

StatusOr<uint32_t> effective_shared_rssid(const PersistentIndexSstablePB& sst) {
    return effective_shared_rssid(sst.filename(), sst.shared_rssid(), sst.rssid_offset());
}

Status project_modern_shared_rssid_sstable(const PersistentIndexSstablePB& sst, uint32_t mapped_rssid,
                                           const TabletMetadataPB* new_metadata, PersistentIndexSstablePB* out) {
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

// Project a non-shared sstable without shared_rssid: an old-tablet-local file
// produced by flush_pk_memtable for THIS merge round, or a rebuilt legacy
// file from a prior merge. Stored rssids already live in the old tablet's id
// space, so a proven affine projection delta suffices. The accumulation
// preserves correctness when the file already carries a non-zero
// rssid_offset (stacked merge case).
Status project_non_shared_legacy_sstable(const PersistentIndexSstablePB& sst, int64_t projection_delta,
                                         PersistentIndexSstablePB* out) {
    RETURN_IF_ERROR(validate_non_shared_legacy_sstable_form(sst));
    const int64_t accumulated_offset = static_cast<int64_t>(sst.rssid_offset()) + projection_delta;
    if (accumulated_offset < std::numeric_limits<int32_t>::min() ||
        accumulated_offset > std::numeric_limits<int32_t>::max()) {
        return Status::Corruption(
                fmt::format("accumulated rssid_offset exceeds int32 range: sst_offset={} ctx_offset={} sum={}",
                            sst.rssid_offset(), projection_delta, accumulated_offset));
    }
    out->set_rssid_offset(static_cast<int32_t>(accumulated_offset));
    const int64_t high = static_cast<int64_t>(extract_rss_rowid_high(sst.max_rss_rowid()));
    const int64_t new_high = high + projection_delta;
    if (new_high < 0 || new_high > std::numeric_limits<uint32_t>::max()) {
        return Status::Corruption(
                fmt::format("rssid high overflow in merge projection: high={} ctx_offset={} new_high={}", high,
                            projection_delta, new_high));
    } else {
        out->set_max_rss_rowid(
                encode_rss_rowid(static_cast<uint32_t>(new_high), extract_rss_rowid_low(sst.max_rss_rowid())));
    }
    out->clear_delvec();
    return Status::OK();
}

struct LegacySstableLiveRssidIndex {
    struct Entry {
        uint32_t source_rssid = 0;
        std::optional<int64_t> target_delta;
        size_t agreement_run = 0;
    };

    std::unordered_set<uint32_t> source_live_rssids;
    std::vector<Entry> entries;
};

struct SstableLiveRssidIndex {
    std::unordered_set<uint32_t> target_live_rssids;
    std::vector<LegacySstableLiveRssidIndex> sources;
};

// Build the live occurrence mapping once after source PK flush has installed the current rowsets. A missing/dead target
// is represented by no delta; adjacent sorted live RSSIDs with the same valid delta share one agreement run. The
// existing affine proof plus one bounded run lookup then proves each legacy SST without rescanning all live RSSIDs.
SstableLiveRssidIndex build_sstable_live_rssid_index(const std::vector<TabletMergeContext>& contexts,
                                                     const TabletMergeAllocationPlan& allocation_plan,
                                                     const TabletMetadataPB& target) {
    SstableLiveRssidIndex result;
    result.target_live_rssids = collect_live_rssids(target);
    result.sources.reserve(contexts.size());
    for (size_t context_index = 0; context_index < contexts.size(); ++context_index) {
        TEST_SYNC_POINT_CALLBACK("legacy_sstable_liveness_index:precompute_context", nullptr);
        LegacySstableLiveRssidIndex source_index;
        source_index.source_live_rssids = collect_live_rssids(*contexts[context_index].metadata());
        source_index.entries.reserve(source_index.source_live_rssids.size());
        for (uint32_t source : source_index.source_live_rssids) {
            TEST_SYNC_POINT_CALLBACK("legacy_sstable_liveness_index:visited_live_rssid", nullptr);
            LegacySstableLiveRssidIndex::Entry entry{.source_rssid = source};
            auto mapped = allocation_plan.projections[context_index].map_occurrence_rssid(source);
            if (mapped.ok() && mapped.value() < allocation_plan.target_next_rowset_id &&
                result.target_live_rssids.contains(mapped.value())) {
                entry.target_delta = static_cast<int64_t>(mapped.value()) - source;
            }
            source_index.entries.emplace_back(std::move(entry));
        }
        std::sort(source_index.entries.begin(), source_index.entries.end(),
                  [](const auto& left, const auto& right) { return left.source_rssid < right.source_rssid; });
        for (size_t i = 1; i < source_index.entries.size(); ++i) {
            source_index.entries[i].agreement_run = source_index.entries[i - 1].agreement_run;
            if (source_index.entries[i].target_delta != source_index.entries[i - 1].target_delta) {
                ++source_index.entries[i].agreement_run;
            }
        }
        result.sources.emplace_back(std::move(source_index));
    }
    return result;
}

bool legacy_sstable_occurrence_agrees(const LegacySstableLiveRssidIndex& source_index,
                                      const std::unordered_set<uint32_t>& target_live_rssids, uint64_t source_begin,
                                      uint32_t source_high, int64_t delta, uint32_t target_end) {
    TEST_SYNC_POINT_CALLBACK("legacy_sstable_liveness_index:bounded_lookup", nullptr);
    if (!source_index.source_live_rssids.contains(source_high)) return false;
    const int64_t projected_begin = static_cast<int64_t>(source_begin) + delta;
    const int64_t projected_high = static_cast<int64_t>(source_high) + delta;
    if (projected_begin < 0 || projected_begin > std::numeric_limits<int32_t>::max() || projected_high < 0 ||
        projected_high >= target_end || !target_live_rssids.contains(static_cast<uint32_t>(projected_high))) {
        return false;
    }
    auto find_source = [&](uint64_t source) {
        return std::lower_bound(source_index.entries.begin(), source_index.entries.end(), source,
                                [](const auto& entry, uint64_t value) { return entry.source_rssid < value; });
    };
    auto first = find_source(source_begin);
    auto high = find_source(source_high);
    return first != source_index.entries.end() && high != source_index.entries.end() &&
           high->source_rssid == source_high && first->agreement_run == high->agreement_run &&
           first->target_delta == delta;
}

std::optional<int64_t> prove_legacy_sstable_affine_domain(const PersistentIndexSstablePB& sst,
                                                          const RssidProjection& projection,
                                                          const LegacySstableLiveRssidIndex& source_index,
                                                          const std::unordered_set<uint32_t>& target_live_rssids,
                                                          uint32_t target_end) {
    if (sst.rssid_offset() < 0) return std::optional<int64_t>{};
    const uint64_t source_begin = static_cast<uint32_t>(sst.rssid_offset());
    const uint32_t source_high = extract_rss_rowid_high(sst.max_rss_rowid());
    if (source_high < source_begin) return std::optional<int64_t>{};
    auto delta = projection.affine_delta(source_begin, uint64_t{source_high} + 1);
    if (!delta.has_value() || !legacy_sstable_occurrence_agrees(source_index, target_live_rssids, source_begin,
                                                                source_high, *delta, target_end)) {
        return std::optional<int64_t>{};
    }
    return delta;
}

enum class MergeSstableMetaMode { kPrivate, kIdentical, kLazyRebuild };

enum class MergeSstableFallbackReason {
    kNone,
    kSharedOrMixed,
    kNonuniformMapping,
    kCohortMismatch,
    kRowsetLayoutMismatch,
    kDuplicatePhysicalFile,
    kUnsupportedSstForm,
    kProjectedDomain,
    kEmbeddedDelvec
};

struct MergeSstableMetaResult {
    MergeSstableMetaMode mode = MergeSstableMetaMode::kLazyRebuild;
    MergeSstableFallbackReason reason = MergeSstableFallbackReason::kNone;
    PersistentIndexSstableMetaPB metadata;
};

enum class MergeSourceRangeProof { kReusable, kLazyFallback };

MergeSstableMetaResult lazy_sstable_meta_result(MergeSstableFallbackReason reason) {
    MergeSstableMetaResult result;
    result.reason = reason;
    return result;
}

Status range_proof_corruption(std::string_view context, const Status& status) {
    return Status::Corruption(fmt::format("{}: {}", context, status.to_string()));
}

StatusOr<MergeSourceRangeProof> validate_metadata_reuse_source_ranges(const std::vector<TabletMergeContext>& contexts,
                                                                      const TabletMetadataPB& target) {
    auto target_schema_pb = target.schema();
    target_schema_pb.set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
    auto target_schema = TabletSchema::create(target_schema_pb);
    auto encode_range = [&](const TabletRangePB& range, std::string_view context) -> StatusOr<SstSeekRange> {
        auto structural_status = TabletRangeHelper::validate_range_structural(range, *target_schema);
        if (!structural_status.ok()) {
            return range_proof_corruption(context, structural_status);
        }
        auto encoded = TabletRangeHelper::create_sst_seek_range_from(range, target_schema);
        if (!encoded.ok()) {
            return range_proof_corruption(context, encoded.status());
        }
        return std::move(encoded).value();
    };

    ASSIGN_OR_RETURN(auto target_range, encode_range(target.range(), "invalid target tablet range"));
    if (contexts.empty()) {
        return Status::Corruption("tablet merge metadata reuse requires at least one source range");
    }

    std::vector<SstSeekRange> source_ranges;
    source_ranges.reserve(contexts.size());
    for (size_t i = 0; i < contexts.size(); ++i) {
        ASSIGN_OR_RETURN(auto source_range, encode_range(contexts[i].metadata()->range(),
                                                         fmt::format("invalid source tablet range at index {}", i)));
        source_ranges.emplace_back(std::move(source_range));
    }

    const auto& target_pb = target.range();
    const auto* comparator = sstable::BytewiseComparator();
    bool source_lower_unbounded = false;
    bool source_upper_unbounded = false;
    std::optional<std::string> source_min_lower;
    std::optional<std::string> source_max_upper;
    for (size_t i = 0; i < contexts.size(); ++i) {
        const auto& source_pb = contexts[i].metadata()->range();
        if (!source_pb.has_lower_bound()) {
            source_lower_unbounded = true;
        } else if (!source_min_lower.has_value() ||
                   comparator->Compare(Slice(source_ranges[i].seek_key), Slice(*source_min_lower)) < 0) {
            source_min_lower = source_ranges[i].seek_key;
        }
        if (!source_pb.has_upper_bound()) {
            source_upper_unbounded = true;
        } else if (!source_max_upper.has_value() ||
                   comparator->Compare(Slice(source_ranges[i].stop_key), Slice(*source_max_upper)) > 0) {
            source_max_upper = source_ranges[i].stop_key;
        }
    }
    const bool source_has_lower = !source_lower_unbounded;
    const bool source_has_upper = !source_upper_unbounded;
    if (source_has_lower != target_pb.has_lower_bound() || source_has_upper != target_pb.has_upper_bound() ||
        (source_has_lower && *source_min_lower != target_range.seek_key) ||
        (source_has_upper && *source_max_upper != target_range.stop_key)) {
        return Status::Corruption("tablet merge source outer bounds do not equal target bounds");
    }

    for (size_t i = 1; i < contexts.size(); ++i) {
        const auto& left_pb = contexts[i - 1].metadata()->range();
        const auto& right_pb = contexts[i].metadata()->range();
        if (!left_pb.has_upper_bound() || !right_pb.has_lower_bound() ||
            comparator->Compare(Slice(source_ranges[i - 1].stop_key), Slice(source_ranges[i].seek_key)) != 0) {
            return MergeSourceRangeProof::kLazyFallback;
        }
    }
    return MergeSourceRangeProof::kReusable;
}

StatusOr<bool> sstable_range_within_tablet(const PersistentIndexSstablePB& sst, const TabletMetadataPB& source) {
    if (!sst.has_range() || !sst.range().has_start_key() || !sst.range().has_end_key()) {
        return false;
    }
    const auto* comparator = sstable::BytewiseComparator();
    if (comparator->Compare(Slice(sst.range().start_key()), Slice(sst.range().end_key())) > 0) {
        return false;
    }
    auto source_schema_pb = source.schema();
    source_schema_pb.set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
    auto source_schema = TabletSchema::create(source_schema_pb);
    if (!TabletRangeHelper::validate_range_structural(source.range(), *source_schema).ok()) {
        return false;
    }
    auto source_range = TabletRangeHelper::create_sst_seek_range_from(source.range(), source_schema);
    if (!source_range.ok()) {
        return false;
    }
    if (source.range().has_lower_bound() &&
        comparator->Compare(Slice(sst.range().start_key()), Slice(source_range->seek_key)) < 0) {
        return false;
    }
    if (source.range().has_upper_bound() &&
        comparator->Compare(Slice(sst.range().end_key()), Slice(source_range->stop_key)) >= 0) {
        return false;
    }
    return true;
}

StatusOr<std::map<uint32_t, TargetSegmentDeclaration>> collect_preflight_target_segments(
        const TabletMergeAllocationPlan& allocation_plan) {
    std::map<uint32_t, TargetSegmentDeclaration> result;
    for (const auto& canonical : allocation_plan.canonicals) {
        const auto& rowset = canonical.source_form_rowset;
        const auto& projection = allocation_plan.projections[canonical.selected_context_index];
        ASSIGN_OR_RETURN(auto target_rowset_id, projection.map_primary_rssid(rowset.id()));
        for (int position = 0; position < rowset.segment_metas_size(); ++position) {
            const uint64_t target_rssid = uint64_t{target_rowset_id} + get_segment_idx(rowset, position);
            if (target_rssid >= allocation_plan.target_next_rowset_id ||
                target_rssid > std::numeric_limits<uint32_t>::max()) {
                return Status::Corruption("tablet merge planned target segment exceeds its authoritative cursor");
            }
            TargetSegmentDeclaration candidate{normalized_physical_base_key(rowset.segment_metas(position)),
                                               rowset.segment_metas(position).shared()};
            auto [iter, inserted] = result.try_emplace(static_cast<uint32_t>(target_rssid), std::move(candidate));
            if (!inserted) {
                return Status::Corruption(
                        fmt::format("tablet merge target RSSID {} has duplicate physical bases", target_rssid));
            }
        }
    }
    return result;
}

StatusOr<std::vector<std::map<uint32_t, uint32_t>>> collect_preflight_source_segments(
        const std::vector<TabletMergeContext>& contexts, const TabletMergeAllocationPlan& allocation_plan,
        const std::map<uint32_t, TargetSegmentDeclaration>& target_segments) {
    std::vector<std::map<uint32_t, uint32_t>> result(contexts.size());
    for (size_t context_index = 0; context_index < contexts.size(); ++context_index) {
        for (const auto& rowset : contexts[context_index].metadata()->rowsets()) {
            for (int position = 0; position < rowset.segment_metas_size(); ++position) {
                const uint32_t source_rssid = get_rssid(rowset, position);
                ASSIGN_OR_RETURN(auto target_rssid,
                                 allocation_plan.projections[context_index].map_occurrence_rssid(source_rssid));
                auto target = target_segments.find(target_rssid);
                if (target == target_segments.end()) {
                    return Status::Corruption(fmt::format("tablet merge source-live RSSID {} maps to missing target {}",
                                                          source_rssid, target_rssid));
                }
                if (normalized_physical_base_key(rowset.segment_metas(position)) != target->second.physical_base_key) {
                    return Status::Corruption(
                            fmt::format("tablet merge source-live RSSID {} disagrees with target {} physical base",
                                        source_rssid, target_rssid));
                }
                auto [iter, inserted] = result[context_index].try_emplace(source_rssid, target_rssid);
                if (!inserted && iter->second != target_rssid) {
                    return Status::Corruption(
                            fmt::format("tablet merge source RSSID {} has conflicting physical bases", source_rssid));
                }
            }
        }
    }
    return result;
}

Status validate_idg_entry_shape(const IndexDeltaGroupEntryPB& entry) {
    if (!entry.has_index_file() || entry.index_file().empty()) {
        return Status::Corruption("tablet merge IDG entry has a missing or empty filename");
    }
    if (entry.has_file_size() && entry.file_size() < 0) {
        return Status::Corruption(fmt::format("tablet merge IDG file {} has a negative size", entry.index_file()));
    }
    std::unordered_set<uint64_t> declared_keys;
    for (const auto& key : entry.keys()) {
        if (!key.has_col_unique_id() || !key.has_index_type()) {
            return Status::Corruption(
                    fmt::format("tablet merge IDG file {} has an incomplete declared key", entry.index_file()));
        }
        if (!declared_keys.insert(idg_pack_key(key.col_unique_id(), key.index_type())).second) {
            return Status::Corruption(
                    fmt::format("tablet merge IDG file {} has a duplicate declared key", entry.index_file()));
        }
    }
    for (const auto& key : entry.dropped_keys()) {
        if (!key.has_col_unique_id() || !key.has_index_type()) {
            return Status::Corruption(
                    fmt::format("tablet merge IDG file {} has an incomplete dropped key", entry.index_file()));
        }
    }
    return Status::OK();
}

struct PreflightSidecarDeclaration {
    std::string declaration;
    std::string physical_base_key;
};

Status register_preflight_sidecar(std::string_view kind, const std::string& filename, const std::string& declaration,
                                  const std::string& physical_base_key,
                                  std::map<std::string, PreflightSidecarDeclaration>* registry) {
    auto [iter, inserted] =
            registry->try_emplace(filename, PreflightSidecarDeclaration{declaration, physical_base_key});
    if (inserted) return Status::OK();
    if (iter->second.physical_base_key != physical_base_key) {
        return Status::Corruption(
                fmt::format("tablet merge {} file {} is attached to a different physical base", kind, filename));
    }
    if (iter->second.declaration != declaration) {
        return Status::Corruption(fmt::format(
                "tablet merge {} file {} has conflicting physical declarations (unique_column_ids/keys, version, "
                "size, encryption, presence, or unknown fields differ)",
                kind, filename));
    }
    return Status::OK();
}

Status validate_preflight_sstable_declaration(const PersistentIndexSstablePB& sstable) {
    if (!sstable.has_filename() || sstable.filename().empty()) {
        return Status::Corruption("tablet merge source SST has a missing or empty filename");
    }
    if (sstable.has_filesize() && sstable.filesize() < 0) {
        return Status::Corruption(fmt::format("tablet merge source SST {} has a negative size", sstable.filename()));
    }
    if (sstable.has_shared_rssid()) {
        if (!has_valid_modern_sstable_shared_version(sstable)) {
            const std::string version =
                    sstable.has_shared_version() ? std::to_string(sstable.shared_version()) : "missing";
            return Status::Corruption(fmt::format(
                    "tablet merge source SST {} has invalid shared_version {}; modern shared form requires a "
                    "positive shared_version",
                    sstable.filename(), version));
        }
        auto effective = effective_shared_rssid(sstable);
        if (!effective.ok()) {
            return Status::Corruption(fmt::format("tablet merge source SST {} has an invalid shared form: {}",
                                                  sstable.filename(), effective.status().message()));
        }
    } else {
        auto legacy_status = validate_non_shared_legacy_sstable_form(sstable);
        if (!legacy_status.ok()) {
            return Status::Corruption(fmt::format("tablet merge source SST {} has an invalid legacy form: {}",
                                                  sstable.filename(), legacy_status.message()));
        }
    }
    if (sstable.has_range()) {
        if (!sstable.range().has_start_key() || !sstable.range().has_end_key()) {
            return Status::Corruption(
                    fmt::format("tablet merge source SST {} has an incomplete range", sstable.filename()));
        }
        if (::starrocks::sstable::BytewiseComparator()->Compare(Slice(sstable.range().start_key()),
                                                                Slice(sstable.range().end_key())) > 0) {
            return Status::Corruption(
                    fmt::format("tablet merge source SST {} has a reversed range", sstable.filename()));
        }
    }
    return Status::OK();
}

PersistentIndexSstablePB normalized_preflight_sstable_form(const PersistentIndexSstablePB& sstable) {
    PersistentIndexSstablePB normalized(sstable);
    normalized.clear_version();
    normalized.clear_filename();
    normalized.clear_filesize();
    normalized.clear_encryption_meta();
    normalized.clear_shared();
    normalized.clear_fileset_id();
    normalized.clear_generation_version();
    return normalized;
}

Status preflight_merge_sources(const std::vector<TabletMergeContext>& contexts,
                               const TabletMergeAllocationPlan& allocation_plan, const TabletMetadataPB& target) {
    ASSIGN_OR_RETURN(const auto target_segments, collect_preflight_target_segments(allocation_plan));
    ASSIGN_OR_RETURN(const auto source_segments,
                     collect_preflight_source_segments(contexts, allocation_plan, target_segments));

    std::map<std::string, PreflightSidecarDeclaration> dcg_files;
    std::map<std::string, PreflightSidecarDeclaration> idg_files;
    std::map<std::string, FileMetaPB> delvec_files;
    for (size_t context_index = 0; context_index < contexts.size(); ++context_index) {
        const auto& metadata = *contexts[context_index].metadata();

        if (metadata.has_dcg_meta()) {
            for (const auto& [source_rssid, source_dcg] : metadata.dcg_meta().dcgs()) {
                auto source_segment = source_segments[context_index].find(source_rssid);
                if (source_segment == source_segments[context_index].end()) {
                    return Status::Corruption(
                            fmt::format("tablet merge stale DCG RSSID {} has no live source segment", source_rssid));
                }
                bool force_target_omission = false;
                TEST_SYNC_POINT_CALLBACK("tablet_merge_test:force_dcg_target_omission", &force_target_omission);
                auto target_segment = target_segments.find(source_segment->second);
                if (force_target_omission || target_segment == target_segments.end()) {
                    return Status::Corruption(
                            fmt::format("tablet merge source-live DCG RSSID {} target was omitted", source_rssid));
                }
                DeltaColumnGroupVerPB normalized(source_dcg);
                RETURN_IF_ERROR(validate_dcg_shape(normalized));
                normalize_dcg_optional_fields(&normalized);
                for (int entry_index = 0; entry_index < normalized.column_files_size(); ++entry_index) {
                    if (normalized.column_files(entry_index).empty()) {
                        return Status::Corruption("tablet merge DCG entry has an empty filename");
                    }
                    if (normalized.column_file_sizes(entry_index) < 0) {
                        return Status::Corruption(fmt::format("tablet merge DCG file {} has a negative size",
                                                              normalized.column_files(entry_index)));
                    }
                    normalized.set_shared_files(entry_index, target_segment->second.shared);
                    const auto single = make_single_entry_dcg(normalized, entry_index);
                    RETURN_IF_ERROR(register_preflight_sidecar("DCG", normalized.column_files(entry_index),
                                                               single.SerializeAsString(),
                                                               target_segment->second.physical_base_key, &dcg_files));
                }
            }
        }

        if (metadata.has_idg_meta()) {
            for (const auto& [source_rssid, source_idg] : metadata.idg_meta().idgs()) {
                auto source_segment = source_segments[context_index].find(source_rssid);
                if (source_segment == source_segments[context_index].end()) continue;
                bool force_target_omission = false;
                TEST_SYNC_POINT_CALLBACK("tablet_merge_test:force_idg_target_omission", &force_target_omission);
                auto target_segment = target_segments.find(source_segment->second);
                if (force_target_omission || target_segment == target_segments.end()) {
                    return Status::Corruption(
                            fmt::format("tablet merge source-live IDG RSSID {} target was omitted", source_rssid));
                }
                for (const auto& entry : source_idg.entries()) {
                    RETURN_IF_ERROR(validate_idg_entry_shape(entry));
                    IndexDeltaGroupEntryPB stable(entry);
                    stable.clear_dropped_keys();
                    stable.clear_shared_file();
                    RETURN_IF_ERROR(register_preflight_sidecar("IDG", entry.index_file(), stable.SerializeAsString(),
                                                               target_segment->second.physical_base_key, &idg_files));
                }
            }
        }

        if (metadata.has_delvec_meta()) {
            for (const auto& [source_rssid, page] : metadata.delvec_meta().delvecs()) {
                auto source_segment = source_segments[context_index].find(source_rssid);
                if (source_segment == source_segments[context_index].end()) continue;
                bool force_target_omission = false;
                TEST_SYNC_POINT_CALLBACK("tablet_merge_test:force_delvec_target_omission", &force_target_omission);
                if (force_target_omission || !target_segments.contains(source_segment->second)) {
                    return Status::Corruption(
                            fmt::format("tablet merge source-live delvec RSSID {} target was omitted", source_rssid));
                }
                auto file = metadata.delvec_meta().version_to_file().find(page.version());
                if (file == metadata.delvec_meta().version_to_file().end()) {
                    return Status::Corruption(
                            fmt::format("tablet merge live delvec page has no file for version {}", page.version()));
                }
                const auto& declaration = file->second;
                if (!declaration.has_name() || declaration.name().empty()) {
                    return Status::Corruption("tablet merge live delvec page has a missing or empty filename");
                }
                if (declaration.has_size()) {
                    if (declaration.size() < 0) {
                        return Status::Corruption(
                                fmt::format("tablet merge delvec file {} has a negative size", declaration.name()));
                    }
                    const uint64_t file_size = declaration.size();
                    if (page.offset() > file_size || page.size() > file_size - page.offset()) {
                        return Status::Corruption(
                                fmt::format("tablet merge delvec page exceeds file {} bounds", declaration.name()));
                    }
                }
                auto [iter, inserted] = delvec_files.try_emplace(declaration.name(), declaration);
                if (!inserted && !delvec_file_metadata_matches(iter->second, declaration)) {
                    return Status::Corruption(fmt::format(
                            "Delvec actual page source metadata mismatch for file {} during tablet merge preflight",
                            declaration.name()));
                }
            }
        }
    }

    bool has_source_sstable = false;
    bool has_sstable_range = false;
    for (const auto& context : contexts) {
        for (const auto& sstable : context.metadata()->sstable_meta().sstables()) {
            has_source_sstable = true;
            has_sstable_range |= sstable.has_range();
        }
    }
    if (!has_source_sstable) return Status::OK();

    if (target.schema().column_size() > 0) {
        auto range_proof = validate_metadata_reuse_source_ranges(contexts, target);
        if (!range_proof.ok()) {
            return Status::Corruption(
                    fmt::format("tablet merge SST source/target range is invalid: {}", range_proof.status().message()));
        }
    } else if (has_sstable_range) {
        return Status::Corruption("tablet merge SST range cannot be validated without a tablet schema");
    }

    std::map<std::string, PersistentIndexSstablePB> physical_sstables;
    for (const auto& context : contexts) {
        for (const auto& sstable : context.metadata()->sstable_meta().sstables()) {
            RETURN_IF_ERROR(validate_preflight_sstable_declaration(sstable));
            auto [iter, inserted] = physical_sstables.try_emplace(sstable.filename(), sstable);
            if (!inserted) {
                const auto& existing = iter->second;
                const bool size_matches = existing.has_filesize() == sstable.has_filesize() &&
                                          (!existing.has_filesize() || existing.filesize() == sstable.filesize());
                const bool encryption_matches =
                        existing.has_encryption_meta() == sstable.has_encryption_meta() &&
                        (!existing.has_encryption_meta() || existing.encryption_meta() == sstable.encryption_meta());
                if (!size_matches || !encryption_matches) {
                    return Status::Corruption(fmt::format(
                            "tablet merge source SST {} has conflicting physical metadata", sstable.filename()));
                }
                if (normalized_preflight_sstable_form(existing).SerializeAsString() !=
                    normalized_preflight_sstable_form(sstable).SerializeAsString()) {
                    return Status::Corruption(fmt::format("tablet merge source SST {} has conflicting form or range",
                                                          sstable.filename()));
                }
            }
        }
    }
    return Status::OK();
}

void order_and_assign_singleton_filesets(PersistentIndexSstableMetaPB* metadata) {
    auto* sstables = metadata->mutable_sstables();
    std::stable_sort(sstables->begin(), sstables->end(),
                     [](const PersistentIndexSstablePB& left, const PersistentIndexSstablePB& right) {
                         return static_cast<int64_t>(left.max_rss_rowid()) <
                                static_cast<int64_t>(right.max_rss_rowid());
                     });
    for (auto& sstable : *sstables) {
        sstable.mutable_fileset_id()->CopyFrom(UniqueId::gen_uid().to_proto());
    }
}

StatusOr<MergeSstableMetaResult> try_project_complete_private_sstables(const std::vector<TabletMergeContext>& contexts,
                                                                       const TabletMergeAllocationPlan& allocation_plan,
                                                                       const TabletMetadataPB& target,
                                                                       MergeSourceRangeProof source_range_proof,
                                                                       const SstableLiveRssidIndex& live_rssids) {
    if (source_range_proof != MergeSourceRangeProof::kReusable) {
        return lazy_sstable_meta_result(MergeSstableFallbackReason::kUnsupportedSstForm);
    }

    std::unordered_set<std::string> filenames;
    MergeSstableMetaResult result;
    result.mode = MergeSstableMetaMode::kPrivate;
    for (size_t context_index = 0; context_index < contexts.size(); ++context_index) {
        const auto& context = contexts[context_index];
        const auto& projection = allocation_plan.projections[context_index];
        const auto& source_live_rssids = live_rssids.sources[context_index];
        for (const auto& rowset : context.metadata()->rowsets()) {
            if (rowset.has_uid() && rowset.uid().hi() == 0 && rowset.uid().lo() == 0) {
                return lazy_sstable_meta_result(MergeSstableFallbackReason::kNonuniformMapping);
            }
        }
        for (const auto& source_sstable : context.metadata()->sstable_meta().sstables()) {
            if (source_sstable.shared()) {
                return lazy_sstable_meta_result(MergeSstableFallbackReason::kSharedOrMixed);
            }
            if (!filenames.insert(source_sstable.filename()).second) {
                return lazy_sstable_meta_result(MergeSstableFallbackReason::kDuplicatePhysicalFile);
            }
            ASSIGN_OR_RETURN(bool range_within_source,
                             sstable_range_within_tablet(source_sstable, *context.metadata()));
            if (!range_within_source) {
                return lazy_sstable_meta_result(MergeSstableFallbackReason::kUnsupportedSstForm);
            }
            PersistentIndexSstablePB projected(source_sstable);
            if (source_sstable.has_shared_rssid()) {
                if (!has_valid_modern_sstable_shared_version(source_sstable)) {
                    return lazy_sstable_meta_result(MergeSstableFallbackReason::kUnsupportedSstForm);
                }
                auto source_rssid = effective_shared_rssid(source_sstable);
                if (!source_rssid.ok()) {
                    return lazy_sstable_meta_result(MergeSstableFallbackReason::kProjectedDomain);
                }
                if (extract_rss_rowid_high(source_sstable.max_rss_rowid()) != *source_rssid ||
                    !source_live_rssids.source_live_rssids.contains(*source_rssid)) {
                    return lazy_sstable_meta_result(MergeSstableFallbackReason::kProjectedDomain);
                }
                auto mapped_rssid = projection.map_occurrence_rssid(*source_rssid);
                if (!mapped_rssid.ok() || !live_rssids.target_live_rssids.contains(mapped_rssid.value())) {
                    return lazy_sstable_meta_result(MergeSstableFallbackReason::kProjectedDomain);
                }
                const auto projection_status =
                        project_modern_shared_rssid_sstable(source_sstable, mapped_rssid.value(), &target, &projected);
                if (!projection_status.ok()) {
                    return lazy_sstable_meta_result(MergeSstableFallbackReason::kEmbeddedDelvec);
                }
            } else {
                if ((source_sstable.has_shared_version() && source_sstable.shared_version() > 0) ||
                    (source_sstable.has_delvec() && source_sstable.delvec().size() > 0) ||
                    source_sstable.rssid_offset() < 0) {
                    return lazy_sstable_meta_result(MergeSstableFallbackReason::kUnsupportedSstForm);
                }
                auto domain_delta = prove_legacy_sstable_affine_domain(source_sstable, projection, source_live_rssids,
                                                                       live_rssids.target_live_rssids,
                                                                       allocation_plan.target_next_rowset_id);
                if (!domain_delta.has_value()) {
                    return lazy_sstable_meta_result(MergeSstableFallbackReason::kNonuniformMapping);
                }
                const auto projection_status =
                        project_non_shared_legacy_sstable(source_sstable, *domain_delta, &projected);
                if (!projection_status.ok()) {
                    return lazy_sstable_meta_result(MergeSstableFallbackReason::kProjectedDomain);
                }
            }
            result.metadata.add_sstables()->Swap(&projected);
        }
    }
    order_and_assign_singleton_filesets(&result.metadata);
    return result;
}

bool same_rowset_physical_layout(const RowsetMetadataPB& left, const RowsetMetadataPB& right) {
    if (!tablet_reshard_helper::same_rowset_uid(left, right) ||
        left.segment_metas_size() != right.segment_metas_size()) {
        return false;
    }
    for (int i = 0; i < left.segment_metas_size(); ++i) {
        if (get_segment_idx(left, i) != get_segment_idx(right, i)) {
            return false;
        }
    }

    // SPLIT or an earlier merge can stamp context-local rowset IDs/ranges and apportion logical statistics, while the
    // physical segment and delete-file declarations remain inherited. Compare every other rowset field, using
    // segment_metas as the canonical physical declaration and normalizing its effective index.
    auto normalized = [](const RowsetMetadataPB& rowset) {
        RowsetMetadataPB result(rowset);
        result.clear_id();
        result.clear_range();
        result.clear_num_rows();
        result.clear_data_size();
        result.clear_num_dels();
        result.clear_deprecated_segments();
        result.clear_deprecated_segment_size();
        result.clear_deprecated_segment_encryption_metas();
        result.clear_deprecated_bundle_file_offsets();
        result.clear_deprecated_shared_segments();
        for (auto& segment : *result.mutable_segment_metas()) {
            segment.clear_segment_idx();
        }
        return result;
    };
    return normalized(left).SerializeAsString() == normalized(right).SerializeAsString();
}

bool identical_rowset_layouts(const TabletMetadataPB& canonical, const TabletMetadataPB& candidate) {
    if (canonical.rowsets_size() != candidate.rowsets_size()) return false;
    for (int i = 0; i < canonical.rowsets_size(); ++i) {
        const auto& left = canonical.rowsets(i);
        const auto& right = candidate.rowsets(i);
        if (!same_rowset_physical_layout(left, right)) {
            return false;
        }
    }
    return true;
}

bool semantic_sstable_equal(const PersistentIndexSstablePB& left, const PersistentIndexSstablePB& right) {
    PersistentIndexSstablePB normalized_left(left);
    PersistentIndexSstablePB normalized_right(right);
    normalized_left.clear_fileset_id();
    normalized_right.clear_fileset_id();
    return normalized_left.SerializeAsString() == normalized_right.SerializeAsString();
}

StatusOr<MergeSstableMetaResult> try_reuse_complete_identical_sstables(const std::vector<TabletMergeContext>& contexts,
                                                                       const TabletMergeAllocationPlan& allocation_plan,
                                                                       const TabletMetadataPB& target,
                                                                       MergeSourceRangeProof source_range_proof,
                                                                       const SstableLiveRssidIndex& live_rssids) {
    if (source_range_proof != MergeSourceRangeProof::kReusable) {
        return lazy_sstable_meta_result(MergeSstableFallbackReason::kUnsupportedSstForm);
    }

    const auto& canonical_metadata = *contexts.front().metadata();
    const auto& canonical_sstables = canonical_metadata.sstable_meta().sstables();
    std::unordered_set<std::string> canonical_filenames;
    for (const auto& sstable : canonical_sstables) {
        if (!sstable.shared()) {
            return lazy_sstable_meta_result(MergeSstableFallbackReason::kSharedOrMixed);
        }
        if (!canonical_filenames.insert(sstable.filename()).second) {
            return lazy_sstable_meta_result(MergeSstableFallbackReason::kDuplicatePhysicalFile);
        }
    }
    for (size_t context_index = 1; context_index < contexts.size(); ++context_index) {
        const auto& candidate_metadata = *contexts[context_index].metadata();
        if (!identical_rowset_layouts(canonical_metadata, candidate_metadata)) {
            return lazy_sstable_meta_result(MergeSstableFallbackReason::kRowsetLayoutMismatch);
        }
        const auto& candidate_sstables = candidate_metadata.sstable_meta().sstables();
        if (candidate_sstables.size() != canonical_sstables.size()) {
            return lazy_sstable_meta_result(MergeSstableFallbackReason::kCohortMismatch);
        }
        for (int i = 0; i < canonical_sstables.size(); ++i) {
            if (!candidate_sstables.Get(i).shared()) {
                return lazy_sstable_meta_result(MergeSstableFallbackReason::kSharedOrMixed);
            }
            if (!semantic_sstable_equal(canonical_sstables.Get(i), candidate_sstables.Get(i))) {
                return lazy_sstable_meta_result(MergeSstableFallbackReason::kCohortMismatch);
            }
        }
    }

    MergeSstableMetaResult result;
    result.mode = MergeSstableMetaMode::kIdentical;
    for (const auto& canonical_sstable : canonical_sstables) {
        PersistentIndexSstablePB projected(canonical_sstable);
        projected.set_shared(true);
        if (canonical_sstable.has_shared_rssid()) {
            if (!has_valid_modern_sstable_shared_version(canonical_sstable)) {
                return lazy_sstable_meta_result(MergeSstableFallbackReason::kUnsupportedSstForm);
            }
            auto source_rssid = effective_shared_rssid(canonical_sstable);
            if (!source_rssid.ok() || extract_rss_rowid_high(canonical_sstable.max_rss_rowid()) != *source_rssid) {
                return lazy_sstable_meta_result(MergeSstableFallbackReason::kProjectedDomain);
            }
            std::optional<uint32_t> common_target;
            for (size_t context_index = 0; context_index < contexts.size(); ++context_index) {
                if (!live_rssids.sources[context_index].source_live_rssids.contains(*source_rssid)) {
                    return lazy_sstable_meta_result(MergeSstableFallbackReason::kProjectedDomain);
                }
                ASSIGN_OR_RETURN(auto mapped,
                                 allocation_plan.projections[context_index].map_occurrence_rssid(*source_rssid));
                if (common_target.has_value() && *common_target != mapped) {
                    return lazy_sstable_meta_result(MergeSstableFallbackReason::kNonuniformMapping);
                }
                common_target = mapped;
            }
            if (!common_target.has_value()) {
                return lazy_sstable_meta_result(MergeSstableFallbackReason::kProjectedDomain);
            }
            const uint32_t mapped_rssid = *common_target;
            if (!live_rssids.target_live_rssids.contains(mapped_rssid)) {
                return lazy_sstable_meta_result(MergeSstableFallbackReason::kProjectedDomain);
            }
            const auto projection_status =
                    project_modern_shared_rssid_sstable(canonical_sstable, mapped_rssid, &target, &projected);
            if (!projection_status.ok()) {
                return lazy_sstable_meta_result(MergeSstableFallbackReason::kEmbeddedDelvec);
            }
        } else {
            if ((canonical_sstable.has_shared_version() && canonical_sstable.shared_version() > 0) ||
                (canonical_sstable.has_delvec() && canonical_sstable.delvec().size() > 0) ||
                canonical_sstable.rssid_offset() < 0) {
                return lazy_sstable_meta_result(MergeSstableFallbackReason::kUnsupportedSstForm);
            }
            const uint64_t source_begin = static_cast<uint32_t>(canonical_sstable.rssid_offset());
            const uint32_t source_high = extract_rss_rowid_high(canonical_sstable.max_rss_rowid());
            auto common_delta = prove_legacy_sstable_affine_domain(
                    canonical_sstable, allocation_plan.projections.front(), live_rssids.sources.front(),
                    live_rssids.target_live_rssids, allocation_plan.target_next_rowset_id);
            if (!common_delta.has_value()) {
                return lazy_sstable_meta_result(MergeSstableFallbackReason::kNonuniformMapping);
            }
            for (size_t context_index = 1; context_index < contexts.size(); ++context_index) {
                if (!legacy_sstable_occurrence_agrees(live_rssids.sources[context_index],
                                                      live_rssids.target_live_rssids, source_begin, source_high,
                                                      *common_delta, allocation_plan.target_next_rowset_id)) {
                    return lazy_sstable_meta_result(MergeSstableFallbackReason::kNonuniformMapping);
                }
            }
            const auto projection_status =
                    project_non_shared_legacy_sstable(canonical_sstable, *common_delta, &projected);
            if (!projection_status.ok()) {
                return lazy_sstable_meta_result(MergeSstableFallbackReason::kProjectedDomain);
            }
        }
        result.metadata.add_sstables()->Swap(&projected);
    }
    order_and_assign_singleton_filesets(&result.metadata);
    return result;
}

Status fold_omitted_sstable_orphans(const std::vector<TabletMergeContext>& contexts, int64_t target_version,
                                    TabletMetadataPB* target) {
    struct FoldedOrphan {
        FileMetaPB file;
        bool version_valid = true;
        int64_t version = 0;
    };
    std::map<std::string, FoldedOrphan> folded;
    for (const auto& context : contexts) {
        for (const auto& sstable : context.metadata()->sstable_meta().sstables()) {
            if (sstable.filename().empty()) {
                return Status::Corruption("tablet merge source SST has an empty filename");
            }
            auto [it, inserted] = folded.try_emplace(sstable.filename());
            auto& orphan = it->second;
            const int64_t occurrence_version = sstable.generation_version();
            const bool occurrence_valid = occurrence_version > 0 && occurrence_version <= target_version;
            if (inserted) {
                orphan.file.set_name(sstable.filename());
                orphan.file.set_size(sstable.filesize());
                orphan.file.set_encryption_meta(sstable.encryption_meta());
                orphan.file.set_shared(true);
                orphan.version_valid = occurrence_valid;
                orphan.version = occurrence_version;
                continue;
            }
            if (orphan.file.size() != sstable.filesize() ||
                orphan.file.encryption_meta() != sstable.encryption_meta()) {
                return Status::Corruption(fmt::format("tablet merge source SST {} has conflicting physical metadata",
                                                      sstable.filename()));
            }
            if (!occurrence_valid || !orphan.version_valid || orphan.version != occurrence_version) {
                orphan.version_valid = false;
            }
        }
    }

    int64_t omitted_bytes = 0;
    for (auto& [filename, orphan] : folded) {
        (void)filename;
        orphan.file.set_version(orphan.version_valid ? orphan.version : 0);
        omitted_bytes += std::max<int64_t>(0, orphan.file.size());
        target->add_orphan_files()->Swap(&orphan.file);
    }
    g_tablet_merge_sstable_omitted_file_total << static_cast<int64_t>(folded.size());
    g_tablet_merge_sstable_omitted_bytes_total << omitted_bytes;
    return Status::OK();
}

void record_merge_sstable_meta_result(const MergeSstableMetaResult& result) {
    switch (result.mode) {
    case MergeSstableMetaMode::kPrivate:
        g_tablet_merge_sstable_meta_private_total << 1;
        return;
    case MergeSstableMetaMode::kIdentical:
        g_tablet_merge_sstable_meta_identical_total << 1;
        return;
    case MergeSstableMetaMode::kLazyRebuild:
        g_tablet_merge_sstable_meta_lazy_rebuild_total << 1;
        break;
    }
    switch (result.reason) {
    case MergeSstableFallbackReason::kNone:
        return;
    case MergeSstableFallbackReason::kSharedOrMixed:
        g_tablet_merge_sstable_fallback_shared_or_mixed_total << 1;
        return;
    case MergeSstableFallbackReason::kNonuniformMapping:
        g_tablet_merge_sstable_fallback_nonuniform_mapping_total << 1;
        return;
    case MergeSstableFallbackReason::kCohortMismatch:
        g_tablet_merge_sstable_fallback_cohort_mismatch_total << 1;
        return;
    case MergeSstableFallbackReason::kRowsetLayoutMismatch:
        g_tablet_merge_sstable_fallback_rowset_layout_mismatch_total << 1;
        return;
    case MergeSstableFallbackReason::kDuplicatePhysicalFile:
        g_tablet_merge_sstable_fallback_duplicate_physical_file_total << 1;
        return;
    case MergeSstableFallbackReason::kUnsupportedSstForm:
        g_tablet_merge_sstable_fallback_unsupported_sst_form_total << 1;
        return;
    case MergeSstableFallbackReason::kProjectedDomain:
        g_tablet_merge_sstable_fallback_projected_domain_total << 1;
        return;
    case MergeSstableFallbackReason::kEmbeddedDelvec:
        g_tablet_merge_sstable_fallback_embedded_delvec_total << 1;
        return;
    }
}

Status merge_sstables(TabletManager* tablet_manager, std::vector<TabletMergeContext>& merge_contexts,
                      const TabletMergeAllocationPlan& allocation_plan, TabletMetadataPB* new_metadata) {
    auto* update_manager = tablet_manager->update_mgr();
    for (auto& context : merge_contexts) {
        bool skip_source_flush = false;
        FAIL_POINT_TRIGGER_EXECUTE(skip_lake_pk_index_merge_source_flush, { skip_source_flush = true; });
        if (skip_source_flush) continue;
        TEST_SYNC_POINT_CALLBACK("merge_sstables:source_pk_flush", nullptr);
        ASSIGN_OR_RETURN(auto flushed, update_manager->flush_pk_memtable(context.metadata(), new_metadata->version()));
        context.set_metadata(std::move(flushed));
    }

    const bool has_source_sstable = std::any_of(merge_contexts.begin(), merge_contexts.end(), [](const auto& context) {
        return !context.metadata()->sstable_meta().sstables().empty();
    });
    if (!has_source_sstable) {
        new_metadata->clear_sstable_meta();
        return Status::OK();
    }
    const auto live_rssids = build_sstable_live_rssid_index(merge_contexts, allocation_plan, *new_metadata);

    TEST_SYNC_POINT_CALLBACK("merge_sstables:metadata_classifier_entry", nullptr);
    DeferOp classifier_exit([] { TEST_SYNC_POINT_CALLBACK("merge_sstables:metadata_classifier_exit", nullptr); });
    ASSIGN_OR_RETURN(const auto source_range_proof,
                     validate_metadata_reuse_source_ranges(merge_contexts, *new_metadata));

    ASSIGN_OR_RETURN(auto private_result,
                     try_project_complete_private_sstables(merge_contexts, allocation_plan, *new_metadata,
                                                           source_range_proof, live_rssids));
    if (private_result.mode == MergeSstableMetaMode::kPrivate) {
        new_metadata->mutable_sstable_meta()->Swap(&private_result.metadata);
        record_merge_sstable_meta_result(private_result);
        return Status::OK();
    }

    ASSIGN_OR_RETURN(auto identical_result,
                     try_reuse_complete_identical_sstables(merge_contexts, allocation_plan, *new_metadata,
                                                           source_range_proof, live_rssids));
    if (identical_result.mode == MergeSstableMetaMode::kIdentical) {
        new_metadata->mutable_sstable_meta()->Swap(&identical_result.metadata);
        record_merge_sstable_meta_result(identical_result);
        return Status::OK();
    }

    MergeSstableMetaResult fallback_result;
    fallback_result.reason = private_result.reason;
    if (fallback_result.reason == MergeSstableFallbackReason::kNone ||
        fallback_result.reason == MergeSstableFallbackReason::kSharedOrMixed) {
        fallback_result.reason = identical_result.reason;
    }
    new_metadata->clear_sstable_meta();
    RETURN_IF_ERROR(fold_omitted_sstable_orphans(merge_contexts, new_metadata->version(), new_metadata));
    record_merge_sstable_meta_result(fallback_result);
    return Status::OK();
}

Status validate_source_rssid_domain(const TabletMetadataPB& metadata) {
    for (const auto& rowset : metadata.rowsets()) {
        const uint64_t max_segment_idx = get_max_segment_idx(rowset);
        const uint64_t segment_span = rowset.segment_metas_size() == 0 ? 1 : max_segment_idx + 1;
        if (uint64_t{rowset.id()} + segment_span > kSourceRssidExclusiveLimit) {
            return Status::InvalidArgument("tablet merge source rowset extent exceeds the uint32 RSSID domain");
        }
        for (const auto& del : rowset.del_files()) {
            const uint64_t replay_offset = del.has_op_offset() ? uint64_t{del.op_offset()} : max_segment_idx;
            if (uint64_t{del.origin_rowset_id()} + replay_offset + 1 > kSourceRssidExclusiveLimit) {
                return Status::InvalidArgument("tablet merge source delete span exceeds the uint32 RSSID domain");
            }
        }
    }
    return Status::OK();
}

StatusOr<uint32_t> compute_supported_next_rowset_id(const TabletMetadataPB& metadata) {
    uint64_t next = 1;
    for (const auto& rowset : metadata.rowsets()) {
        const uint64_t rowset_id = rowset.id();
        const uint64_t max_segment_idx = get_max_segment_idx(rowset);
        const uint64_t segment_span = rowset.segment_metas_size() == 0 ? 1 : max_segment_idx + 1;
        next = std::max(next, rowset_id + segment_span);
        for (const auto& del : rowset.del_files()) {
            const uint64_t replay_offset = del.origin_rowset_id() == rowset.id()
                                                   ? (del.has_op_offset() ? uint64_t{del.op_offset()} : max_segment_idx)
                                                   : max_segment_idx;
            next = std::max(next, rowset_id + replay_offset + 1);
            if (del.has_op_offset()) {
                next = std::max(next, uint64_t{del.origin_rowset_id()} + uint64_t{del.op_offset()} + 1);
            }
        }
    }
    for (const auto& sst : metadata.sstable_meta().sstables()) {
        if (sst.max_rss_rowid() > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return Status::InvalidArgument("tablet merge SST watermark exceeds the supported signed domain");
        }
        next = std::max(next, static_cast<uint64_t>(extract_rss_rowid_high(sst.max_rss_rowid())) + 1);
    }
    if (next > static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
        return Status::InvalidArgument("tablet merge exhausts the supported rssid allocation domain");
    }
    return static_cast<uint32_t>(next);
}

void merge_schemas(const std::vector<TabletMergeContext>& merge_contexts, TabletMetadataPB* new_metadata) {
    // Step 1: Collect all historical_schemas from all old tablets (union by schema_id)
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

// Reconcile the async vector-index build watermark across ALL merge sources into new_metadata.
// The merged tablet contains rowsets from every source, so a rowset is only guaranteed built if
// it was built in its OWN source. Inheriting source[0]'s watermark alone (via the CopyFrom that
// seeds new_metadata) could falsely claim another source's unbuilt rowsets are built, permanently
// skipping their .vi build. MIN over sources is the largest value that holds for every merged
// rowset; the build task's per-.vi existence check skips already-built ones, so a conservative
// (lower) watermark only re-examines, never rebuilds needlessly. A source without the field
// guarantees nothing is built and contributes 0; if no source has the field, leave it unset.
void reconcile_vector_index_built_version(const std::vector<TabletMergeContext>& merge_contexts,
                                          TabletMetadataPB* new_metadata) {
    bool any_has_built_version = false;
    int64_t min_built_version = std::numeric_limits<int64_t>::max();
    for (const auto& ctx : merge_contexts) {
        int64_t v = ctx.metadata()->has_vector_index_built_version() ? ctx.metadata()->vector_index_built_version() : 0;
        any_has_built_version |= ctx.metadata()->has_vector_index_built_version();
        min_built_version = std::min(min_built_version, v);
    }
    if (any_has_built_version) {
        new_metadata->set_vector_index_built_version(min_built_version);
    } else {
        new_metadata->clear_vector_index_built_version();
    }
}

bool uses_cloud_native_pk_index(const TabletMetadataPB& metadata) {
    return is_primary_key(metadata) && metadata.enable_persistent_index() &&
           metadata.persistent_index_type() == PersistentIndexTypePB::CLOUD_NATIVE;
}

} // namespace

DEFINE_FAIL_POINT(tablet_merge_after_rssid_reassign);
StatusOr<MutableTabletMetadataPtr> merge_tablet(TabletManager* tablet_manager,
                                                const std::vector<TabletMetadataPtr>& old_tablet_metadatas,
                                                const MergingTabletInfoPB& merging_tablet, int64_t new_version,
                                                const TxnInfoPB& txn_info, bool skip_sstable_merge) {
    if (old_tablet_metadatas.empty()) {
        return Status::InvalidArgument("No old tablet metadata to merge");
    }

    std::vector<TabletMergeContext> merge_contexts;
    merge_contexts.reserve(old_tablet_metadatas.size());
    for (const auto& old_tablet_metadata : old_tablet_metadatas) {
        if (old_tablet_metadata == nullptr) {
            return Status::InvalidArgument("old tablet metadata is null");
        }
        // Source RSSIDs occupy the full uint32 domain. Only the packed target
        // cursor is restricted to INT32_MAX; source SST reuse is proved later
        // by the modern/legacy classifiers against the packed projection.
        RETURN_IF_ERROR(validate_source_rssid_domain(*old_tablet_metadata));
        if (!skip_sstable_merge && is_primary_key(*old_tablet_metadata)) {
            for (const auto& rowset : old_tablet_metadata->rowsets()) {
                const uint64_t source_rowset_id = static_cast<uint64_t>(rowset.id());
                if (source_rowset_id == 0) {
                    return Status::InvalidArgument(
                            fmt::format("Writable primary-key tablet merge source tablet {} has invalid rowset {}",
                                        old_tablet_metadata->id(), source_rowset_id));
                }
            }
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
    new_tablet_metadata->clear_idg_meta();
    new_tablet_metadata->clear_rowset_to_schema();
    new_tablet_metadata->clear_compaction_inputs();
    new_tablet_metadata->clear_orphan_files();
    new_tablet_metadata->clear_prev_garbage_version();
    new_tablet_metadata->set_cumulative_point(0);

    // Reconcile the async vector-index build watermark to the MIN over all merge sources so the
    // build task never skips another source's unbuilt rowsets (see the helper for the invariant).
    reconcile_vector_index_built_version(merge_contexts, new_tablet_metadata.get());

    const bool discard_empty_rowsets = !skip_sstable_merge && is_primary_key(*merge_contexts.front().metadata());
    ASSIGN_OR_RETURN(auto allocation_plan, build_tablet_merge_allocation_plan(merge_contexts, discard_empty_rowsets));

    // Merge tablet-level range via union_range
    TabletRangePB merged_range = merge_contexts.front().metadata()->range();
    for (size_t i = 1; i < merge_contexts.size(); ++i) {
        ASSIGN_OR_RETURN(merged_range,
                         tablet_reshard_helper::union_range(merged_range, merge_contexts[i].metadata()->range()));
    }
    new_tablet_metadata->mutable_range()->CopyFrom(merged_range);

    RETURN_IF_ERROR(preflight_merge_sources(merge_contexts, allocation_plan, *new_tablet_metadata));

    FAIL_POINT_TRIGGER_RETURN_ERROR(tablet_merge_after_rssid_reassign);

    // Phase 2: Merge rowsets (version-driven k-way merge with dedup).
    // canonical_contribs collects each canonical rowset's contributing
    // old tablets' old-tablet-local ranges; consumed by the PK fail-fast coverage
    // check below and by gap-delvec synthesis.
    CanonicalContribMap canonical_contribs;
    RETURN_IF_ERROR(materialize_planned_rowsets(allocation_plan, new_tablet_metadata.get(), &canonical_contribs));

    // Phase 2.5: Merge schemas (must run before gap synthesis + merge_dcg_meta,
    // which need historical_schemas to locate rebuild schemas for shared-segment
    // rebuild).
    merge_schemas(merge_contexts, new_tablet_metadata.get());

    new_tablet_metadata->set_next_rowset_id(allocation_plan.target_next_rowset_id);

    // Synthesize the per-target gap bitmaps once (PK only). The same specs drive
    // both DCG coverage-acceptance (merge_dcg_meta) and delvec masking
    // (merge_delvecs), so the two paths cannot diverge. For non-PK tables the
    // specs stay empty, which keeps DCG coverage strict.
    std::vector<CanonicalGapSpec> gap_specs;
    if (is_primary_key(*new_tablet_metadata)) {
        ASSIGN_OR_RETURN(gap_specs,
                         compute_synthesized_gap_specs(tablet_manager, *new_tablet_metadata, canonical_contribs));
        if (!gap_specs.empty()) {
            g_tablet_merge_gap_delvec_total << 1;
        }
    }

    // Phase 3: occurrence projections consume the immutable allocation plan.
    RETURN_IF_ERROR(merge_dcg_meta(tablet_manager, merge_contexts, allocation_plan, merging_tablet.new_tablet_id(),
                                   new_version, txn_info.txn_id(), gap_specs, new_tablet_metadata.get()));

    // Remap the lake ADD INDEX fast-path IDG (.idx) entries into the merged rssid space,
    // mirroring merge_dcg_meta. Metadata-only (no segment rebuild); see merge_idg_meta.
    RETURN_IF_ERROR(merge_idg_meta(merge_contexts, allocation_plan, new_tablet_metadata.get()));

    if (is_primary_key(*new_tablet_metadata)) {
        RETURN_IF_ERROR(merge_delvecs(tablet_manager, merge_contexts, allocation_plan, gap_specs, new_version,
                                      txn_info.txn_id(), new_tablet_metadata.get()));
    }

    if (skip_sstable_merge) {
        // Read-only alias: leave it without a primary index rather than paying the rebuild.
        new_tablet_metadata->clear_sstable_meta();
    } else if (uses_cloud_native_pk_index(*new_tablet_metadata)) {
        RETURN_IF_ERROR(merge_sstables(tablet_manager, merge_contexts, allocation_plan, new_tablet_metadata.get()));
    } else {
        // SST classification and source flushing are cloud-native PK contracts. Other key/index modes retain
        // their merged rowset and sidecar metadata without manufacturing a primary-index attachment.
        new_tablet_metadata->clear_sstable_meta();
    }

    // Phase 4: Finalize
    ASSIGN_OR_RETURN(auto required_next_rowset_id, compute_supported_next_rowset_id(*new_tablet_metadata));
    if (required_next_rowset_id > allocation_plan.target_next_rowset_id) {
        return Status::Corruption("tablet merge output exceeds its authoritative RSSID cursor");
    }
    new_tablet_metadata->set_next_rowset_id(allocation_plan.target_next_rowset_id);

    // No re-share here: the merged tablet OWNS its segments via the same ownership-transfer
    // model that the identical-tablet and split paths already use. The source old tablets
    // are marked all-shared (set_all_data_files_shared in tablet_reshard.cpp), so their
    // drop/vacuum skips these files; the merged tablet then inherits the split-side
    // per-segment flags unchanged. A split-pruned segment keeps shared=false (owned by the
    // merged tablet, freed by its own GC); a spanning segment keeps shared=true (still
    // referenced by any non-merged split sibling). Leaving the flags untouched keeps
    // genuinely-private compaction/rewrite outputs on the local-output GC path rather than
    // leaking them onto the shared-file path.
    return new_tablet_metadata;
}

} // namespace starrocks::lake

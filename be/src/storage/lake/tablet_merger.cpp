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
#include "column/column_helper.h"
#include "common/config_rowset_fwd.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "storage/chunk_helper.h"
#include "storage/del_vector.h"
#include "storage/delta_column_group.h"
#include "storage/lake/filenames.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_range_helper.h"
#include "storage/lake/tablet_reshard_helper.h"
#include "storage/olap_common.h"
#include "storage/options.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet_schema.h"

namespace {

bvar::Adder<int64_t> g_tablet_merge_dcg_rebuild_total("tablet_merge_dcg_rebuild_total");
bvar::Adder<int64_t> g_tablet_merge_dcg_rebuild_fallback_not_supported_total(
        "tablet_merge_dcg_rebuild_fallback_not_supported_total");

} // namespace

namespace starrocks::lake {

namespace {

class TabletMergeContext {
public:
    explicit TabletMergeContext(TabletMetadataPtr metadata) : _metadata(std::move(metadata)) {}

    const TabletMetadataPtr& metadata() const { return _metadata; }

    int64_t rssid_offset() const { return _rssid_offset; }
    void set_rssid_offset(int64_t offset) { _rssid_offset = offset; }

    // Maps an original rssid to the final output rssid.
    // If the rssid was deduped, returns the canonical rssid from shared_rssid_map;
    // otherwise applies the default offset mapping with overflow check.
    StatusOr<uint32_t> map_rssid(uint32_t rssid) const {
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

    bool has_shared_rssid_mapping() const { return !_shared_rssid_map.empty(); }

    // Fills shared_rssid_map so that all rssids occupied by |rowset| map to
    // the corresponding rssid in |canonical_rowset|.
    void update_shared_rssid_map(const RowsetMetadataPB& rowset, const RowsetMetadataPB& canonical_rowset) {
        uint32_t canonical_rssid = canonical_rowset.id();
        _shared_rssid_map[rowset.id()] = canonical_rssid;
        for (int seg_pos = 0; seg_pos < rowset.segments_size(); ++seg_pos) {
            uint32_t rssid = get_rssid(rowset, seg_pos);
            uint32_t seg_offset = rssid - rowset.id();
            _shared_rssid_map[rssid] = canonical_rssid + seg_offset;
        }
    }

    bool has_next_rowset() const { return _current_rowset_index < _metadata->rowsets_size(); }
    const RowsetMetadataPB& current_rowset() const { return _metadata->rowsets(_current_rowset_index); }
    void advance_rowset() { ++_current_rowset_index; }

private:
    TabletMetadataPtr _metadata;
    int64_t _rssid_offset = 0;
    int _current_rowset_index = 0;
    // Dedup-produced rssid remap table.
    // key: original rssid in this child metadata
    // value: canonical rowset's actual rssid in the final output
    // rssid not in this map uses the default offset mapping.
    std::unordered_map<uint32_t, uint32_t> _shared_rssid_map;
};

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

Status update_canonical(RowsetMetadataPB* canonical_rowset, const RowsetMetadataPB& duplicate_rowset) {
    if (canonical_rowset->has_range() && duplicate_rowset.has_range()) {
        ASSIGN_OR_RETURN(auto merged_range,
                         tablet_reshard_helper::union_range(canonical_rowset->range(), duplicate_rowset.range()));
        canonical_rowset->mutable_range()->CopyFrom(merged_range);
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

Status merge_rowsets(std::vector<TabletMergeContext>& merge_contexts, TabletMetadataPB* new_metadata) {
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

        // Version change: update version_start_index
        if (rowset.version() != current_version) {
            current_version = rowset.version();
            version_start_index = new_metadata->rowsets_size();
        }

        // Check for duplicate in [version_start_index, end)
        int canonical_index = -1;
        for (int i = version_start_index; i < new_metadata->rowsets_size(); ++i) {
            if (is_duplicate_rowset(rowset, new_metadata->rowsets(i))) {
                canonical_index = i;
                break;
            }
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
                RETURN_IF_ERROR(update_canonical(new_metadata->mutable_rowsets(canonical_index), rowset));
            }
            // predicate: just skip
        } else {
            // First occurrence: output
            RETURN_IF_ERROR(add_rowset(merge_contexts[min_child_index], rowset, new_metadata));
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
// Per-target rebuild (rebuild_dcg_for_target) implements Steps A-F of the
// design: locate base segment via get_rssid scan, resolve merged schema,
// compute row windows per source-child rowset using the existing range ->
// SeekRange -> rowid-range pipeline, validate coverage, assemble rebuilt
// chunk (donor file per column + updater-child window overrides), write a
// new .cols file, install one entry into new_dcgs[T].

struct DcgSurvivingEntry {
    size_t child_index;
    uint32_t orig_segment_id;
    int entry_index;
    // Single-entry normalized copy of the source DCG entry (all 5 fields at
    // index 0 of the resulting PB). Keeping entries in single-entry form keeps
    // bookkeeping and downstream emission uniform.
    DeltaColumnGroupVerPB single_entry;
};

struct DcgSourceRowsetRef {
    size_t child_index;
    const RowsetMetadataPB* rowset = nullptr;
    int segment_pos = 0;
    const TabletRangePB* effective_range = nullptr; // rowset.range() else ctx tablet range; null = unbounded
};

struct DcgTargetWork {
    std::vector<DcgSurvivingEntry> entries;
    std::vector<DcgSourceRowsetRef> source_refs;
};

DeltaColumnGroupVerPB make_single_entry_dcg_from_index(const DeltaColumnGroupVerPB& src, int i) {
    DeltaColumnGroupVerPB out;
    out.add_column_files(src.column_files(i));
    out.add_unique_column_ids()->CopyFrom(src.unique_column_ids(i));
    out.add_versions(src.versions(i));
    out.add_encryption_metas(src.encryption_metas(i));
    out.add_shared_files(src.shared_files(i));
    return out;
}

// basename assertion: merge_dcg_meta joins these strings with a tablet segment
// root via TabletManager::segment_location. Reject any traversal component.
Status ensure_basename(const std::string& name) {
    if (name.empty() || name.find('/') != std::string::npos || name == "." || name == "..") {
        return Status::Corruption(fmt::format("DCG metadata contains non-basename filename: '{}'", name));
    }
    return Status::OK();
}

// Pass 1 — walk each child's dcg_meta and rowsets, dedup by filename across
// children, and accumulate source-rowset refs per target T.
Status dcg_pass1_collect(const std::vector<TabletMergeContext>& merge_contexts,
                         std::map<uint32_t, DcgTargetWork>* work_by_target) {
    // Track which .cols filenames we have already observed per target so that
    // subsequent children with the same filename are deduped (and verified).
    // Store size_t indexes into DcgTargetWork::entries (NOT raw pointers),
    // since push_back can reallocate the vector and invalidate pointers.
    std::map<uint32_t, std::unordered_map<std::string, size_t>> seen_files;

    for (size_t ci = 0; ci < merge_contexts.size(); ++ci) {
        const auto& ctx = merge_contexts[ci];

        // (a) Accumulate source-rowset refs from every rowset that references
        // any target via get_rssid -> ctx.map_rssid.
        for (const auto& rowset : ctx.metadata()->rowsets()) {
            for (int p = 0; p < rowset.segments_size(); ++p) {
                uint32_t orig_rssid = get_rssid(rowset, p);
                auto target_or = ctx.map_rssid(orig_rssid);
                if (!target_or.ok()) continue;
                uint32_t target = *target_or;
                DcgSourceRowsetRef ref;
                ref.child_index = ci;
                ref.rowset = &rowset;
                ref.segment_pos = p;
                if (rowset.has_range()) {
                    ref.effective_range = &rowset.range();
                } else if (ctx.metadata()->has_range()) {
                    ref.effective_range = &ctx.metadata()->range();
                } else {
                    ref.effective_range = nullptr; // unbounded: full segment
                }
                (*work_by_target)[target].source_refs.push_back(std::move(ref));
            }
        }

        // (b) Walk dcg_meta: validate, normalize, split into single-entry
        // records, dedup by .cols filename.
        if (!ctx.metadata()->has_dcg_meta()) continue;
        for (const auto& [segment_id, dcg_value] : ctx.metadata()->dcg_meta().dcgs()) {
            ASSIGN_OR_RETURN(uint32_t target, ctx.map_rssid(segment_id));

            DeltaColumnGroupVerPB normalized = dcg_value;
            RETURN_IF_ERROR(validate_dcg_shape(normalized));
            normalize_dcg_optional_fields(&normalized);

            for (int i = 0; i < normalized.column_files_size(); ++i) {
                const std::string& file = normalized.column_files(i);
                RETURN_IF_ERROR(ensure_basename(file));

                auto& target_work = (*work_by_target)[target];
                auto& file_map = seen_files[target];
                auto it = file_map.find(file);
                if (it != file_map.end()) {
                    // Exact dedup across children: verify entry-level consistency
                    // against the previously stored entry. Index lookup is safe
                    // even if the vector reallocated between insertions.
                    RETURN_IF_ERROR(verify_dcg_entry_consistency(target_work.entries[it->second].single_entry, 0,
                                                                 normalized, i));
                    continue;
                }

                DcgSurvivingEntry entry;
                entry.child_index = ci;
                entry.orig_segment_id = segment_id;
                entry.entry_index = i;
                entry.single_entry = make_single_entry_dcg_from_index(normalized, i);

                const size_t new_index = target_work.entries.size();
                target_work.entries.push_back(std::move(entry));
                file_map[file] = new_index;
            }
        }
    }
    return Status::OK();
}

// For a merged rowset, find the segment position p such that get_rssid(rowset, p) == T.
// Returns -1 if not found.
int find_segment_pos_in_rowset(const RowsetMetadataPB& rowset, uint32_t target) {
    for (int p = 0; p < rowset.segments_size(); ++p) {
        if (get_rssid(rowset, p) == target) {
            return p;
        }
    }
    return -1;
}

// Step A — locate the merged rowset + segment position that owns T.
StatusOr<std::pair<const RowsetMetadataPB*, int>> locate_target_in_merged(const TabletMetadataPB& new_metadata,
                                                                          uint32_t target) {
    for (const auto& rowset : new_metadata.rowsets()) {
        int p = find_segment_pos_in_rowset(rowset, target);
        if (p >= 0) return std::make_pair(&rowset, p);
    }
    return Status::InternalError(fmt::format("DCG rebuild: target rssid {} not found in merged metadata", target));
}

// Step B — resolve the merged tablet schema PB for a given rowset.
const TabletSchemaPB* resolve_rowset_schema_pb(const TabletMetadataPB& new_metadata, const RowsetMetadataPB& rowset) {
    const auto& rowset_to_schema = new_metadata.rowset_to_schema();
    const auto it = rowset_to_schema.find(rowset.id());
    if (it != rowset_to_schema.end()) {
        const auto& hist = new_metadata.historical_schemas();
        auto sit = hist.find(it->second);
        if (sit != hist.end()) return &sit->second;
    }
    if (new_metadata.has_schema()) return &new_metadata.schema();
    return nullptr;
}

struct DcgRowWindow {
    size_t child_index;
    Range<rowid_t> range;
};

// Step C — compute row windows in segment T for every source-child rowset
// that references T. Coverage over [0, num_rows_of_T) is validated.
Status compute_row_windows(TabletManager* tablet_mgr, int64_t new_tablet_id, const RowsetMetadataPB& target_rowset,
                           int target_segment_pos, const TabletSchemaCSPtr& full_tablet_schema,
                           const std::vector<DcgSourceRowsetRef>& source_refs, std::vector<DcgRowWindow>* out_windows) {
    // Open base segment for index lookups.
    RETURN_IF_ERROR(ensure_basename(target_rowset.segments(target_segment_pos)));
    FileInfo base_info;
    base_info.path = tablet_mgr->segment_location(new_tablet_id, target_rowset.segments(target_segment_pos));
    if (target_rowset.segment_size_size() > target_segment_pos) {
        base_info.size = target_rowset.segment_size(target_segment_pos);
    }
    if (target_rowset.bundle_file_offsets_size() > target_segment_pos) {
        base_info.bundle_file_offset = target_rowset.bundle_file_offsets(target_segment_pos);
    }
    if (target_rowset.segment_encryption_metas_size() > target_segment_pos) {
        base_info.encryption_meta = target_rowset.segment_encryption_metas(target_segment_pos);
    }

    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(base_info.path));
    ASSIGN_OR_RETURN(auto base_segment, Segment::open(fs, base_info, /*segment_id=*/0, full_tablet_schema,
                                                      /*footer_length_hint=*/nullptr, /*partial_rowset_footer=*/nullptr,
                                                      /*lake_io_opts=*/LakeIOOptions{}, tablet_mgr));

    const rowid_t num_rows = static_cast<rowid_t>(base_segment->num_rows());

    // Dedup source refs that produce identical windows (e.g., a shared rowset
    // referenced by multiple children with the same effective range).
    out_windows->clear();
    out_windows->reserve(source_refs.size());

    for (const auto& ref : source_refs) {
        Range<rowid_t> window{0, num_rows};
        if (ref.effective_range != nullptr) {
            ASSIGN_OR_RETURN(auto seek_range,
                             TabletRangeHelper::create_seek_range_from(*ref.effective_range, full_tablet_schema,
                                                                       /*mem_pool=*/nullptr));
            LakeIOOptions lake_io_opts{.fill_data_cache = false};
            ASSIGN_OR_RETURN(auto rowid_range_opt,
                             segment_seek_range_to_rowid_range(base_segment, seek_range, lake_io_opts));
            if (!rowid_range_opt.has_value()) {
                continue; // empty window
            }
            window = *rowid_range_opt;
            // Clip to [0, num_rows)
            window = Range<rowid_t>(std::max<rowid_t>(window.begin(), 0), std::min<rowid_t>(window.end(), num_rows));
            if (window.begin() >= window.end()) {
                continue;
            }
        }
        out_windows->push_back({ref.child_index, window});
    }

    if (out_windows->empty()) {
        return Status::NotSupported(
                fmt::format("DCG rebuild: no valid row windows computed for target rssid (num_rows={})", num_rows));
    }

    // Dedup windows that belong to the SAME child AND have the same range
    // (e.g., a child's shared rowset surfaced twice through different scans).
    // Do NOT dedup windows from different children even if the range matches:
    // those represent distinct authoritative updaters for the same rows and
    // must surface as an overlap failure, not be silently collapsed.
    std::sort(out_windows->begin(), out_windows->end(), [](const DcgRowWindow& a, const DcgRowWindow& b) {
        if (a.range.begin() != b.range.begin()) return a.range.begin() < b.range.begin();
        if (a.range.end() != b.range.end()) return a.range.end() < b.range.end();
        return a.child_index < b.child_index;
    });
    std::vector<DcgRowWindow> dedup;
    dedup.reserve(out_windows->size());
    for (auto& w : *out_windows) {
        if (!dedup.empty() && dedup.back().range.begin() == w.range.begin() &&
            dedup.back().range.end() == w.range.end() && dedup.back().child_index == w.child_index) {
            continue; // same child + same range: safe to collapse
        }
        dedup.push_back(w);
    }
    *out_windows = std::move(dedup);

    // Coverage validation: windows must be contiguous and cover [0, num_rows).
    if ((*out_windows)[0].range.begin() != 0) {
        return Status::NotSupported(
                fmt::format("DCG rebuild: row window coverage gap at the start (first.begin={}, expect 0)",
                            (*out_windows)[0].range.begin()));
    }
    for (size_t k = 0; k + 1 < out_windows->size(); ++k) {
        if ((*out_windows)[k].range.end() != (*out_windows)[k + 1].range.begin()) {
            return Status::NotSupported(fmt::format("DCG rebuild: row window coverage gap or overlap ({}->{} vs {})",
                                                    (*out_windows)[k].range.begin(), (*out_windows)[k].range.end(),
                                                    (*out_windows)[k + 1].range.begin()));
        }
    }
    if (out_windows->back().range.end() != num_rows) {
        return Status::NotSupported(
                fmt::format("DCG rebuild: row window coverage gap at the end (last.end={}, expect {})",
                            out_windows->back().range.end(), num_rows));
    }
    return Status::OK();
}

// Helper: open a source .cols file as a Segment (projection = entry's
// unique_column_ids restricted subset of the merged tablet schema).
StatusOr<std::shared_ptr<Segment>> open_source_dcg_segment(TabletManager* tablet_mgr, int64_t owner_tablet_id,
                                                           const std::string& relative_path,
                                                           const std::string& encryption_meta,
                                                           const TabletSchemaCSPtr& entry_schema) {
    RETURN_IF_ERROR(ensure_basename(relative_path));
    FileInfo info;
    info.path = tablet_mgr->segment_location(owner_tablet_id, relative_path);
    info.encryption_meta = encryption_meta;
    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(info.path));
    return Segment::open(fs, info, /*segment_id=*/0, entry_schema, /*footer_length_hint=*/nullptr,
                         /*partial_rowset_footer=*/nullptr, /*lake_io_opts=*/LakeIOOptions{}, tablet_mgr);
}

// Helper: read [lo, hi) rows of column UID from |segment| (previously opened
// with entry_schema that contains UID). The column is appended to |dst|.
Status read_column_range(const std::shared_ptr<Segment>& segment, const TabletSchemaCSPtr& entry_schema, uint32_t uid,
                         rowid_t lo, rowid_t hi, Column* dst) {
    const int32_t col_idx = entry_schema->field_index(static_cast<ColumnUID>(uid));
    if (col_idx < 0) {
        return Status::Corruption(fmt::format("DCG rebuild: source segment schema is missing column UID {}", uid));
    }
    const auto& tablet_column = entry_schema->column(col_idx);
    OlapReaderStatistics stats;

    ASSIGN_OR_RETURN(auto col_iter, segment->new_column_iterator(tablet_column, /*path=*/nullptr));

    // Build a RandomAccessFile for the segment's file (needed by ColumnIteratorOptions.read_file).
    // Use segment's cached file_size / encryption via its own API: reuse segment's new_iterator path is too heavy,
    // so we build a dedicated RandomAccessFile here.
    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(segment->file_info().path));
    RandomAccessFileOptions raf_opts;
    if (!segment->file_info().encryption_meta.empty()) {
        ASSIGN_OR_RETURN(auto info, KeyCache::instance().unwrap_encryption_meta(segment->file_info().encryption_meta));
        raf_opts.encryption_info = std::move(info);
    }
    ASSIGN_OR_RETURN(auto raf, fs->new_random_access_file_with_bundling(raf_opts, segment->file_info()));

    ColumnIteratorOptions iter_opts;
    iter_opts.read_file = raf.get();
    iter_opts.stats = &stats;
    iter_opts.lake_io_opts = LakeIOOptions{.fill_data_cache = false};
    iter_opts.chunk_size = std::max<int>(1, static_cast<int>(hi - lo));
    RETURN_IF_ERROR(col_iter->init(iter_opts));
    RETURN_IF_ERROR(col_iter->seek_to_ordinal(lo));

    size_t remaining = hi - lo;
    while (remaining > 0) {
        size_t n = remaining;
        RETURN_IF_ERROR(col_iter->next_batch(&n, dst));
        if (n == 0) {
            return Status::InternalError("DCG rebuild: column iterator returned 0 rows before exhausting range");
        }
        remaining -= n;
    }
    return Status::OK();
}

// Per-target rebuild — Steps A-F.
// Returns the single-entry PB describing the newly written .cols file.
StatusOr<DeltaColumnGroupVerPB> rebuild_dcg_for_target(TabletManager* tablet_mgr,
                                                       const std::vector<TabletMergeContext>& merge_contexts,
                                                       int64_t new_tablet_id, int64_t new_version, int64_t txn_id,
                                                       const TabletMetadataPB& new_metadata, uint32_t target,
                                                       const std::vector<uint32_t>& rebuild_columns,
                                                       const std::vector<const DcgSurvivingEntry*>& conflicting_entries,
                                                       const std::vector<DcgSourceRowsetRef>& source_refs) {
    TEST_SYNC_POINT_CALLBACK("merge_dcg_meta:before_rebuild", &target);

    // Step A — locate merged rowset + segment position for T.
    ASSIGN_OR_RETURN(auto located, locate_target_in_merged(new_metadata, target));
    const RowsetMetadataPB& target_rowset = *located.first;
    const int target_segment_pos = located.second;

    // Step B — resolve full tablet schema + rebuild schema.
    const TabletSchemaPB* pb = resolve_rowset_schema_pb(new_metadata, target_rowset);
    if (pb == nullptr) {
        return Status::NotSupported(
                fmt::format("DCG rebuild: no tablet schema available for rowset {}", target_rowset.id()));
    }
    TabletSchemaCSPtr full_tablet_schema = TabletSchema::create(*pb);
    if (full_tablet_schema->sort_key_idxes().empty()) {
        return Status::NotSupported("DCG rebuild: tablet schema has no sort key");
    }
    std::vector<ColumnUID> rebuild_uids;
    rebuild_uids.reserve(rebuild_columns.size());
    for (uint32_t u : rebuild_columns) rebuild_uids.push_back(static_cast<ColumnUID>(u));
    TabletSchemaCSPtr rebuild_schema = TabletSchema::create_with_uid(full_tablet_schema, rebuild_uids);
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
    RETURN_IF_ERROR(compute_row_windows(tablet_mgr, new_tablet_id, target_rowset, target_segment_pos,
                                        full_tablet_schema, source_refs, &windows));
    const rowid_t num_rows = windows.back().range.end();

    // For each rebuild column, pick:
    // - default donor: any conflicting entry that claims the UID (first found).
    // - per-child overrides: the conflicting entry from child C_i that claims the UID,
    //   to be used for rows in C_i's owner window.
    struct ColumnSources {
        const DcgSurvivingEntry* donor = nullptr;
        std::unordered_map<size_t, const DcgSurvivingEntry*> by_child; // child_index -> entry
    };
    std::unordered_map<uint32_t, ColumnSources> column_sources;
    for (uint32_t uid : rebuild_columns) {
        for (const DcgSurvivingEntry* e : conflicting_entries) {
            bool claims = false;
            for (auto c : e->single_entry.unique_column_ids(0).column_ids()) {
                if (c == uid) {
                    claims = true;
                    break;
                }
            }
            if (!claims) continue;
            auto& cs = column_sources[uid];
            if (cs.donor == nullptr) cs.donor = e;
            cs.by_child[e->child_index] = e;
        }
        if (column_sources[uid].donor == nullptr) {
            return Status::InternalError(fmt::format("DCG rebuild: no donor found for column UID {}", uid));
        }
    }

    // Open each referenced source .cols segment (cached per entry address).
    std::unordered_map<const DcgSurvivingEntry*, std::shared_ptr<Segment>> opened_sources;
    std::unordered_map<const DcgSurvivingEntry*, TabletSchemaCSPtr> entry_schemas;

    auto get_source_segment = [&](const DcgSurvivingEntry* e) -> StatusOr<std::shared_ptr<Segment>> {
        auto it = opened_sources.find(e);
        if (it != opened_sources.end()) return it->second;
        std::vector<ColumnUID> uids;
        for (auto c : e->single_entry.unique_column_ids(0).column_ids()) uids.push_back(static_cast<ColumnUID>(c));
        TabletSchemaCSPtr entry_schema = TabletSchema::create_with_uid(full_tablet_schema, uids);
        int64_t owner_tablet_id = merge_contexts[e->child_index].metadata()->id();
        const std::string& file = e->single_entry.column_files(0);
        const std::string& enc = e->single_entry.encryption_metas(0);
        ASSIGN_OR_RETURN(auto seg, open_source_dcg_segment(tablet_mgr, owner_tablet_id, file, enc, entry_schema));
        entry_schemas[e] = entry_schema;
        opened_sources[e] = seg;
        return seg;
    };

    TEST_SYNC_POINT_CALLBACK("merge_dcg_meta:after_open_sources", &target);

    // Step D — assemble columns IN rebuild_schema ORDER. TabletSchema::create_with_uid
    // preserves the base schema's column order, which can differ from the
    // insertion order of `rebuild_columns`. Chunk binds columns positionally,
    // so we must iterate schema positions, not UID insertion order, to avoid
    // writing column data into the wrong (UID, type) slot.
    //
    // Full-column materialization keeps this code path simple and correct;
    // DCG files are already at segment size, so the peak is bounded by a
    // single rebuilt column over the full segment. Row-batch streaming is a
    // future optimization.
    const size_t num_cols = rebuild_schema->num_columns();
    Columns rebuilt_columns(num_cols);
    std::vector<uint32_t> ordered_uids;
    ordered_uids.reserve(num_cols);
    for (size_t col_idx = 0; col_idx < num_cols; ++col_idx) {
        const auto& tablet_col = rebuild_schema->column(col_idx);
        const uint32_t uid = static_cast<uint32_t>(tablet_col.unique_id());
        ordered_uids.push_back(uid);

        auto cs_it = column_sources.find(uid);
        if (cs_it == column_sources.end()) {
            return Status::InternalError(fmt::format("DCG rebuild: rebuild_schema has UID {} with no source", uid));
        }
        const auto& cs = cs_it->second;

        auto field = ChunkHelper::convert_field(col_idx, tablet_col);
        MutableColumnPtr out_col = ChunkHelper::column_from_field(field);
        out_col->reserve(num_rows);

        for (const auto& w : windows) {
            const DcgSurvivingEntry* source = nullptr;
            auto override_it = cs.by_child.find(w.child_index);
            source = (override_it != cs.by_child.end()) ? override_it->second : cs.donor;
            ASSIGN_OR_RETURN(auto seg, get_source_segment(source));
            RETURN_IF_ERROR(
                    read_column_range(seg, entry_schemas[source], uid, w.range.begin(), w.range.end(), out_col.get()));
        }

        if (out_col->size() != static_cast<size_t>(num_rows)) {
            return Status::InternalError(
                    fmt::format("DCG rebuild: column UID {} size {} != num_rows {}", uid, out_col->size(), num_rows));
        }
        rebuilt_columns[col_idx] = std::move(out_col);
    }

    // Step F — write new .cols file.
    Schema output_schema = ChunkHelper::convert_schema(rebuild_schema);
    auto out_chunk = std::make_shared<Chunk>(std::move(rebuilt_columns), std::make_shared<Schema>(output_schema));

    const std::string new_file_basename = gen_cols_filename(txn_id);
    const std::string new_path = tablet_mgr->segment_location(new_tablet_id, new_file_basename);
    WritableFileOptions wopts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    SegmentWriterOptions writer_opts;
    if (new_metadata.has_flat_json_config()) {
        writer_opts.flat_json_config = std::make_shared<FlatJsonConfig>();
        writer_opts.flat_json_config->update(new_metadata.flat_json_config());
    }
    if (config::enable_transparent_data_encryption) {
        ASSIGN_OR_RETURN(auto pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
        wopts.encryption_info = pair.info;
        writer_opts.encryption_meta = std::move(pair.encryption_meta);
    }
    ASSIGN_OR_RETURN(auto wfile, fs::new_writable_file(wopts, new_path));
    auto segment_writer =
            std::make_unique<SegmentWriter>(std::move(wfile), /*segment_id=*/0, rebuild_schema, writer_opts);
    RETURN_IF_ERROR(segment_writer->init(false));
    RETURN_IF_ERROR(segment_writer->append_chunk(*out_chunk));
    uint64_t seg_file_size = 0;
    uint64_t index_size = 0;
    uint64_t footer_position = 0;
    RETURN_IF_ERROR(segment_writer->finalize(&seg_file_size, &index_size, &footer_position));

    TEST_SYNC_POINT_CALLBACK("merge_dcg_meta:after_write_cols", const_cast<std::string*>(&new_file_basename));

    // Step E — build single-entry PB for the rebuilt file. Emit unique_column_ids
    // in the SAME order as the written chunk's columns (rebuild_schema order).
    // Mismatched order would cause reader schema binding to mismatch the physical
    // column positions in the .cols segment.
    DeltaColumnGroupVerPB rebuilt;
    rebuilt.add_column_files(new_file_basename);
    auto* uids_pb = rebuilt.add_unique_column_ids();
    for (uint32_t uid : ordered_uids) uids_pb->add_column_ids(uid);
    rebuilt.add_versions(new_version);
    rebuilt.add_encryption_metas(segment_writer->encryption_meta());
    rebuilt.add_shared_files(false);
    return rebuilt;
}

Status merge_dcg_meta(TabletManager* tablet_mgr, const std::vector<TabletMergeContext>& merge_contexts,
                      int64_t new_tablet_id, int64_t new_version, int64_t txn_id, TabletMetadataPB* new_metadata) {
    std::map<uint32_t, DcgTargetWork> work_by_target;
    RETURN_IF_ERROR(dcg_pass1_collect(merge_contexts, &work_by_target));

    auto* new_dcgs = new_metadata->mutable_dcg_meta()->mutable_dcgs();

    // Track full paths of rebuilt .cols files so we can best-effort clean them
    // up if a later target's rebuild fails partway through. Downstream failures
    // (merge_delvecs/merge_sstables/publish) still rely on standard orphan-file
    // vacuum, which matches the pattern used by merge_delvec_files.
    std::vector<std::string> rebuilt_paths;
    auto cleanup_on_failure = [&]() {
        for (const auto& p : rebuilt_paths) {
            auto st = fs::delete_file(p);
            LOG_IF(WARNING, !st.ok() && !st.is_not_found())
                    << "failed to clean up partial DCG rebuild file " << p << ": " << st;
        }
        rebuilt_paths.clear();
    };

    for (auto& [target, work] : work_by_target) {
        if (work.entries.empty()) continue;

        // Build column UID -> entries index (stable indices, not pointers).
        std::unordered_map<uint32_t, std::vector<size_t>> uid_to_entry_indices;
        for (size_t ei = 0; ei < work.entries.size(); ++ei) {
            for (auto uid : work.entries[ei].single_entry.unique_column_ids(0).column_ids()) {
                uid_to_entry_indices[uid].push_back(ei);
            }
        }

        // Identify conflicting entries: any entry claiming a UID shared with another entry.
        std::vector<bool> is_conflicting(work.entries.size(), false);
        for (auto& [uid, indices] : uid_to_entry_indices) {
            if (indices.size() > 1) {
                for (size_t ei : indices) is_conflicting[ei] = true;
            }
        }

        DeltaColumnGroupVerPB final_dcg;

        // Emit non-conflicting entries unchanged.
        for (size_t ei = 0; ei < work.entries.size(); ++ei) {
            if (is_conflicting[ei]) continue;
            const auto& e = work.entries[ei];
            final_dcg.add_column_files(e.single_entry.column_files(0));
            final_dcg.add_unique_column_ids()->CopyFrom(e.single_entry.unique_column_ids(0));
            final_dcg.add_versions(e.single_entry.versions(0));
            final_dcg.add_encryption_metas(e.single_entry.encryption_metas(0));
            final_dcg.add_shared_files(e.single_entry.shared_files(0));
        }

        bool has_conflict = false;
        for (bool b : is_conflicting) has_conflict |= b;

        if (has_conflict) {
            // Fold ALL columns of every conflicting entry into rebuild_columns
            // so the reader's first-entry-wins rule can't leak stale values.
            std::vector<uint32_t> rebuild_columns;
            std::unordered_set<uint32_t> seen;
            std::vector<const DcgSurvivingEntry*> conflicting_entries;
            for (size_t ei = 0; ei < work.entries.size(); ++ei) {
                if (!is_conflicting[ei]) continue;
                conflicting_entries.push_back(&work.entries[ei]);
                for (auto uid : work.entries[ei].single_entry.unique_column_ids(0).column_ids()) {
                    if (seen.insert(uid).second) rebuild_columns.push_back(uid);
                }
            }

            StatusOr<DeltaColumnGroupVerPB> rebuilt_or = rebuild_dcg_for_target(
                    tablet_mgr, merge_contexts, new_tablet_id, new_version, txn_id, *new_metadata, target,
                    rebuild_columns, conflicting_entries, work.source_refs);
            if (!rebuilt_or.ok()) {
                if (rebuilt_or.status().is_not_supported()) {
                    g_tablet_merge_dcg_rebuild_fallback_not_supported_total << 1;
                }
                cleanup_on_failure();
                return rebuilt_or.status();
            }
            const auto& rb = *rebuilt_or;
            rebuilt_paths.push_back(tablet_mgr->segment_location(new_tablet_id, rb.column_files(0)));
            final_dcg.add_column_files(rb.column_files(0));
            final_dcg.add_unique_column_ids()->CopyFrom(rb.unique_column_ids(0));
            final_dcg.add_versions(rb.versions(0));
            final_dcg.add_encryption_metas(rb.encryption_metas(0));
            final_dcg.add_shared_files(rb.shared_files(0));
            g_tablet_merge_dcg_rebuild_total << 1;
        }

        if (final_dcg.column_files_size() == 0) continue;
        auto shape_st = validate_dcg_shape(final_dcg);
        if (!shape_st.ok()) {
            cleanup_on_failure();
            return shape_st;
        }
        (*new_dcgs)[target] = std::move(final_dcg);
    }

    return Status::OK();
}

Status merge_delvecs(TabletManager* tablet_manager, const std::vector<TabletMergeContext>& merge_contexts,
                     int64_t new_version, int64_t txn_id, TabletMetadataPB* new_metadata) {
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

    if (unique_delvec_files.empty()) {
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

    // Phase 4: Write one file (old files concatenated + union_buffer appended)
    FileMetaPB new_delvec_file;
    std::vector<uint64_t> offsets;
    uint64_t union_base_offset = 0;
    RETURN_IF_ERROR(merge_delvec_files(tablet_manager, unique_delvec_files, new_metadata->id(), txn_id,
                                       &new_delvec_file, &offsets, Slice(union_buffer), &union_base_offset));

    // Build base_offset_by_file_name
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

Status merge_sstables(const std::vector<TabletMergeContext>& merge_contexts, TabletMetadataPB* new_metadata) {
    auto* dest = new_metadata->mutable_sstable_meta()->mutable_sstables();
    // key: filename -> index in dest, for shared dedup + consistency check
    std::unordered_map<std::string, int> shared_dedup;

    for (const auto& ctx : merge_contexts) {
        if (!ctx.metadata()->has_sstable_meta()) continue;

        for (const auto& sst : ctx.metadata()->sstable_meta().sstables()) {
            // Dedup: only shared sstables can be duplicates
            if (sst.shared()) {
                auto [it, inserted] = shared_dedup.emplace(sst.filename(), dest->size());
                if (!inserted) {
                    // Dedup hit: consistency check (only fields that are not projected)
                    const auto& existing = dest->Get(it->second);
                    if (existing.filesize() != sst.filesize() || existing.encryption_meta() != sst.encryption_meta() ||
                        existing.range().start_key() != sst.range().start_key() ||
                        existing.range().end_key() != sst.range().end_key() ||
                        existing.fileset_id().hi() != sst.fileset_id().hi() ||
                        existing.fileset_id().lo() != sst.fileset_id().lo()) {
                        return Status::Corruption("Shared sstable metadata mismatch for same filename");
                    }
                    continue; // skip duplicate
                }
            }

            // Projection: branch on has_shared_rssid, not on shared flag
            auto* out = dest->Add();
            out->CopyFrom(sst);

            if (sst.has_shared_rssid()) {
                // shared_rssid path: project rssid, clear offset, project delvec
                ASSIGN_OR_RETURN(auto mapped_rssid, ctx.map_rssid(sst.shared_rssid()));
                out->set_shared_rssid(mapped_rssid);
                out->set_rssid_offset(0); // clear to avoid double-transform in read path

                // max_rss_rowid: replace high part with mapped rssid
                uint64_t low = sst.max_rss_rowid() & 0xffffffffULL;
                out->set_max_rss_rowid((static_cast<uint64_t>(mapped_rssid) << 32) | low);

                // delvec projection via new_metadata->delvec_meta()
                if (sst.has_delvec() && sst.delvec().size() > 0) {
                    auto dv_it = new_metadata->delvec_meta().delvecs().find(mapped_rssid);
                    if (dv_it == new_metadata->delvec_meta().delvecs().end()) {
                        return Status::Corruption("Delvec page not found for sstable after merge");
                    }
                    out->mutable_delvec()->CopyFrom(dv_it->second);
                }
            } else {
                // No shared_rssid (legacy format): use rssid_offset
                if (sst.has_delvec() && sst.delvec().size() > 0) {
                    return Status::Corruption("Sstable has delvec but no shared_rssid, cannot project delvec");
                }
                out->set_rssid_offset(static_cast<int32_t>(ctx.rssid_offset()));
                uint64_t low = sst.max_rss_rowid() & 0xffffffffULL;
                int64_t high = static_cast<int64_t>(sst.max_rss_rowid() >> 32);
                out->set_max_rss_rowid((static_cast<uint64_t>(high + ctx.rssid_offset()) << 32) | low);
                out->clear_delvec();
            }
        }
    }

    return Status::OK();
}

void update_next_rowset_id(TabletMetadataPB* metadata) {
    uint32_t max_end = 1; // invariant: next_rowset_id >= 1
    for (const auto& rowset : metadata->rowsets()) {
        max_end = std::max(max_end, rowset.id() + get_rowset_id_step(rowset));
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

    // Phase 1: Prepare rssid offsets and merged range
    for (size_t i = 1; i < merge_contexts.size(); ++i) {
        // Temporarily set next_rowset_id for compute_rssid_offset
        uint32_t temp_next = merge_contexts.front().metadata()->next_rowset_id();
        for (size_t j = 0; j < i; ++j) {
            for (const auto& rowset : merge_contexts[j].metadata()->rowsets()) {
                uint32_t end = static_cast<uint32_t>(static_cast<int64_t>(rowset.id() + get_rowset_id_step(rowset)) +
                                                     merge_contexts[j].rssid_offset());
                temp_next = std::max(temp_next, end);
            }
        }
        new_tablet_metadata->set_next_rowset_id(temp_next);
        merge_contexts[i].set_rssid_offset(compute_rssid_offset(*new_tablet_metadata, *merge_contexts[i].metadata()));
    }

    // Merge tablet-level range via union_range
    TabletRangePB merged_range = merge_contexts.front().metadata()->range();
    for (size_t i = 1; i < merge_contexts.size(); ++i) {
        ASSIGN_OR_RETURN(merged_range,
                         tablet_reshard_helper::union_range(merged_range, merge_contexts[i].metadata()->range()));
    }
    new_tablet_metadata->mutable_range()->CopyFrom(merged_range);

    // Phase 2: Merge rowsets (version-driven k-way merge with dedup)
    RETURN_IF_ERROR(merge_rowsets(merge_contexts, new_tablet_metadata.get()));

    // Phase 2.5: Merge schemas (must run before merge_dcg_meta, which needs
    // historical_schemas to locate rebuild schemas for shared-segment rebuild).
    merge_schemas(merge_contexts, new_tablet_metadata.get());

    // Phase 3: Projections (map_rssid uses shared_rssid_map + rssid_offset)
    RETURN_IF_ERROR(merge_dcg_meta(tablet_manager, merge_contexts, merging_tablet.new_tablet_id(), new_version,
                                   txn_info.txn_id(), new_tablet_metadata.get()));

    if (is_primary_key(*new_tablet_metadata)) {
        RETURN_IF_ERROR(merge_delvecs(tablet_manager, merge_contexts, new_version, txn_info.txn_id(),
                                      new_tablet_metadata.get()));
    }

    RETURN_IF_ERROR(merge_sstables(merge_contexts, new_tablet_metadata.get()));

    // Phase 4: Finalize
    update_next_rowset_id(new_tablet_metadata.get());

    return new_tablet_metadata;
}

} // namespace starrocks::lake

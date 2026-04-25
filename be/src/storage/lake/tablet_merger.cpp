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
#include "storage/lake/update_manager.h"
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
    // Reseat the backing metadata pointer. Used by flush_persistent_index to
    // substitute a spliced snapshot (same rowsets, sstable_meta updated to
    // include freshly-flushed PK-index sstables).
    void set_metadata(TabletMetadataPtr metadata) { _metadata = std::move(metadata); }

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
            bool entry_claims_this_column = false;
            for (auto claimed_unique_id : entry->single_entry.unique_column_ids(0).column_ids()) {
                if (claimed_unique_id == unique_id) {
                    entry_claims_this_column = true;
                    break;
                }
            }
            if (!entry_claims_this_column) continue;
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

        // Build column UID -> entries index (stable indices, not pointers).
        std::unordered_map<uint32_t, std::vector<size_t>> entry_indices_by_unique_id;
        for (size_t entry_index = 0; entry_index < target_work.entries.size(); ++entry_index) {
            for (auto unique_id : target_work.entries[entry_index].single_entry.unique_column_ids(0).column_ids()) {
                entry_indices_by_unique_id[unique_id].push_back(entry_index);
            }
        }

        // Identify conflicting entries: any entry claiming a UID shared with another entry.
        std::vector<bool> entry_is_conflicting(target_work.entries.size(), false);
        for (auto& [unique_id, entry_indices] : entry_indices_by_unique_id) {
            if (entry_indices.size() > 1) {
                for (size_t entry_index : entry_indices) entry_is_conflicting[entry_index] = true;
            }
        }

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

Status merge_sstables(TabletManager* tablet_manager, std::vector<TabletMergeContext>& merge_contexts,
                      TabletMetadataPB* new_metadata) {
    auto* dest = new_metadata->mutable_sstable_meta()->mutable_sstables();
    // key: filename -> index in dest, for shared dedup + consistency check
    std::unordered_map<std::string, int> shared_dedup;

    auto* update_manager = tablet_manager->update_mgr();
    for (auto& ctx : merge_contexts) {
        // Flush the tablet's PK-index memtable into sstables so that the
        // inherited sstable_meta covers all live data of its rowsets. Covers
        // the case where a child accumulated post-split DML that never
        // reached shared storage before merge; see the symmetric call in
        // split_tablet for the pre-split side of the invariant.
        ASSIGN_OR_RETURN(auto flushed_metadata, update_manager->flush_pk_memtable(ctx.metadata()));
        ctx.set_metadata(std::move(flushed_metadata));
        if (!ctx.metadata()->has_sstable_meta()) continue;

        for (const auto& sst : ctx.metadata()->sstable_meta().sstables()) {
            // Dedup: only shared sstables can be duplicates
            if (sst.shared()) {
                auto [it, inserted] = shared_dedup.emplace(sst.filename(), dest->size());
                if (!inserted) {
                    // Dedup hit: check the fields that identify the physical file.
                    // fileset_id is intentionally excluded: it is a grouping hint that
                    // PersistentIndexSstableFileset::init may synthesize per-load when
                    // the source sstable has no persisted id, so two ctxs sharing the
                    // same legacy file can legitimately hold different ids. The merged
                    // tablet re-derives grouping from whatever id the dedup keeps.
                    const auto& existing = dest->Get(it->second);
                    if (existing.filesize() != sst.filesize() || existing.encryption_meta() != sst.encryption_meta() ||
                        existing.range().start_key() != sst.range().start_key() ||
                        existing.range().end_key() != sst.range().end_key()) {
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
                // No shared_rssid (legacy format): accumulate rssid_offset so that a
                // stacked merge (parent sstable already has an offset from a prior
                // merge) composes correctly. The read path at
                // persistent_index_sstable.cpp:214 adds the sstable's rssid_offset
                // once to each stored rssid, so the stored offset must be the total
                // cumulative shift from the original rowset-id to the current tablet.
                if (sst.has_delvec() && sst.delvec().size() > 0) {
                    return Status::Corruption("Sstable has delvec but no shared_rssid, cannot project delvec");
                }
                const int64_t accumulated_offset = static_cast<int64_t>(sst.rssid_offset()) + ctx.rssid_offset();
                if (accumulated_offset < std::numeric_limits<int32_t>::min() ||
                    accumulated_offset > std::numeric_limits<int32_t>::max()) {
                    return Status::Corruption(fmt::format(
                            "accumulated rssid_offset exceeds int32 range: sst_offset={} ctx_offset={} sum={}",
                            sst.rssid_offset(), ctx.rssid_offset(), accumulated_offset));
                }
                out->set_rssid_offset(static_cast<int32_t>(accumulated_offset));
                uint64_t low = sst.max_rss_rowid() & 0xffffffffULL;
                int64_t high = static_cast<int64_t>(sst.max_rss_rowid() >> 32);
                const int64_t new_high = high + ctx.rssid_offset();
                if (new_high < 0 || new_high > std::numeric_limits<uint32_t>::max()) {
                    return Status::Corruption(
                            fmt::format("rssid high overflow in merge projection: high={} ctx_offset={} new_high={}",
                                        high, ctx.rssid_offset(), new_high));
                }
                out->set_max_rss_rowid((static_cast<uint64_t>(new_high) << 32) | low);
                out->clear_delvec();
            }
        }
    }

    // The merge above appends sstables in source-child iteration order. The post-projection
    // max_rss_rowid is what LakePersistentIndex::commit() and the size-tiered compaction
    // strategy use to enforce the index-wide ordering invariant. If different children
    // contributed sstables whose projected (rssid<<32|rowid) values interleave — for
    // example, when a tombstone-bearing delete-only sstable in one child has its low
    // word saturated near UINT32_MAX and a freshly-written sstable in the next child
    // has a smaller projected high word — the source-order output would carry the
    // disorder forward and any later commit/compaction on the merged tablet would
    // refuse to publish with "sstables are not ordered". Reorder defensively here so
    // the merged metadata always satisfies the invariant.
    //
    // Constraint: a single naive sort by max_rss_rowid alone breaks a separate invariant
    // that LakePersistentIndex::init() and apply_opcompaction rely on — sstables sharing
    // the same fileset_id must be contiguous in metadata order. Same fileset_id sstables
    // can span a wide max_rss_rowid range (a fileset accretes via append() across
    // multiple memtable flushes, persistent_index_sstable_fileset.cpp:96), and other
    // fileset_ids' sstables can fall within that range. A flat sort by max_rss_rowid
    // would interleave them, splitting one logical fileset into multiple physical
    // filesets in init()'s grouping (which keys on adjacent fileset_id) and confusing
    // apply_opcompaction's contiguous-range find_if (lake_persistent_index.cpp:838).
    //
    // Approach: bucket source-order entries into contiguous-fileset_id blocks, sort
    // blocks by their first element's max_rss_rowid, and emit. This:
    //   * Keeps same-fileset_id sstables together (init / apply_opcompaction invariant).
    //   * Within a block, preserves source order — already monotonic in max_rss_rowid
    //     per source child since the projection paths preserve relative order
    //     (rssid_offset path adds a constant; shared_rssid path collapses to a single
    //     mapped_rssid with low word in range order).
    //   * Across blocks, orders by first-element max_rss_rowid — the sort key the flat
    //     sort would have used. Cross-block monotonicity matches the original PR's
    //     intent for cross-source ordering.
    //
    // Compare as `int64_t` rather than `uint64_t` to match the downstream check:
    // LakePersistentIndex::commit() (lake_persistent_index.cpp:880-881) fetches
    // max_rss_rowid into an `int64_t` and does a signed `>` comparison. When the
    // encoded (rssid<<32|rowid) sets the high bit — e.g. an ingest_sst entry with
    // rssid >= 2^31, or a delete-only sstable whose memtable max_rss_rowid was set to
    // (rowset_id<<32|UINT32_MAX) (persistent_index_memtable.cpp:110, 131) — unsigned
    // ordering is the reverse of signed ordering against any low-rssid sibling.
    // Sstables without an explicit fileset_id are not part of any logical fileset
    // group (they predate fileset_id, or are test fixtures). Treat each as its own
    // singleton block so block-sort orders them by max_rss_rowid like the original
    // flat sort would. When both sides have a fileset_id, only consider them in the
    // same block if the ids match.
    auto same_block_as_predecessor = [](const PersistentIndexSstablePB& prev, const PersistentIndexSstablePB& cur) {
        if (!prev.has_fileset_id() || !cur.has_fileset_id()) return false;
        return prev.fileset_id().hi() == cur.fileset_id().hi() && prev.fileset_id().lo() == cur.fileset_id().lo();
    };

    struct SstableBlock {
        std::vector<PersistentIndexSstablePB> sstables;
        int64_t sort_key = 0;
    };
    std::vector<SstableBlock> blocks;
    blocks.reserve(dest->size());
    for (const auto& sst : *dest) {
        if (blocks.empty() || !same_block_as_predecessor(blocks.back().sstables.back(), sst)) {
            blocks.emplace_back();
            blocks.back().sort_key = static_cast<int64_t>(sst.max_rss_rowid());
        }
        blocks.back().sstables.push_back(sst);
    }
    std::stable_sort(blocks.begin(), blocks.end(),
                     [](const SstableBlock& a, const SstableBlock& b) { return a.sort_key < b.sort_key; });

    dest->Clear();
    for (const auto& block : blocks) {
        for (const auto& sst : block.sstables) {
            *dest->Add() = sst;
        }
    }

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
            uint64_t projected_high = sst.max_rss_rowid() >> 32;
            // Clamp the projected high to uint32 range; rssids are uint32 elsewhere
            // and any saturated high word would already break the rssid encoding.
            if (projected_high < std::numeric_limits<uint32_t>::max()) {
                max_end = std::max(max_end, static_cast<uint32_t>(projected_high) + 1);
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

    RETURN_IF_ERROR(merge_sstables(tablet_manager, merge_contexts, new_tablet_metadata.get()));

    // Phase 4: Finalize
    update_next_rowset_id(new_tablet_metadata.get());

    return new_tablet_metadata;
}

} // namespace starrocks::lake

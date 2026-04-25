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

#include <algorithm>
#include <limits>
#include <map>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "base/hash/crc32c.h"
#include "base/testutil/sync_point.h"
#include "storage/del_vector.h"
#include "storage/lake/filenames.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reshard_helper.h"
#include "storage/lake/update_manager.h"
#include "storage/options.h"

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

Status merge_dcg_meta(const std::vector<TabletMergeContext>& merge_contexts, TabletMetadataPB* new_metadata) {
    auto* new_dcgs = new_metadata->mutable_dcg_meta()->mutable_dcgs();

    for (const auto& ctx : merge_contexts) {
        if (!ctx.metadata()->has_dcg_meta()) continue;

        for (const auto& [segment_id, dcg_value] : ctx.metadata()->dcg_meta().dcgs()) {
            ASSIGN_OR_RETURN(uint32_t target, ctx.map_rssid(segment_id));

            // Validate + normalize incoming (copy because dcg_value is const)
            DeltaColumnGroupVerPB incoming = dcg_value;
            RETURN_IF_ERROR(validate_dcg_shape(incoming));
            normalize_dcg_optional_fields(&incoming);

            auto it = new_dcgs->find(target);
            if (it == new_dcgs->end()) {
                (*new_dcgs)[target] = std::move(incoming);
                continue;
            }

            // Entry-level merge into existing DCG
            auto& existing = it->second;

            for (int i = 0; i < incoming.column_files_size(); ++i) {
                const auto& new_file = incoming.column_files(i);

                // Step 1: exact dedup — same .cols filename
                bool found_dup = false;
                for (int j = 0; j < existing.column_files_size(); ++j) {
                    if (existing.column_files(j) == new_file) {
                        RETURN_IF_ERROR(verify_dcg_entry_consistency(existing, j, incoming, i));
                        found_dup = true;
                        break;
                    }
                }
                if (found_dup) continue;

                // Step 2: column overlap check
                for (int j = 0; j < existing.unique_column_ids_size(); ++j) {
                    for (auto e_cid : existing.unique_column_ids(j).column_ids()) {
                        for (auto n_cid : incoming.unique_column_ids(i).column_ids()) {
                            if (e_cid == n_cid) {
                                return Status::NotSupported(
                                        "DCG conflict: same column updated independently by different split children");
                            }
                        }
                    }
                }

                // Step 3: disjoint columns — append entry (all 5 fields)
                existing.add_column_files(new_file);
                existing.add_unique_column_ids()->CopyFrom(incoming.unique_column_ids(i));
                existing.add_versions(incoming.versions(i));
                existing.add_encryption_metas(incoming.encryption_metas(i));
                existing.add_shared_files(incoming.shared_files(i));
            }
        }
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

    // Phase 3: Projections (map_rssid uses shared_rssid_map + rssid_offset)
    RETURN_IF_ERROR(merge_dcg_meta(merge_contexts, new_tablet_metadata.get()));

    if (is_primary_key(*new_tablet_metadata)) {
        RETURN_IF_ERROR(merge_delvecs(tablet_manager, merge_contexts, new_version, txn_info.txn_id(),
                                      new_tablet_metadata.get()));
    }

    RETURN_IF_ERROR(merge_sstables(tablet_manager, merge_contexts, new_tablet_metadata.get()));

    // Phase 4: Finalize
    merge_schemas(merge_contexts, new_tablet_metadata.get());
    update_next_rowset_id(new_tablet_metadata.get());

    return new_tablet_metadata;
}

} // namespace starrocks::lake

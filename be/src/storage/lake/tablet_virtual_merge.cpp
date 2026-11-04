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

#include "storage/lake/tablet_virtual_merge.h"

#include <algorithm>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "base/hash/crc32c.h"
#include "storage/del_vector.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reshard_helper.h"
#include "storage/options.h"

namespace starrocks::lake {

namespace {

// One page a target rssid draws from, and the child it lives in. The child matters: get_del_vec
// resolves the file path through the metadata it is handed, so a page must be read against its own
// child's metadata, not any sibling's.
struct AliasDelvecSource {
    size_t child_index = 0;
    DelvecPageInfo info;
};

// One logical rowset of the alias and every child occurrence of it.
struct AliasRowset {
    uint32_t target_id = 0;
    // How many rssids to reserve after its id -- get_rowset_id_step, i.e. the highest segment index plus
    // one. That equals the segment count while the indices are contiguous, and exceeds it once partial
    // compaction or batch merge leaves them sparse; reserving the span either way keeps the emitted ids
    // from overlapping. Del files add nothing: a del file's rssid reuses the index of the segment it
    // follows (resolve_del_op_offset resolves to a segment index either way).
    uint32_t segment_cnt = 1;
    std::vector<std::pair<size_t, uint32_t>> sources; // (child index, the id that child gave it)
};

} // namespace

StatusOr<MutableTabletMetadataPtr> virtual_merge_for_read(TabletManager* tablet_manager,
                                                          const std::vector<TabletMetadataPtr>& child_metadatas,
                                                          const MergingTabletInfoPB& merging_tablet,
                                                          int64_t new_version, const TxnInfoPB& txn_info) {
    if (child_metadatas.empty()) {
        return Status::InvalidArgument("virtual merge: no child tablet metadata");
    }
    for (const auto& child : child_metadatas) {
        if (child == nullptr) {
            return Status::InvalidArgument("virtual merge: child tablet metadata is null");
        }
        // A read of a column-mode partial update or a lake ADD INDEX resolves these per segment, and
        // nothing here projects them -- the rowsets are renumbered, so a carried-over key would name the
        // wrong segment. Refuse rather than drop them silently. build_parent_tablet_metadata refuses such
        // a child before reaching here; this is the same answer for anyone else who calls in.
        if ((child->has_dcg_meta() && !child->dcg_meta().dcgs().empty()) ||
            (child->has_idg_meta() && !child->idg_meta().idgs().empty())) {
            return Status::NotSupported(
                    fmt::format("virtual merge does not support DCG or IDG metadata, child={}", child->id()));
        }
    }

    // Identity, and the fields a view must not inherit. Everything else comes from the first child: the
    // schema, the key type, the index settings -- all identical across siblings of one split.
    auto alias = std::make_shared<TabletMetadataPB>(*child_metadatas.front());
    alias->set_id(merging_tablet.new_tablet_id());
    alias->set_version(new_version);
    alias->set_commit_time(txn_info.commit_time());
    alias->set_gtid(txn_info.gtid());
    alias->clear_rowsets();
    alias->clear_delvec_meta();
    // Every sidecar keyed by rssid, dropped rather than inherited: the alias renumbers the rowsets below,
    // so a carried-over key would point at the wrong segment. Nothing needs them -- a read of this shape
    // wants rowsets and delete vectors, and the caller refuses a child that carries DCG or IDG at all.
    alias->clear_sstable_meta();
    alias->clear_dcg_meta();
    alias->clear_idg_meta();
    alias->clear_rowset_to_schema();
    alias->clear_compaction_inputs();
    alias->clear_orphan_files();
    alias->clear_prev_garbage_version();
    alias->set_cumulative_point(0);

    // The async vector-index build watermark has to be the MIN over the children, or the build task would
    // skip a sibling's unbuilt rowsets. Absence is distinct from zero here, exactly as in a real merge.
    bool any_built_version = false;
    int64_t min_built_version = std::numeric_limits<int64_t>::max();
    for (const auto& child : child_metadatas) {
        any_built_version |= child->has_vector_index_built_version();
        min_built_version = std::min(min_built_version, child->vector_index_built_version());
    }
    if (any_built_version) {
        alias->set_vector_index_built_version(min_built_version);
    } else {
        alias->clear_vector_index_built_version();
    }

    // The parent's range is the union of its children's -- they partition it.
    TabletRangePB merged_range = child_metadatas.front()->range();
    for (size_t i = 1; i < child_metadatas.size(); ++i) {
        ASSIGN_OR_RETURN(merged_range, tablet_reshard_helper::union_range(merged_range, child_metadatas[i]->range()));
    }
    alias->mutable_range()->CopyFrom(merged_range);

    // Group by `uid`, the global id of a rowset's data that a split copies verbatim into every child -- the
    // same key a real MERGE dedups by. The per-tablet `id` is not that key: after the split the children
    // allocate ids independently from the same starting point, so their own new rowsets collide on it.
    std::map<std::pair<int64_t, int64_t>, size_t> group_of_uid;
    std::vector<AliasRowset> groups; // first-seen order, and the order the alias emits
    uint32_t next_id = 1;

    for (size_t child_index = 0; child_index < child_metadatas.size(); ++child_index) {
        for (const auto& rowset : child_metadatas[child_index]->rowsets()) {
            DCHECK(tablet_reshard_helper::has_valid_uid(rowset))
                    << "rowset reaching the virtual merge must carry a valid uid: rowset_id=" << rowset.id()
                    << " version=" << rowset.version();
            if (!tablet_reshard_helper::has_valid_uid(rowset)) {
                return Status::InternalError("rowset reaching the virtual merge has no uid");
            }
            const std::pair<int64_t, int64_t> uid{rowset.uid().hi(), rowset.uid().lo()};
            auto [iter, inserted] = group_of_uid.try_emplace(uid, groups.size());
            if (inserted) {
                // A fresh sequential id, wide enough for every rssid the rowset owns. Nothing allocates
                // from a read alias -- writes go to the children -- so the ids only have to be distinct.
                auto* emitted = alias->add_rowsets();
                emitted->CopyFrom(rowset);
                emitted->set_id(next_id);
                groups.push_back(AliasRowset{next_id, get_rowset_id_step(rowset), {}});
                next_id += groups.back().segment_cnt;
            } else {
                auto* emitted = alias->mutable_rowsets(static_cast<int>(iter->second));
                // A split apportions these across the children (tablet_splitter sets each child's
                // share from its own rowset_stats), so one occurrence carries only part of the
                // rowset and the parent's view is the sum.
                emitted->set_num_rows(emitted->num_rows() + rowset.num_rows());
                emitted->set_data_size(emitted->data_size() + rowset.data_size());
                emitted->set_num_dels(emitted->num_dels() + rowset.num_dels());
                // Emitting one occurrence assumes the siblings hold the same segments, which holds
                // because tablet_splitter does not prune this shape. If that ever changes, the rows
                // only a later sibling kept would silently vanish from the alias, so fail instead.
                if (emitted->segment_metas_size() != rowset.segment_metas_size()) {
                    return Status::Corruption(fmt::format(
                            "virtual merge: rowset {} of tablet {} carries {} segments but its sibling in tablet "
                            "{} carries {}; a pruned sibling cannot be represented by one occurrence",
                            rowset.id(), child_metadatas[child_index]->id(), rowset.segment_metas_size(),
                            child_metadatas.front()->id(), emitted->segment_metas_size()));
                }
            }
            groups[iter->second].sources.emplace_back(child_index, rowset.id());
        }
    }
    alias->set_next_rowset_id(next_id);

    // Schemas: the union of the children's, with each emitted rowset's binding re-keyed to the id it got.
    // Looked up under the child it came from, because two children's post-split rowsets can share an id
    // while pointing at different schemas.
    auto* historical = alias->mutable_historical_schemas();
    std::map<std::pair<size_t, uint32_t>, int64_t> schema_id_of_source;
    for (size_t child_index = 0; child_index < child_metadatas.size(); ++child_index) {
        for (const auto& [schema_id, schema] : child_metadatas[child_index]->historical_schemas()) {
            (*historical)[schema_id] = schema;
        }
        for (const auto& [rowset_id, schema_id] : child_metadatas[child_index]->rowset_to_schema()) {
            schema_id_of_source[{child_index, rowset_id}] = schema_id;
        }
    }
    auto* rowset_to_schema = alias->mutable_rowset_to_schema();
    for (const auto& group : groups) {
        const auto& [child_index, source_id] = group.sources.front();
        auto it = schema_id_of_source.find({child_index, source_id});
        if (it != schema_id_of_source.end()) {
            (*rowset_to_schema)[group.target_id] = it->second;
        }
    }

    // Delete vectors, keyed by rssid = rowset id + segment index. So the same segment is `source_id + i`
    // in the child and `target_id + i` in the alias, and walking i over the rowset's segments is all the
    // re-keying needs.
    //
    // Unioning the children's pages for a segment is what makes the alias the parent's view: each row of a
    // shared segment belongs to exactly one child, and only its owner deletes it. The children's pages for
    // one inherited segment are byte-identical, and unioning a bitmap with itself is that bitmap.
    // Collect the distinct pages each target rssid draws from, WITHOUT decoding any of them. Only an
    // rssid that draws from more than one has to be merged; the common one -- an inherited page no child
    // has touched since the split, or a segment only one child ever deleted from -- is copied straight
    // through, so neither its bitmap nor its serialized form is ever held in memory here. That matters
    // because this runs on the publish critical path once per version for as long as the split lives:
    // materializing every page would hold roughly the tablet's whole delete-vector set, decoded, plus
    // the serialized copy, at once.
    std::map<uint32_t, std::vector<AliasDelvecSource>> sources_of_target;
    for (size_t g = 0; g < groups.size(); ++g) {
        const auto& group = groups[g];
        // groups and the emitted rowsets are built in the same pass, so they stay index-aligned.
        const auto& emitted = alias->rowsets(static_cast<int>(g));
        // The segment indices this rowset actually holds -- NOT the whole [0, id_step) span. A rowset's
        // segment_idx values only start out contiguous: partial compaction and batch merge leave them
        // sparse, and scanning the span would then do a lookup per unused rssid in every gap, on the
        // publish critical path, once per version. A rowset carrying only del files keeps no segment
        // but still owns its own number, which is where resolve_del_op_offset lands.
        std::vector<uint32_t> segment_idxes;
        segment_idxes.reserve(emitted.segment_metas_size());
        for (int pos = 0; pos < emitted.segment_metas_size(); ++pos) {
            segment_idxes.push_back(get_segment_idx(emitted, pos));
        }
        if (segment_idxes.empty() && emitted.del_files_size() > 0) {
            segment_idxes.push_back(0);
        }
        for (const uint32_t i : segment_idxes) {
            // Right after the split every child holds the SAME page for an inherited segment -- one file,
            // one offset -- and they only diverge once each starts deleting its own rows. get_del_vec
            // caches per tablet id, so a sibling's identical page would be fetched and decoded again
            // rather than reused. Union each distinct page once; identical ones contribute the same bits
            // anyway, so skipping them changes nothing but the I/O.
            std::set<std::tuple<std::string, uint64_t, uint64_t>> unioned_pages;
            for (const auto& [child_index, source_id] : group.sources) {
                const auto& child = *child_metadatas[child_index];
                const auto& child_delvecs = child.delvec_meta().delvecs();
                auto it = child_delvecs.find(source_id + i);
                if (it == child_delvecs.end()) {
                    continue; // this child deleted nothing in that segment
                }
                const auto& page = it->second;
                const auto& files = child.delvec_meta().version_to_file();
                auto file_it = files.find(page.version());
                if (file_it == files.end()) {
                    return Status::Corruption(
                            fmt::format("virtual merge: delvec page of rssid {} names version {}, which has no "
                                        "file in tablet {}",
                                        source_id + i, page.version(), child.id()));
                }
                // Identify the page by the FILE it lives in, not by its version. A version number says
                // nothing about which bytes a page holds: one publish advances every child to the same
                // version and each child writes its OWN delvec file for it, its own page at offset 0 --
                // so two children that deleted the same number of rows produce pages agreeing on version,
                // offset and size while listing different rowids, and the second was dropped as a
                // duplicate. The name does identify the bytes, because gen_delvec_filename mints a fresh
                // uuid per write: equal names are one object, which is exactly what an inherited page is
                // in every child -- the case this dedup exists for. Same key as merge_delvecs in
                // tablet_merger.cpp, which resolves the file name from the page version the same way.
                if (!unioned_pages.emplace(file_it->second.name(), page.offset(), page.size()).second) {
                    continue; // the same physical page this rssid already took
                }
                sources_of_target[group.target_id + i].push_back(AliasDelvecSource{
                        .child_index = child_index,
                        .info = DelvecPageInfo{.tablet_id = child.id(), .delvec_file = file_it->second, .page = page}});
            }
        }
    }

    if (sources_of_target.empty()) {
        return alias;
    }

    // One file the alias owns, rather than references into the children's: it must not own a child's file,
    // and it is rewritten at every version anyway.
    std::vector<uint32_t> target_rssids;
    std::vector<DelvecPagePB> pages;
    std::vector<DelvecOutputPage> output_pages;
    target_rssids.reserve(sources_of_target.size());
    pages.reserve(sources_of_target.size());
    output_pages.reserve(sources_of_target.size());
    for (auto& [target_rssid, sources] : sources_of_target) {
        DelvecPagePB out_page;
        out_page.set_version(new_version);
        out_page.set_crc32c_gen_version(new_version);
        // A single source is copied byte for byte, through write_compacted_delvec_pages's bounded buffer.
        // Its checksum carries over with its bytes, and only when the source's own was valid: meta_file
        // reads crc32c_gen_version != version as "no checksum", so an invalid one must not be restated as
        // this version's. An encrypted file is refused as a raw page, so it takes the decode path.
        const bool copy_verbatim = sources.size() == 1 && !sources.front().info.delvec_file.name().empty() &&
                                   sources.front().info.delvec_file.encryption_meta().empty();
        if (copy_verbatim) {
            const auto& only = sources.front().info;
            out_page.set_size(only.page.size());
            if (only.page.has_crc32c() && only.page.crc32c_gen_version() == only.page.version()) {
                out_page.set_crc32c(only.page.crc32c());
            } else {
                out_page.clear_crc32c();
                out_page.clear_crc32c_gen_version();
            }
            output_pages.push_back(DelvecOutputPage{.raw_page = only});
        } else {
            DelVector merged;
            for (const auto& source : sources) {
                DelVector page_delvec;
                LakeIOOptions io_opts{.fill_data_cache = false};
                RETURN_IF_ERROR(get_del_vec(tablet_manager, *child_metadatas[source.child_index], source.info.page,
                                            false, io_opts, &page_delvec));
                if (page_delvec.empty()) {
                    continue;
                }
                merged.union_with(new_version, *page_delvec.roaring());
            }
            std::string data = merged.save();
            out_page.set_size(data.size());
            out_page.set_crc32c(crc32c::Mask(crc32c::Value(data.data(), data.size())));
            output_pages.push_back(DelvecOutputPage{.serialized_page = std::move(data)});
        }
        target_rssids.push_back(target_rssid);
        pages.push_back(std::move(out_page));
    }

    FileMetaPB delvec_file;
    std::vector<uint64_t> page_offsets;
    RETURN_IF_ERROR(write_compacted_delvec_pages(tablet_manager, output_pages, alias->id(), txn_info.txn_id(),
                                                 &delvec_file, &page_offsets));
    if (page_offsets.size() != output_pages.size()) {
        return Status::InternalError("virtual merge: delvec writer returned one offset per page mismatch");
    }

    auto* delvec_meta = alias->mutable_delvec_meta();
    (*delvec_meta->mutable_version_to_file())[new_version] = delvec_file;
    for (size_t i = 0; i < target_rssids.size(); ++i) {
        pages[i].set_offset(page_offsets[i]);
        (*delvec_meta->mutable_delvecs())[target_rssids[i]] = std::move(pages[i]);
    }

    return alias;
}

} // namespace starrocks::lake

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

#include "storage/lake/lake_primary_key_recover.h"

#include <map>
#include <memory>
#include <string>
#include <string_view>

#include "column/column.h"
#include "column/fixed_length_column.h"
#include "column/serde/column_array_serde.h"
#include "common/constexpr.h"
#include "fs/fs_util.h"
#include "platform/key_cache.h"
#include "storage/chunk_helper.h"
#include "storage/lake/del_file_with_mask_ids.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/primary_key_manager.h"
#include "storage/lake/tablet.h"
#include "storage/storage_engine.h"
#include "storage/tablet_meta_manager.h"
#include "storage_primitive/primary_key_encoder.h"

namespace starrocks::lake {

namespace {

// Corruption of del file |del| of rowset |rowset_id| in tablet |tablet_id|: |what| says what is wrong with it.
Status corrupt_del_file(int64_t tablet_id, uint32_t rowset_id, const DelfileWithRowsetId& del, std::string_view what) {
    return Status::Corruption(
            fmt::format("del file {} of rowset {} in tablet {} {}", del.name(), rowset_id, tablet_id, what));
}

// Checks del file |del|, which |rowset| wrote itself. A rowset marked position_delete holds only del files with mask
// ids. A del file with mask ids has a row mask, and one of keys alone has none. A row mask masks no more rows than its
// set has, and refers to a mask page exactly when it masks rows.
Status check_del_file(int64_t tablet_id, const RowsetMetadataPB& rowset, const DelfileWithRowsetId& del) {
    auto corrupt = [&](std::string_view what) { return corrupt_del_file(tablet_id, rowset.id(), del, what); };
    const bool has_mask_ids = del.format() == DelfileFormatPB::DEL_FILE_SEGMENT;
    if (rowset.position_delete() && !has_mask_ids) {
        return corrupt("holds keys alone, though its rowset is a position delete's");
    }
    if (del.has_row_mask() != has_mask_ids) {
        return corrupt(has_mask_ids ? "holds mask ids but has no row mask" : "has a row mask but holds keys alone");
    }
    const auto& row_mask = del.row_mask();
    if (row_mask.num_masked_rows() < 0 || row_mask.num_masked_rows() > row_mask.total_rows()) {
        return corrupt(fmt::format("masks {} of {} rows", row_mask.num_masked_rows(), row_mask.total_rows()));
    }
    if (row_mask.num_masked_rows() == 0 && row_mask.has_mask_page()) {
        return corrupt("masks no row but refers to a page");
    }
    if (row_mask.num_masked_rows() > 0 && row_mask.mask_page().size() == 0) {
        return corrupt(fmt::format("masks {} rows but refers to no page", row_mask.num_masked_rows()));
    }
    return Status::OK();
}

} // namespace

Status LakePrimaryKeyRecover::read_position_delete_mask_pages() {
    _position_delete_mask_pages.clear();
    for (const auto& rowset : _metadata->rowsets()) {
        for (const auto& del : rowset.del_files()) {
            // A del file that a compaction output carried over from one of its inputs is not replayed: pre_cleanup()
            // drops it, row mask and all.
            if (del.origin_rowset_id() != rowset.id()) {
                continue;
            }
            RETURN_IF_ERROR(check_del_file(_tablet->id(), rowset, del));
            const auto& row_mask = del.row_mask();
            if (row_mask.num_masked_rows() == 0) {
                continue;
            }
            auto corrupt = [&](std::string_view what) {
                return corrupt_del_file(_tablet->id(), rowset.id(), del, what);
            };
            const auto& page = row_mask.mask_page();
            const PositionDeleteMaskPageId id = position_delete_mask_page_id(page);
            DelVectorPtr delvec;
            if (auto read = _position_delete_mask_pages.find(id); read != _position_delete_mask_pages.end()) {
                // Row masks that refer to one page name it alike. One that names a page's place with another size or
                // checksum refers to something that is not there.
                const auto& known = read->second.page;
                if (page.size() != known.size() || page.crc32c() != known.crc32c()) {
                    return corrupt(fmt::format(
                            "refers to the page at version {} offset {} with size {} and checksum {}, which another "
                            "row mask names with size {} and checksum {}",
                            page.version(), page.offset(), page.size(), page.crc32c(), known.size(), known.crc32c()));
                }
                delvec = read->second.delvec;
            } else {
                // Called before pre_cleanup() clears the delete vector metadata and resets the builder, so the builder
                // still finds the files, and the pages this publish appended.
                ASSIGN_OR_RETURN(delvec, _builder->read_position_delete_mask_page(page));
            }
            if (static_cast<int64_t>(delvec->cardinality()) != row_mask.num_masked_rows()) {
                return corrupt(fmt::format("masks {} rows but its page holds {}", row_mask.num_masked_rows(),
                                           delvec->cardinality()));
            }
            _position_delete_mask_pages.emplace(id, PositionDeleteMaskPage{page, std::move(delvec)});
        }
    }
    return Status::OK();
}

Status LakePrimaryKeyRecover::pre_cleanup() {
    // 0. read the mask pages of position deletes, while the metadata still says which file holds each version, and the
    // builder still holds the pages this publish appended
    RETURN_IF_ERROR(read_position_delete_mask_pages());
    // 1. reset delvec & sst in metadata and clean delvec in builder.
    // Nothing refers to the dropped delvec files and sstables any more, hand them to vacuum.
    for (auto& [version, file] : *_metadata->mutable_delvec_meta()->mutable_version_to_file()) {
        file.set_version(version);
        _metadata->mutable_orphan_files()->Add(std::move(file));
    }
    for (const auto& sst : _metadata->sstable_meta().sstables()) {
        FileMetaPB file_meta;
        file_meta.set_name(sst.filename());
        file_meta.set_size(sst.filesize());
        file_meta.set_shared(sst.shared());
        file_meta.set_version(sst.generation_version());
        _metadata->mutable_orphan_files()->Add(std::move(file_meta));
    }
    _metadata->clear_delvec_meta();
    _metadata->clear_sstable_meta();
    // A del file a compaction output carried over from one of its inputs is dropped too. Recover does not replay
    // it, and the index rebuild that follows must not either, or the index would erase rows the rebuilt delvecs
    // keep live. Its deleted rows may come back, which recover allows.
    for (auto& rowset : *_metadata->mutable_rowsets()) {
        auto* dels = rowset.mutable_del_files();
        auto inherited = std::stable_partition(dels->begin(), dels->end(), [&](const DelfileWithRowsetId& del) {
            return del.origin_rowset_id() == rowset.id();
        });
        for (auto it = inherited; it != dels->end(); ++it) {
            FileMetaPB file_meta;
            file_meta.set_name(it->name());
            file_meta.set_shared(it->shared());
            file_meta.set_version(it->version());
            _metadata->mutable_orphan_files()->Add(std::move(file_meta));
        }
        dels->erase(inherited, dels->end());
    }
    // A compaction without output segments keeps an output rowset only to carry those del files; with them gone
    // it holds nothing, and an empty ordinary rowset is not a valid shape, so drop it.
    auto* rowsets = _metadata->mutable_rowsets();
    auto emptied = std::stable_partition(rowsets->begin(), rowsets->end(), [](const RowsetMetadataPB& rowset) {
        return rowset.has_delete_predicate() || rowset.segment_metas_size() > 0 || rowset.del_files_size() > 0;
    });
    for (auto it = emptied; it != rowsets->end(); ++it) {
        _metadata->mutable_rowset_to_schema()->erase(it->id());
    }
    rowsets->erase(emptied, rowsets->end());
    // The builder may still hold delvecs appended before recover, and must not let a segment that the
    // rebuild finds no deletes for fall back to the base version's delvec. The mask pages it holds go too: step 0 read
    // those that row masks refer to.
    _builder->reset_delvecs_for_recover();
    // 2. reset primary index
    RETURN_ERROR_IF_FALSE(_tablet->primary_key_mgr()->try_remove_primary_index_cache(_tablet->id()));
    DataDir* data_dir = StorageEngine::instance()->get_persistent_index_store(_tablet->id());
    if (data_dir != nullptr) {
        // clear local persistent index's index meta from rocksdb, and index files.
        RETURN_IF_ERROR(TabletMetaManager::remove_tablet_persistent_index_meta(data_dir, _tablet->id()));
        std::string tablet_pk_path =
                strings::Substitute("$0/$1/", data_dir->get_persistent_index_path(), _tablet->id());
        RETURN_IF_ERROR(fs::remove_all(tablet_pk_path));
    }
    return Status::OK();
}

// Primary key schema
starrocks::Schema LakePrimaryKeyRecover::generate_pkey_schema() {
    std::shared_ptr<TabletSchema> tablet_schema = std::make_shared<TabletSchema>(_metadata->schema());
    std::vector<ColumnId> pk_columns(tablet_schema->num_key_columns());
    for (auto i = 0; i < tablet_schema->num_key_columns(); i++) {
        pk_columns[i] = (ColumnId)i;
    }
    return ChunkHelper::convert_schema(tablet_schema, pk_columns);
}

Status LakePrimaryKeyRecover::sort_rowsets(std::vector<RowsetPtr>* rowsets) {
    auto replay_key = [](const RowsetPtr& rowset) {
        const RowsetMetadataPB& meta = rowset->metadata();
        const uint32_t place = meta.has_max_compact_input_rowset_id() ? meta.max_compact_input_rowset_id() : meta.id();
        return std::make_pair(place, meta.id());
    };
    std::sort(rowsets->begin(), rowsets->end(),
              [&](const RowsetPtr& a, const RowsetPtr& b) { return replay_key(a) < replay_key(b); });
    return Status::OK();
}

Status LakePrimaryKeyRecover::rowset_iterator(
        const starrocks::Schema& pkey_schema, OlapReaderStatistics& stats,
        const std::function<Status(const std::vector<ChunkIteratorPtr>&, const std::vector<DelKeysReader>&,
                                   const std::vector<uint32_t>&, uint32_t)>& handler) {
    auto rowsets = _tablet->get_rowsets(_metadata);
    // Sort the rowsets in order of primary key occurrence,
    // so we can generate correct delvecs
    RETURN_IF_ERROR(sort_rowsets(&rowsets));
    for (auto& rowset : rowsets) {
        auto res = rowset->get_each_segment_iterator(pkey_schema, false, &stats);
        if (!res.ok()) {
            return res.status();
        }
        // iterator for each segment
        auto& itrs = res.value();
        // reader for each del file
        std::vector<DelKeysReader> del_keys_readers;
        for (int del_idx = 0; del_idx < rowset->metadata().del_files_size(); ++del_idx) {
            const auto& del = rowset->metadata().del_files(del_idx);
            if (del.origin_rowset_id() != rowset->id()) {
                // if del file is not origin from this rowset, skip it
                // because that means this rowset is generated by compaction
                break;
            }
            del_keys_readers.emplace_back([this, del](Column* keys) { return read_keys_to_delete(del, keys); });
        }
        // Position of each del file in the merged (segments + dels) replay sequence consumed by the
        // handler. A del file logically follows segment index op_offset, so the segments preceding it
        // are this rowset's iterators with segment index <= op_offset (segs_before, counted below) and
        // the del files preceding it are the j already collected; hence its merged position is
        // segs_before + j. This interleaves deletes at the same point the apply path does, so recover
        // reproduces the same delvec. del_keys_readers only reads origin del files (the loop above breaks on
        // non-origin), so op_offset is in this rowset's segment-index space. Reduces to the legacy
        // "after all segments" when op_offset is the max segment index.
        std::vector<uint32_t> delidxs;
        delidxs.reserve(del_keys_readers.size());
        for (uint32_t j = 0; j < del_keys_readers.size(); ++j) {
            const auto& del = rowset->metadata().del_files(j);
            // Fall back to the max segment id when op_offset was not recorded (older metadata): the
            // delete then sorts after all segments (num_segments + j), i.e. the legacy recover
            // behavior. Matches the apply/persist fallback in resolve_del_op_offset().
            const uint32_t op_offset = del.has_op_offset() ? del.op_offset() : get_max_segment_idx(rowset->metadata());
            // Segments preceding this del in the replay = this rowset's segment iterators whose segment
            // index is <= op_offset. Counting them (rather than using op_offset + 1) stays correct when
            // segment_idx is no longer contiguous -- e.g. after tablet split/pruning a rowset can retain
            // sparse segment indices, where op_offset + 1 would over- or under-count and replay the
            // delete at the wrong point (erasing a row live publish kept, or vice versa). It also covers
            // a segmentless delete-only rowset (count == 0, so the del lands at a position the handler
            // reaches) and reduces to op_offset + 1 for the common contiguous case. itrs[k] corresponds
            // to the k-th segment_meta, whose index is get_segment_idx(metadata, k).
            uint32_t segs_before = 0;
            for (uint32_t k = 0; k < itrs.size(); ++k) {
                if (get_segment_idx(rowset->metadata(), static_cast<int32_t>(k)) <= op_offset) {
                    ++segs_before;
                }
            }
            delidxs.push_back(segs_before + j);
        }
        RETURN_IF_ERROR(handler(itrs, del_keys_readers, delidxs, rowset->id()));
    }
    return Status::OK();
}

Status LakePrimaryKeyRecover::read_keys_to_delete(const DelfileWithRowsetId& del, Column* keys) {
    if (del.format() == DelfileFormatPB::DEL_FILE_SEGMENT) {
        // A position delete's del file deletes the keys of its live candidate rows: the entries its mask page does not
        // mask.
        const auto& row_mask = del.row_mask();
        const roaring::Roaring* stale_mask_ids = nullptr;
        if (row_mask.num_masked_rows() > 0) {
            const auto id = position_delete_mask_page_id(row_mask.mask_page());
            stale_mask_ids = _position_delete_mask_pages.at(id).delvec->roaring();
        }
        return read_live_row_keys(_tablet->tablet_mgr(), _tablet->id(), del, row_mask.total_rows(), stale_mask_ids,
                                  keys);
    }
    RandomAccessFileOptions ropts;
    if (!del.encryption_meta().empty()) {
        ASSIGN_OR_RETURN(ropts.encryption_info, KeyCache::instance().unwrap_encryption_meta(del.encryption_meta()));
    }
    ASSIGN_OR_RETURN(auto read_file, fs::new_random_access_file(
                                             ropts, _tablet->tablet_mgr()->del_location(_metadata->id(), del.name())));
    // Recovery rebuilds every delvec from these deletes, so a corrupt del file that still deserializes would bake the
    // wrong rows into the recovered delvecs -- permanently, since recovery treats the result as the new source of
    // truth. So the checksum is verified whenever one was recorded.
    ASSIGN_OR_RETURN(auto content, read_and_verify_del_file(read_file.get(), del, _metadata->id()));
    const auto* begin = reinterpret_cast<const uint8_t*>(content.data());
    return serde::ColumnArraySerde::deserialize(begin, begin + content.size(), keys).status();
}

// generate delvec and save
Status LakePrimaryKeyRecover::finalize_delvec(const PrimaryIndex::DeletesMap& new_deletes) {
    // num_dels must follow the rebuilt delvecs, not the ones recover discarded.
    std::unordered_map<uint32_t, RowsetMetadataPB*> rssid_to_rowset;
    for (int i = 0; i < _metadata->rowsets_size(); i++) {
        auto* rowset = _metadata->mutable_rowsets(i);
        rowset->set_num_dels(0);
        for (int j = 0; j < rowset->segment_metas_size(); j++) {
            rssid_to_rowset[get_rssid(*rowset, j)] = rowset;
        }
    }
    for (auto& new_delete : new_deletes) {
        auto delvec_ptr = std::make_shared<DelVector>();
        auto& del_ids = new_delete.second;
        delvec_ptr->init(_metadata->version(), del_ids.data(), del_ids.size());
        _builder->append_delvec(delvec_ptr, new_delete.first);
        auto it = rssid_to_rowset.find(new_delete.first);
        if (it != rssid_to_rowset.end()) {
            it->second->set_num_dels(it->second->num_dels() + delvec_ptr->cardinality());
        }
    }
    // pre_cleanup() dropped the record of which file holds each version, and the pages this publish appended, so the
    // pages row masks refer to go into this publish's delete vector file anew, and the row masks are pointed at them
    // there.
    std::map<PositionDeleteMaskPageId, DelvecPagePB> rewritten;
    for (const auto& [id, read] : _position_delete_mask_pages) {
        rewritten.emplace(id, _builder->append_position_delete_mask_page(read.delvec));
    }
    for (auto& rowset : *_metadata->mutable_rowsets()) {
        for (auto& del : *rowset.mutable_del_files()) {
            if (!del.row_mask().has_mask_page()) {
                continue;
            }
            const auto& page = del.row_mask().mask_page();
            auto iter = rewritten.find(position_delete_mask_page_id(page));
            if (iter == rewritten.end()) {
                return Status::InternalError(fmt::format(
                        "page of the row mask of del file {} in tablet {} was not read", del.name(), _tablet->id()));
            }
            *del.mutable_row_mask()->mutable_mask_page() = iter->second;
        }
    }
    return Status::OK();
}

int64_t LakePrimaryKeyRecover::tablet_id() {
    return _tablet->id();
}

StatusOr<PrimaryKeyEncodingType> LakePrimaryKeyRecover::primary_key_encoding_type() const {
    auto tablet_schema = std::make_unique<TabletSchema>(_metadata->schema());
    return tablet_schema->primary_key_encoding_type_or_error();
}

} // namespace starrocks::lake
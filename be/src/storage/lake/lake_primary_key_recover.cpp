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

#include "column/column.h"
#include "common/constexpr.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "storage/chunk_helper.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet.h"
#include "storage/lake/update_manager.h"
#include "storage/primary_key_encoder.h"
#include "storage/storage_engine.h"
#include "storage/tablet_meta_manager.h"

namespace starrocks::lake {

Status LakePrimaryKeyRecover::pre_cleanup() {
    // 1. reset delvec & sst in metadata and clean delvec in builder.
    // Nothing refers to the dropped delvec files and sstables any more, hand them to vacuum.
    for (auto& item : *_metadata->mutable_delvec_meta()->mutable_version_to_file()) {
        _metadata->mutable_orphan_files()->Add(std::move(item.second));
    }
    for (const auto& sst : _metadata->sstable_meta().sstables()) {
        FileMetaPB file_meta;
        file_meta.set_name(sst.filename());
        file_meta.set_size(sst.filesize());
        file_meta.set_shared(sst.shared());
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
            _metadata->mutable_orphan_files()->Add(std::move(file_meta));
        }
        dels->erase(inherited, dels->end());
    }
    // A compaction without output segments keeps an output rowset only to carry those del files; with them gone
    // it holds nothing, and an empty ordinary rowset is not a valid shape, so drop it.
    auto* rowsets = _metadata->mutable_rowsets();
    auto emptied = std::stable_partition(rowsets->begin(), rowsets->end(), [](const RowsetMetadataPB& rowset) {
        return rowset.has_delete_predicate() || rowset.segments_size() > 0 || rowset.del_files_size() > 0;
    });
    for (auto it = emptied; it != rowsets->end(); ++it) {
        _metadata->mutable_rowset_to_schema()->erase(it->id());
    }
    rowsets->erase(emptied, rowsets->end());
    // The builder may still hold delvecs appended before recover, and must not let a segment that the
    // rebuild finds no deletes for fall back to the base version's delvec.
    _builder->reset_delvecs_for_recover();
    // 2. reset primary index
    RETURN_ERROR_IF_FALSE(_tablet->update_mgr()->try_remove_primary_index_cache(_tablet->id()));
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
    std::sort(rowsets->begin(), rowsets->end(), [](const RowsetPtr& a, const RowsetPtr& b) {
        const RowsetMetadataPB& rowset_meta_a = a->metadata();
        const RowsetMetadataPB& rowset_meta_b = b->metadata();
        // if rowset was generated by compaction, use max compact input rowset id as its compare id
        uint32_t rowset_a_comp_id = rowset_meta_a.has_max_compact_input_rowset_id()
                                            ? rowset_meta_a.max_compact_input_rowset_id()
                                            : rowset_meta_a.id();
        uint32_t rowset_b_comp_id = rowset_meta_b.has_max_compact_input_rowset_id()
                                            ? rowset_meta_b.max_compact_input_rowset_id()
                                            : rowset_meta_b.id();
        return rowset_a_comp_id < rowset_b_comp_id;
    });
    return Status::OK();
}

Status LakePrimaryKeyRecover::rowset_iterator(
        const starrocks::Schema& pkey_schema, OlapReaderStatistics& stats,
        const std::function<Status(const std::vector<ChunkIteratorPtr>&,
                                   const std::vector<std::unique_ptr<RandomAccessFile>>&, const std::vector<uint32_t>&,
                                   uint32_t)>& handler) {
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
        // iterator for each del files
        std::vector<std::unique_ptr<RandomAccessFile>> del_rfs;
        for (int del_idx = 0; del_idx < rowset->metadata().del_files_size(); ++del_idx) {
            const auto& del = rowset->metadata().del_files(del_idx);
            if (del.origin_rowset_id() != rowset->id()) {
                // if del file is not origin from this rowset, skip it
                // because that means this rowset is generated by compaction
                break;
            }
            RandomAccessFileOptions ropts;
            if (!del.encryption_meta().empty()) {
                ASSIGN_OR_RETURN(ropts.encryption_info,
                                 KeyCache::instance().unwrap_encryption_meta(del.encryption_meta()));
            }
            ASSIGN_OR_RETURN(auto read_file, fs::new_random_access_file(ropts, _tablet->tablet_mgr()->del_location(
                                                                                       _metadata->id(), del.name())));
            del_rfs.push_back(std::move(read_file));
        }
        // position of del files in itrs
        std::vector<uint32_t> delidxs;
        for (uint32_t i = itrs.size(); i < itrs.size() + del_rfs.size(); ++i) {
            delidxs.push_back(i);
        }
        RETURN_IF_ERROR(handler(itrs, del_rfs, delidxs, rowset->id()));
    }
    return Status::OK();
}

// generate delvec and save
Status LakePrimaryKeyRecover::finalize_delvec(const PrimaryIndex::DeletesMap& new_deletes) {
    // num_dels must follow the rebuilt delvecs, not the ones recover discarded.
    std::unordered_map<uint32_t, RowsetMetadataPB*> rssid_to_rowset;
    for (int i = 0; i < _metadata->rowsets_size(); i++) {
        auto* rowset = _metadata->mutable_rowsets(i);
        rowset->set_num_dels(0);
        for (int j = 0; j < rowset->segments_size(); j++) {
            rssid_to_rowset[rowset->id() + j] = rowset;
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
    return Status::OK();
}

int64_t LakePrimaryKeyRecover::tablet_id() {
    return _tablet->id();
}

} // namespace starrocks::lake
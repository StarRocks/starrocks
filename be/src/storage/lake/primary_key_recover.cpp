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

#include "storage/lake/primary_key_recover.h"

#include "column/column.h"
#include "common/constexpr.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet.h"
#include "storage/lake/update_manager.h"
#include "storage/primary_key_encoder.h"
#include "storage/storage_engine.h"
#include "storage/tablet_meta_manager.h"

namespace starrocks::lake {

Status PrimaryKeyRecover::pre_cleanup() {
    // 1. reset delvec in metadata and clean delvec in builder
    // TODO reclaim delvec files
    _metadata->clear_delvec_meta();
    // 2. reset primary index
    _tablet->update_mgr()->remove_primary_index_cache(_tablet->id());
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

Status PrimaryKeyRecover::_init_schema(const TabletMetadata& metadata) {
    std::shared_ptr<TabletSchema> tablet_schema = std::make_shared<TabletSchema>(metadata.schema());
    std::vector<ColumnId> pk_columns(tablet_schema->num_key_columns());
    for (auto i = 0; i < tablet_schema->num_key_columns(); i++) {
        pk_columns[i] = (ColumnId)i;
    }
    _pkey_schema = ChunkHelper::convert_schema(*tablet_schema, pk_columns);

    if (pk_columns.size() > 1) {
        // more than one key column
        if (!PrimaryKeyEncoder::create_column(_pkey_schema, &_pk_column).ok()) {
            CHECK(false) << "create column for primary key encoder failed";
        }
    }
    return Status::OK();
}

Status PrimaryKeyRecover::recover() {
    // 1. build schema
    RETURN_IF_ERROR(_init_schema(*_metadata));
    // For simplicity, we use temp pk index for generate delvec, and then rebuild real pk index when retry publish
    LakePrimaryIndex index(_pkey_schema);

    OlapReaderStatistics stats;
    std::vector<uint32_t> rowids;
    rowids.reserve(DEFAULT_CHUNK_SIZE);
    PrimaryIndex::DeletesMap new_deletes;
    auto chunk_shared_ptr = ChunkHelper::new_chunk(_pkey_schema, DEFAULT_CHUNK_SIZE);
    auto chunk = chunk_shared_ptr.get();
    // 2. scan all rowsets and segments to build primary index
    ASSIGN_OR_RETURN(auto rowsets, _tablet->get_rowsets(*_metadata));
    // 3. rebuild primary key index and generate delete maps
    for (auto& rowset : rowsets) {
        auto res = rowset->get_each_segment_iterator(_pkey_schema, &stats);
        if (!res.ok()) {
            return res.status();
        }
        auto& itrs = res.value();
        CHECK(itrs.size() == rowset->num_segments()) << "itrs.size != num_segments";
        for (size_t i = 0; i < itrs.size(); i++) {
            auto itr = itrs[i].get();
            if (itr == nullptr) {
                continue;
            }
            uint32_t row_id_start = 0;
            while (true) {
                chunk->reset();
                rowids.clear();
                auto st = itr->get_next(chunk, &rowids);
                if (st.is_end_of_file()) {
                    break;
                } else if (!st.ok()) {
                    return st;
                } else {
                    Column* pkc = nullptr;
                    if (_pk_column) {
                        _pk_column->reset_column();
                        PrimaryKeyEncoder::encode(_pkey_schema, *chunk, 0, chunk->num_rows(), _pk_column.get());
                        pkc = _pk_column.get();
                    } else {
                        pkc = chunk->columns()[0].get();
                    }
                    // upsert and generate new deletes
                    RETURN_IF_ERROR(index.upsert(rowset->id() + i, row_id_start, *pkc, &new_deletes));
                    row_id_start += pkc->size();
                }
            }
            itr->close();
        }
    }
    // 4. generate delvec and add them to builder
    for (auto& new_delete : new_deletes) {
        auto delvec_ptr = std::make_shared<DelVector>();
        auto& del_ids = new_delete.second;
        delvec_ptr->init(_metadata->version(), del_ids.data(), del_ids.size());
        _builder->append_delvec(delvec_ptr, new_delete.first);
    }

    return Status::OK();
}

} // namespace starrocks::lake
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

#include "storage/lake/lake_primary_index.h"

#include <bvar/bvar.h>

#include "storage/chunk_helper.h"
#include "storage/lake/lake_local_persistent_index.h"
#include "storage/lake/lake_persistent_index.h"
#include "storage/lake/local_pk_index_manager.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet.h"
#include "storage/primary_key_encoder.h"
#include "storage/tablet_meta_manager.h"
#include "testutil/sync_point.h"
#include "util/trace.h"

namespace starrocks::lake {

static bvar::LatencyRecorder g_load_pk_index_latency("lake_load_pk_index");

LakePrimaryIndex::~LakePrimaryIndex() {
    if (!_enable_persistent_index && _persistent_index != nullptr) {
        auto st = LocalPkIndexManager::clear_persistent_index(_tablet_id);
        LOG_IF(WARNING, !st.ok()) << "Fail to clear pk index from local disk: " << st.to_string();
    }
}

Status LakePrimaryIndex::lake_load(TabletManager* tablet_mgr, const TabletMetadataPtr& metadata, int64_t base_version,
                                   const MetaFileBuilder* builder) {
    TRACE_COUNTER_SCOPE_LATENCY_US("primary_index_load_latency_us");
    std::lock_guard<std::mutex> lg(_lock);
    if (_loaded) {
        return _status;
    }
    _status = _do_lake_load(tablet_mgr, metadata, base_version, builder);
    TEST_SYNC_POINT_CALLBACK("lake_index_load.1", &_status);
    if (_status.ok()) {
        // update data version when memory index or persistent index load finish.
        _data_version = base_version;
    }
    _tablet_id = metadata->id();
    _loaded = true;
    TRACE("end load pk index");
    if (!_status.ok()) {
        LOG(WARNING) << "load LakePrimaryIndex error: " << _status << " tablet:" << _tablet_id;
    }
    return _status;
}

bool LakePrimaryIndex::is_load(int64_t base_version) {
    std::lock_guard<std::mutex> lg(_lock);
    return _loaded && _data_version >= base_version;
}

Status LakePrimaryIndex::_do_lake_load(TabletManager* tablet_mgr, const TabletMetadataPtr& metadata,
                                       int64_t base_version, const MetaFileBuilder* builder) {
    MonotonicStopWatch watch;
    watch.start();
    // 1. create and set key column schema
    std::shared_ptr<TabletSchema> tablet_schema = std::make_shared<TabletSchema>(metadata->schema());
    vector<ColumnId> pk_columns(tablet_schema->num_key_columns());
    for (auto i = 0; i < tablet_schema->num_key_columns(); i++) {
        pk_columns[i] = (ColumnId)i;
    }
    auto pkey_schema = ChunkHelper::convert_schema(tablet_schema, pk_columns);
    _set_schema(pkey_schema);

    // load persistent index if enable persistent index meta
    size_t fix_size = PrimaryKeyEncoder::get_encoded_fixed_size(pkey_schema);
    if (metadata->enable_persistent_index() && (fix_size <= 128)) {
        DCHECK(_persistent_index == nullptr);

        switch (metadata->persistent_index_type()) {
        case PersistentIndexTypePB::LOCAL: {
            // Even if `enable_persistent_index` is enabled,
            // it may not take effect if is as compute node without any storage path.
            if (StorageEngine::instance()->get_persistent_index_store(metadata->id()) == nullptr) {
                LOG(WARNING) << "lake_persistent_index_type of LOCAL will not take effect when as cn without any "
                                "storage path";
                return Status::InternalError(
                        "lake_persistent_index_type of LOCAL will not take effect when as cn without any storage "
                        "path");
            }
            std::string path = strings::Substitute(
                    "$0/$1/",
                    StorageEngine::instance()->get_persistent_index_store(metadata->id())->get_persistent_index_path(),
                    metadata->id());

            RETURN_IF_ERROR(StorageEngine::instance()
                                    ->get_persistent_index_store(metadata->id())
                                    ->create_dir_if_path_not_exists(path));
            _persistent_index = std::make_shared<LakeLocalPersistentIndex>(path);
            set_enable_persistent_index(true);
            return dynamic_cast<LakeLocalPersistentIndex*>(_persistent_index.get())
                    ->load_from_lake_tablet(tablet_mgr, metadata, base_version, builder);
        }
        case PersistentIndexTypePB::CLOUD_NATIVE: {
            _persistent_index = std::make_shared<LakePersistentIndex>(tablet_mgr, metadata->id());
            set_enable_persistent_index(true);
            auto* lake_persistent_index = dynamic_cast<LakePersistentIndex*>(_persistent_index.get());
            RETURN_IF_ERROR(lake_persistent_index->init(metadata->sstable_meta()));
            return lake_persistent_index->load_from_lake_tablet(tablet_mgr, metadata, base_version, builder);
        }
        default:
            return Status::InternalError("Unsupported lake_persistent_index_type " +
                                         PersistentIndexTypePB_Name(metadata->persistent_index_type()));
        }
    }

    OlapReaderStatistics stats;
    std::unique_ptr<Column> pk_column;
    if (pk_columns.size() > 1) {
        // more than one key column
        RETURN_IF_ERROR(PrimaryKeyEncoder::create_column(pkey_schema, &pk_column));
    }
    vector<uint32_t> rowids;
    rowids.reserve(4096);
    auto chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, 4096);
    auto chunk = chunk_shared_ptr.get();
    // 2. scan all rowsets and segments to build primary index
    auto rowsets = Rowset::get_rowsets(tablet_mgr, metadata);
    // NOTICE: primary index will be builded by segment files in metadata, and delvecs.
    // The delvecs we need are stored in delvec file by base_version and current MetaFileBuilder's cache.
    for (auto& rowset : rowsets) {
        auto res = rowset->get_each_segment_iterator_with_delvec(pkey_schema, base_version, builder, &stats);
        if (!res.ok()) {
            return res.status();
        }
        auto& itrs = res.value();
        RETURN_ERROR_IF_FALSE(itrs.size() == rowset->num_segments(), "itrs.size != num_segments");
        for (size_t i = 0; i < itrs.size(); i++) {
            auto itr = itrs[i].get();
            if (itr == nullptr) {
                continue;
            }
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
                    if (pk_column) {
                        pk_column->reset_column();
                        PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, chunk->num_rows(), pk_column.get());
                        pkc = pk_column.get();
                    } else {
                        pkc = chunk->columns()[0].get();
                    }
                    RETURN_IF_ERROR(insert(rowset->id() + i, rowids, *pkc));
                }
            }
            itr->close();
        }
    }
    auto cost_ns = watch.elapsed_time();
    g_load_pk_index_latency << cost_ns / 1000;
    LOG_IF(INFO, cost_ns >= /*10ms=*/10 * 1000 * 1000)
            << "LakePrimaryIndex load cost(ms): " << watch.elapsed_time() / 1000000;
    return Status::OK();
}

Status LakePrimaryIndex::apply_opcompaction(const TabletMetadata& metadata,
                                            const TxnLogPB_OpCompaction& op_compaction) {
    if (!_enable_persistent_index) {
        return Status::OK();
    }

    switch (metadata.persistent_index_type()) {
    case PersistentIndexTypePB::LOCAL: {
        return Status::OK();
    }
    case PersistentIndexTypePB::CLOUD_NATIVE: {
        auto* lake_persistent_index = dynamic_cast<LakePersistentIndex*>(_persistent_index.get());
        if (lake_persistent_index != nullptr) {
            return lake_persistent_index->apply_opcompaction(op_compaction);
        } else {
            return Status::InternalError("Persistent index is not a LakePersistentIndex.");
        }
    }
    default:
        return Status::InternalError("Unsupported lake_persistent_index_type " +
                                     PersistentIndexTypePB_Name(metadata.persistent_index_type()));
    }
    return Status::OK();
}

Status LakePrimaryIndex::commit(const TabletMetadataPtr& metadata, MetaFileBuilder* builder) {
    TRACE_COUNTER_SCOPE_LATENCY_US("primary_index_commit_latency_us");
    if (!_enable_persistent_index) {
        return Status::OK();
    }

    switch (metadata->persistent_index_type()) {
    case PersistentIndexTypePB::LOCAL: {
        // only take affect in local persistent index
        PersistentIndexMetaPB index_meta;
        DataDir* data_dir = StorageEngine::instance()->get_persistent_index_store(_tablet_id);
        RETURN_IF_ERROR(TabletMetaManager::get_persistent_index_meta(data_dir, _tablet_id, &index_meta));
        RETURN_IF_ERROR(PrimaryIndex::commit(&index_meta));
        RETURN_IF_ERROR(TabletMetaManager::write_persistent_index_meta(data_dir, _tablet_id, index_meta));
        RETURN_IF_ERROR(on_commited());
        set_local_pk_index_write_amp_score(PersistentIndex::major_compaction_score(index_meta));
        // Call `on_commited` here, which will be safe to remove old files.
        // Because if version publishing fails after `on_commited`, index will be rebuild.
        return Status::OK();
    }
    case PersistentIndexTypePB::CLOUD_NATIVE: {
        auto* lake_persistent_index = dynamic_cast<LakePersistentIndex*>(_persistent_index.get());
        if (lake_persistent_index != nullptr) {
            return lake_persistent_index->commit(builder);
        } else {
            return Status::InternalError("Persistent index is not a LakePersistentIndex.");
        }
    }
    default:
        return Status::InternalError("Unsupported lake_persistent_index_type " +
                                     PersistentIndexTypePB_Name(metadata->persistent_index_type()));
    }
    return Status::OK();
}

double LakePrimaryIndex::get_local_pk_index_write_amp_score() {
    if (!_enable_persistent_index) {
        return 0.0;
    }
    auto* local_persistent_index = dynamic_cast<LakeLocalPersistentIndex*>(_persistent_index.get());
    if (local_persistent_index != nullptr) {
        return local_persistent_index->get_write_amp_score();
    }
    return 0.0;
}

void LakePrimaryIndex::set_local_pk_index_write_amp_score(double score) {
    if (!_enable_persistent_index) {
        return;
    }
    auto* local_persistent_index = dynamic_cast<LakeLocalPersistentIndex*>(_persistent_index.get());
    if (local_persistent_index != nullptr) {
        local_persistent_index->set_write_amp_score(score);
    }
}

static void old_values_to_deletes(const std::vector<uint64_t>& old_values, DeletesMap* deletes) {
    for (uint64_t old : old_values) {
        if (old != NullIndexValue) {
            (*deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & ROWID_MASK));
        }
    }
}

Status LakePrimaryIndex::erase(const TabletMetadataPtr& metadata, const Column& pks, DeletesMap* deletes,
                               uint32_t rowset_id) {
    // No need to setup rebuild point for in-memory index and local persistent index,
    // so keep using previous erase interface.
    if (!_enable_persistent_index) {
        return PrimaryIndex::erase(pks, deletes);
    }

    switch (metadata->persistent_index_type()) {
    case PersistentIndexTypePB::LOCAL: {
        return PrimaryIndex::erase(pks, deletes);
    }
    case PersistentIndexTypePB::CLOUD_NATIVE: {
        auto* lake_persistent_index = dynamic_cast<LakePersistentIndex*>(_persistent_index.get());
        if (lake_persistent_index != nullptr) {
            std::vector<Slice> keys;
            std::vector<uint64_t> old_values(pks.size(), NullIndexValue);
            const Slice* vkeys = _build_persistent_keys(pks, 0, pks.size(), &keys);
            // Cloud native index need to setup rowset id as rebuild point when erase.
            RETURN_IF_ERROR(lake_persistent_index->erase(pks.size(), vkeys,
                                                         reinterpret_cast<IndexValue*>(old_values.data()), rowset_id));
            old_values_to_deletes(old_values, deletes);
            return Status::OK();
        } else {
            return Status::InternalError("Persistent index is not a LakePersistentIndex.");
        }
    }
    default:
        return Status::InternalError("Unsupported lake_persistent_index_type " +
                                     PersistentIndexTypePB_Name(metadata->persistent_index_type()));
    }
}

} // namespace starrocks::lake

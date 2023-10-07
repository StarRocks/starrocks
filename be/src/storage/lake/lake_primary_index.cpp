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
#include "storage/lake/rowset.h"
#include "storage/lake/tablet.h"
#include "storage/primary_key_encoder.h"
#include "testutil/sync_point.h"
#include "util/trace.h"

namespace starrocks::lake {

static bvar::LatencyRecorder g_load_pk_index_latency("lake_load_pk_index");

Status LakePrimaryIndex::lake_load(Tablet* tablet, const TabletMetadata& metadata, int64_t base_version,
                                   const MetaFileBuilder* builder) {
    TRACE_COUNTER_SCOPE_LATENCY_US("primary_index_load_latency_us");
    std::lock_guard<std::mutex> lg(_lock);
    if (_loaded) {
        return _status;
    }
    _status = _do_lake_load(tablet, metadata, base_version, builder);
    TEST_SYNC_POINT_CALLBACK("lake_index_load.1", &_status);
    if (_status.ok()) {
        // update data version when memory index or persistent index load finish.
        _data_version = base_version;
    }
    _tablet_id = tablet->id();
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

Status LakePrimaryIndex::_do_lake_load(Tablet* tablet, const TabletMetadata& metadata, int64_t base_version,
                                       const MetaFileBuilder* builder) {
    MonotonicStopWatch watch;
    watch.start();
    // 1. create and set key column schema
    std::unique_ptr<TabletSchema> tablet_schema = std::make_unique<TabletSchema>(metadata.schema());
    vector<ColumnId> pk_columns(tablet_schema->num_key_columns());
    for (auto i = 0; i < tablet_schema->num_key_columns(); i++) {
        pk_columns[i] = (ColumnId)i;
    }
    auto pkey_schema = ChunkHelper::convert_schema(*tablet_schema, pk_columns);
    _set_schema(pkey_schema);

    // load persistent index if enable persistent index meta
    size_t fix_size = PrimaryKeyEncoder::get_encoded_fixed_size(pkey_schema);

    if (tablet->get_enable_persistent_index(base_version) && (fix_size <= 128)) {
        DCHECK(_persistent_index == nullptr);

        auto persistent_index_type = tablet->get_persistent_index_type(base_version);
        if (persistent_index_type.ok()) {
            switch (persistent_index_type.value()) {
            case PersistentIndexTypePB::LOCAL: {
                // Even if `enable_persistent_index` is enabled,
                // it may not take effect if is as compute node without any storage path.
                if (StorageEngine::instance()->get_persistent_index_store(tablet->id()) == nullptr) {
                    LOG(WARNING) << "lake_persistent_index_type of LOCAL will not take effect when as cn without any "
                                    "storage path";
                    return Status::InternalError(
                            "lake_persistent_index_type of LOCAL will not take effect when as cn without any storage "
                            "path");
                }
                std::string path = strings::Substitute("$0/$1/",
                                                       StorageEngine::instance()
                                                               ->get_persistent_index_store(tablet->id())
                                                               ->get_persistent_index_path(),
                                                       tablet->id());

                RETURN_IF_ERROR(StorageEngine::instance()
                                        ->get_persistent_index_store(tablet->id())
                                        ->create_dir_if_path_not_exists(path));
                _persistent_index = std::make_unique<LakeLocalPersistentIndex>(path);
                return dynamic_cast<LakeLocalPersistentIndex*>(_persistent_index.get())
                        ->load_from_lake_tablet(tablet, metadata, base_version, builder);
            }
            default:
                LOG(WARNING) << "only support LOCAL lake_persistent_index_type for now";
                return Status::InternalError("only support LOCAL lake_persistent_index_type for now");
            }
        }
    }

    OlapReaderStatistics stats;
    std::unique_ptr<Column> pk_column;
    if (pk_columns.size() > 1) {
        // more than one key column
        if (!PrimaryKeyEncoder::create_column(pkey_schema, &pk_column).ok()) {
            CHECK(false) << "create column for primary key encoder failed";
        }
    }
    vector<uint32_t> rowids;
    rowids.reserve(4096);
    auto chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, 4096);
    auto chunk = chunk_shared_ptr.get();
    // 2. scan all rowsets and segments to build primary index
    auto rowsets = tablet->get_rowsets(metadata);
    if (!rowsets.ok()) {
        return rowsets.status();
    }
    // NOTICE: primary index will be builded by segment files in metadata, and delvecs.
    // The delvecs we need are stored in delvec file by base_version and current MetaFileBuilder's cache.
    for (auto& rowset : *rowsets) {
        auto res = rowset->get_each_segment_iterator_with_delvec(pkey_schema, base_version, builder, &stats);
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
                    auto st = insert(rowset->id() + i, rowids, *pkc);
                    if (!st.ok()) {
                        return st;
                    }
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

} // namespace starrocks::lake

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

#include "storage/lake/lake_local_persistent_index_tablet_loader.h"

#include "storage/chunk_helper.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet.h"
#include "storage/storage_engine.h"

namespace starrocks::lake {

starrocks::Schema LakeLocalPersistentIndexTabletLoader::generate_pkey_schema() {
    std::shared_ptr<TabletSchema> tablet_schema = std::make_unique<TabletSchema>(_metadata->schema());
    vector<ColumnId> pk_columns(tablet_schema->num_key_columns());
    for (auto i = 0; i < tablet_schema->num_key_columns(); i++) {
        pk_columns[i] = (ColumnId)i;
    }
    return ChunkHelper::convert_schema(tablet_schema, pk_columns);
}

DataDir* LakeLocalPersistentIndexTabletLoader::data_dir() {
    return StorageEngine::instance()->get_persistent_index_store(_metadata->id());
}

TTabletId LakeLocalPersistentIndexTabletLoader::tablet_id() {
    return _metadata->id();
}

StatusOr<EditVersion> LakeLocalPersistentIndexTabletLoader::applied_version() {
    return EditVersion(_base_version, 0);
}

void LakeLocalPersistentIndexTabletLoader::setting() {
    // persistent index' minor compaction is a new strategy to decrease the IO amplification.
    // More detail: https://github.com/StarRocks/starrocks/issues/27581.
    // disable minor_compaction in cloud native table for now, will enable it later
    config::enable_pindex_minor_compaction = false;
}

Status LakeLocalPersistentIndexTabletLoader::rowset_iterator(
        const Schema& pkey_schema,
        const std::function<Status(const std::vector<ChunkIteratorPtr>&, uint32_t)>& handler) {
    OlapReaderStatistics stats;
    // scan all rowsets and segments to build primary index
    auto rowsets = Rowset::get_rowsets(_tablet_mgr, _metadata);
    _rowset_num = rowsets.size();

    // NOTICE: primary index will be builded by segment files in metadata, and delvecs.
    // The delvecs we need are stored in delvec file by base_version and current MetaFileBuilder's cache.
    for (auto& rowset : rowsets) {
        _total_data_size += rowset->data_size();
        _total_segments += rowset->num_segments();

        auto res = rowset->get_each_segment_iterator_with_delvec(pkey_schema, _base_version, _builder, &stats);
        if (!res.ok()) {
            return res.status();
        }
        auto& itrs = res.value();
        CHECK(itrs.size() == rowset->num_segments()) << "itrs.size != num_segments";
        RETURN_IF_ERROR(handler(itrs, rowset->id()));
    }

    return Status::OK();
}

} // namespace starrocks::lake
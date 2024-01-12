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

#include "storage/persistent_index_tablet_loader.h"

#include "storage/chunk_helper.h"
#include "storage/rowset/rowset.h"
#include "storage/tablet.h"
#include "storage/tablet_updates.h"

namespace starrocks {

starrocks::Schema PersistentIndexTabletLoader::generate_pkey_schema() {
    auto tablet_schema_ptr = _tablet->tablet_schema();
    vector<ColumnId> pk_columns(tablet_schema_ptr->num_key_columns());
    for (auto i = 0; i < tablet_schema_ptr->num_key_columns(); i++) {
        pk_columns[i] = (ColumnId)i;
    }
    return ChunkHelper::convert_schema(tablet_schema_ptr, pk_columns);
}

DataDir* PersistentIndexTabletLoader::data_dir() {
    return _tablet->data_dir();
}

TTabletId PersistentIndexTabletLoader::tablet_id() {
    return _tablet->tablet_id();
}

StatusOr<EditVersion> PersistentIndexTabletLoader::applied_version() {
    EditVersion lastest_applied_version;
    RETURN_IF_ERROR(_tablet->updates()->get_latest_applied_version(&lastest_applied_version));
    return lastest_applied_version;
}

Status PersistentIndexTabletLoader::rowset_iterator(
        const Schema& pkey_schema,
        const std::function<Status(const std::vector<ChunkIteratorPtr>&, uint32_t)>& handler) {
    OlapReaderStatistics stats;
    int64_t apply_version = 0;
    std::vector<RowsetSharedPtr> rowsets;
    std::vector<uint32_t> rowset_ids;
    RETURN_IF_ERROR(_tablet->updates()->get_apply_version_and_rowsets(&apply_version, &rowsets, &rowset_ids));

    size_t total_rows = 0;
    for (auto& rowset : rowsets) {
        _total_data_size += rowset->data_disk_size();
        _total_segments += rowset->num_segments();
        total_rows += rowset->num_rows();
    }
    _rowset_num = rowsets.size();
    size_t total_rows2 = 0;
    size_t total_dels = 0;
    auto status = _tablet->updates()->get_rowsets_total_stats(rowset_ids, &total_rows2, &total_dels);
    if (!status.ok() || total_rows2 != total_rows) {
        LOG(WARNING) << "load primary index get_rowsets_total_stats error: " << status;
    }
    DCHECK(total_rows2 == total_rows);
    if (_total_data_size > 4000000000 || total_rows > 10000000 || _total_segments > 400) {
        LOG(INFO) << "load large primary index start tablet:" << _tablet->tablet_id() << " version:" << apply_version
                  << " #rowset:" << rowsets.size() << " #segment:" << _total_segments << " #row:" << total_rows << " -"
                  << total_dels << "=" << total_rows - total_dels << " bytes:" << _total_data_size;
    }
    for (auto& rowset : rowsets) {
        RowsetReleaseGuard guard(rowset);
        auto res = rowset->get_segment_iterators2(pkey_schema, _tablet->tablet_schema(), data_dir()->get_meta(),
                                                  apply_version, &stats);
        if (!res.ok()) {
            return res.status();
        }
        auto& itrs = res.value();
        CHECK(itrs.size() == rowset->num_segments()) << "itrs.size != num_segments";
        RETURN_IF_ERROR(handler(itrs, rowset->rowset_meta()->get_rowset_seg_id()));
    }
    return Status::OK();
}
} // namespace starrocks
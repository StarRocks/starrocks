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

#include "storage/local_primary_key_compaction_conflict_resolver.h"

#include "storage/chunk_helper.h"
#include "storage/del_vector.h"
#include "storage/kv_store.h"
#include "storage/primary_index.h"
#include "storage/tablet.h"
#include "storage/update_manager.h"

namespace starrocks {

StatusOr<std::string> LocalPrimaryKeyCompactionConflictResolver::filename() const {
    return local_rows_mapper_filename(_tablet, _rowset->rowset_id_str());
}

Schema LocalPrimaryKeyCompactionConflictResolver::generate_pkey_schema() {
    const auto& schema = _rowset->schema();
    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < schema->num_key_columns(); i++) {
        pk_columns.push_back(static_cast<uint32_t>(i));
    }

    return ChunkHelper::convert_schema(schema, pk_columns);
}

Status LocalPrimaryKeyCompactionConflictResolver::segment_iterator(
        const std::function<Status(const CompactConflictResolveParams&, const std::vector<ChunkIteratorPtr>&,
                                   const std::function<void(uint32_t, const DelVectorPtr&, uint32_t)>&)>& handler) {
    OlapReaderStatistics stats;
    auto pkey_schema = generate_pkey_schema();
    RowsetReleaseGuard guard(_rowset->shared_from_this());
    const auto& schema = _rowset->schema();
    ASSIGN_OR_RETURN(auto segment_iters, _rowset->get_segment_iterators2(pkey_schema, schema, nullptr, 0, &stats));
    RETURN_ERROR_IF_FALSE(segment_iters.size() == _rowset->num_segments(), "itrs.size != num_segments");
    // init delvec loader
    auto delvec_loader = std::make_unique<LocalDelvecLoader>(_tablet->data_dir()->get_meta());
    // init params
    CompactConflictResolveParams params;
    params.tablet_id = _rowset->rowset_meta()->tablet_id();
    params.rowset_id = _rowset->rowset_meta()->get_rowset_seg_id();
    params.base_version = _base_version;
    params.new_version = _new_version;
    params.delvec_loader = delvec_loader.get();
    params.index = _index;
    return handler(params, segment_iters, [&](uint32_t rssid, const DelVectorPtr& dv, uint32_t num_dels) {
        *_total_deletes += num_dels;
        _delvecs->emplace_back(rssid, dv);
    });
}

Status LocalPrimaryKeyCompactionConflictResolver::breakpoint_check() {
    return _tablet->updates()->breakpoint_check();
}

} // namespace starrocks
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

#include "storage/lake/lake_primary_key_compaction_conflict_resolver.h"

#include "runtime/exec_env.h"
#include "storage/chunk_helper.h"
#include "storage/lake/filenames.h"
#include "storage/lake/lake_delvec_loader.h"
#include "storage/lake/rowset.h"
#include "storage/lake/types_fwd.h"
#include "storage/lake/update_manager.h"
#include "storage/rows_mapper.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake {

StatusOr<std::string> LakePrimaryKeyCompactionConflictResolver::filename() const {
    return lake_rows_mapper_filename(_rowset->tablet_id(), _txn_id);
}

Schema LakePrimaryKeyCompactionConflictResolver::generate_pkey_schema() {
    std::shared_ptr<TabletSchema> tablet_schema = std::make_shared<TabletSchema>(_metadata->schema());
    std::vector<uint32_t> pk_columns;
    for (size_t i = 0; i < tablet_schema->num_key_columns(); i++) {
        pk_columns.push_back(static_cast<uint32_t>(i));
    }

    return ChunkHelper::convert_schema(tablet_schema, pk_columns);
}

Status LakePrimaryKeyCompactionConflictResolver::segment_iterator(
        const std::function<Status(const CompactConflictResolveParams&, const std::vector<ChunkIteratorPtr>&,
                                   const std::function<void(uint32_t, const DelVectorPtr&, uint32_t)>&)>& handler) {
    OlapReaderStatistics stats;
    auto pkey_schema = generate_pkey_schema();
    ASSIGN_OR_RETURN(auto segment_iters, _rowset->get_each_segment_iterator(pkey_schema, false, &stats));
    RETURN_ERROR_IF_FALSE(segment_iters.size() == _rowset->num_segments());
    // init delvec loader
    SegmentReadOptions seg_options;

    auto delvec_loader =
            std::make_unique<LakeDelvecLoader>(_tablet_mgr, _builder, false /* fill cache */, seg_options.lake_io_opts);
    // init params
    CompactConflictResolveParams params;
    params.tablet_id = _rowset->tablet_id();
    params.rowset_id = _metadata->next_rowset_id();
    params.base_version = _base_version;
    params.new_version = _metadata->version();
    params.delvec_loader = delvec_loader.get();
    params.index = _index;
    return handler(params, segment_iters, [&](uint32_t rssid, const DelVectorPtr& dv, uint32_t num_dels) {
        (*_segment_id_to_add_dels)[rssid] += num_dels;
        _delvecs->emplace_back(rssid, dv);
    });
}

} // namespace starrocks::lake
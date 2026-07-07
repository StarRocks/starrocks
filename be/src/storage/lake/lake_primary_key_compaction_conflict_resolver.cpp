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

#include "storage/chunk_helper.h"
#include "storage/lake/filenames.h"
#include "storage/lake/lake_delvec_loader.h"
#include "storage/lake/rowset.h"
#include "storage/lake/types_fwd.h"
#include "storage/lake/update_manager.h"
#include "storage/rows_mapper.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake {

StatusOr<FileInfo> LakePrimaryKeyCompactionConflictResolver::filename() const {
    FileInfo info;
    // The rows mapper (.lcrm) is always stored on remote storage (S3/HDFS) and tracked in the
    // txn log, so any compute node can read it during publish without file replication. The
    // caller only reaches this resolver via light publish, which is gated on has_lcrm_file(),
    // so _lcrm_file.name() is guaranteed non-empty here.
    // PERFORMANCE: Use the size from metadata (_lcrm_file.size) to avoid a ~10-50ms get_size()
    // HEAD request to S3/HDFS per mapper file.
    ASSIGN_OR_RETURN(info.path, lake_rows_mapper_filename(_tablet_mgr, _rowset->tablet_id(), _lcrm_file.name()));
    if (_lcrm_file.size() > 0) {
        info.size = _lcrm_file.size();
    }
    return info;
}

Schema LakePrimaryKeyCompactionConflictResolver::generate_pkey_schema() {
    std::shared_ptr<TabletSchema> tablet_schema = std::make_shared<TabletSchema>(_metadata->schema());
    std::vector<uint32_t> pk_columns;
    pk_columns.reserve(tablet_schema->num_key_columns());
    for (size_t i = 0; i < tablet_schema->num_key_columns(); i++) {
        pk_columns.push_back(static_cast<uint32_t>(i));
    }

    return ChunkHelper::convert_schema(tablet_schema, pk_columns);
}

StatusOr<PrimaryKeyEncodingType> LakePrimaryKeyCompactionConflictResolver::primary_key_encoding_type() const {
    return _rowset->tablet_schema()->primary_key_encoding_type_or_error();
}

Status LakePrimaryKeyCompactionConflictResolver::segment_iterator(
        const std::function<Status(const CompactConflictResolveParams&, const std::vector<ChunkIteratorPtr>&,
                                   const std::function<void(uint32_t, const DelVectorPtr&, uint32_t)>&)>& handler) {
    OlapReaderStatistics stats;
    auto pkey_schema = generate_pkey_schema();
    ASSIGN_OR_RETURN(auto segment_iters, _rowset->get_each_segment_iterator(pkey_schema, false, &stats));
    RETURN_ERROR_IF_FALSE(segment_iters.size() == _rowset->num_segments());
    // init delvec loader
    LakeIOOptions lake_io_opts{.fill_data_cache = true, .skip_disk_cache = false};

    // Cache the base-version metadata once so per-rssid delvec loads skip get_tablet_metadata
    // and its TabletMetadataPB deep copy (hot when one compaction merges hundreds of rowsets).
    ASSIGN_OR_RETURN(auto base_metadata,
                     _tablet_mgr->get_tablet_metadata(_rowset->tablet_id(), _base_version, true /* fill_cache */));
    auto delvec_loader = std::make_unique<LakeDelvecLoader>(_tablet_mgr, _builder, true /* fill cache */, lake_io_opts,
                                                            std::move(base_metadata));
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

Status LakePrimaryKeyCompactionConflictResolver::segment_iterator(
        const std::function<Status(const CompactConflictResolveParams&, const std::vector<SegmentPtr>&,
                                   const std::function<void(uint32_t, const DelVectorPtr&, uint32_t)>&)>& handler) {
    // load all segments
    std::vector<SegmentPtr> segments;
    RETURN_IF_ERROR(_rowset->load_segments(&segments, true /* file cache*/));
    RETURN_ERROR_IF_FALSE(segments.size() == _rowset->num_segments());
    // init delvec loader
    LakeIOOptions lake_io_opts{.fill_data_cache = true, .skip_disk_cache = false};

    // See the overload above: cache base-version metadata once to skip per-rssid deep copy.
    ASSIGN_OR_RETURN(auto base_metadata,
                     _tablet_mgr->get_tablet_metadata(_rowset->tablet_id(), _base_version, true /* fill_cache */));
    auto delvec_loader = std::make_unique<LakeDelvecLoader>(_tablet_mgr, _builder, true /* fill cache */, lake_io_opts,
                                                            std::move(base_metadata));
    // init params
    CompactConflictResolveParams params;
    params.tablet_id = _rowset->tablet_id();
    params.rowset_id = _metadata->next_rowset_id();
    params.base_version = _base_version;
    params.new_version = _metadata->version();
    params.delvec_loader = delvec_loader.get();
    params.index = _index;
    return handler(params, segments, [&](uint32_t rssid, const DelVectorPtr& dv, uint32_t num_dels) {
        (*_segment_id_to_add_dels)[rssid] += num_dels;
        _delvecs->emplace_back(rssid, dv);
    });
}

std::vector<uint32_t> LakePrimaryKeyCompactionConflictResolver::output_segment_num_rows() const {
    // segment_metas order matches the positionally-aligned segment/iterator vectors produced by
    // load_segments / get_each_segment_iterator, so index i here is the i-th resolved segment.
    const auto& rowset_meta = _rowset->metadata();
    std::vector<uint32_t> result;
    result.reserve(rowset_meta.segment_metas_size());
    for (int i = 0; i < rowset_meta.segment_metas_size(); i++) {
        const auto& seg_meta = rowset_meta.segment_metas(i);
        // num_rows is optional; report "unknown" for an old rowset that lacks it so the base resolver
        // fails clearly instead of under-advancing the rows-mapper. Sibling readers guard it the same
        // way (see lake_persistent_index.cpp / rowset.cpp).
        result.push_back(seg_meta.has_num_rows() ? static_cast<uint32_t>(seg_meta.num_rows()) : kUnknownSegmentNumRows);
    }
    return result;
}

} // namespace starrocks::lake
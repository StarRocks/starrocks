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

#include "storage/lake/rowset.h"

#include <future>
#include <unordered_set>

#include "base/debug/trace.h"
#include "column/datum_convert.h"
#include "common/config.h"
#include "runtime/current_thread.h"
#include "storage/chunk_helper.h"
#include "storage/delete_predicates.h"
#include "storage/lake/column_mode_partial_update_handler.h"
#include "storage/lake/lake_delvec_loader.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/segment_metadata_filter.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_range_helper.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/update_manager.h"
#include "storage/projection_iterator.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/short_key_range_option.h"
#include "storage/seek_range.h"
#include "storage/tablet_schema_map.h"
#include "storage/union_iterator.h"
#include "types/logical_type.h"
#include "types/type_descriptor.h"

namespace starrocks::lake {

Rowset::Rowset(TabletManager* tablet_mgr, int64_t tablet_id, const RowsetMetadataPB* metadata, int index,
               TabletSchemaPtr tablet_schema)
        : _tablet_mgr(tablet_mgr),
          _tablet_id(tablet_id),
          _metadata(metadata),
          _index(index),
          _tablet_schema(std::move(tablet_schema)),
          _parallel_load(config::enable_load_segment_parallel),
          _compaction_segment_limit(0) {}

Rowset::Rowset(TabletManager* tablet_mgr, TabletMetadataPtr tablet_metadata, int rowset_index,
               size_t compaction_segment_limit)
        : _tablet_mgr(tablet_mgr),
          _tablet_id(tablet_metadata->id()),
          _metadata(&tablet_metadata->rowsets(rowset_index)),
          _index(rowset_index),
          _tablet_metadata(std::move(tablet_metadata)),
          _parallel_load(config::enable_load_segment_parallel),
          _compaction_segment_limit(0) {
    auto rowset_id = _tablet_metadata->rowsets(rowset_index).id();
    if (_tablet_metadata->rowset_to_schema().empty() ||
        _tablet_metadata->rowset_to_schema().find(rowset_id) == _tablet_metadata->rowset_to_schema().end()) {
        _tablet_schema = GlobalTabletSchemaMap::Instance()->emplace(_tablet_metadata->schema()).first;
    } else {
        auto schema_id = _tablet_metadata->rowset_to_schema().at(rowset_id);
        CHECK(_tablet_metadata->historical_schemas().count(schema_id) > 0);
        _tablet_schema =
                GlobalTabletSchemaMap::Instance()->emplace(_tablet_metadata->historical_schemas().at(schema_id)).first;
    }

    // if segments are loaded in parallel, can not perform partial compaction, since
    // loaded segments might not be in the same sequence than that in rowset metadata
    if (is_overlapped() && compaction_segment_limit > 0 && !_parallel_load) {
        // check if ouput schema has `struct` column, if is, do not perform partial compaction
        bool skip_limit = false;
        for (const auto& column : _tablet_schema->columns()) {
            if (column.type() == LogicalType::TYPE_STRUCT) {
                skip_limit = true;
                break;
            }
        }
        _compaction_segment_limit = skip_limit ? 0 : compaction_segment_limit;
    } else {
        // every segment will be used
        _compaction_segment_limit = 0;
    }
}

Rowset::Rowset(TabletManager* tablet_mgr, TabletMetadataPtr tablet_metadata, int rowset_index, int32_t segment_start,
               int32_t segment_end)
        : _tablet_mgr(tablet_mgr),
          _tablet_id(tablet_metadata->id()),
          _metadata(&tablet_metadata->rowsets(rowset_index)),
          _index(rowset_index),
          _tablet_metadata(std::move(tablet_metadata)),
          _parallel_load(false), // Disable parallel load to ensure segment order
          _compaction_segment_limit(0),
          _segment_range_start(segment_start),
          _segment_range_end(segment_end) {
    DCHECK(segment_start >= 0);
    DCHECK(segment_end > segment_start);
    DCHECK(segment_end <= _metadata->segments_size());

    auto rowset_id = _tablet_metadata->rowsets(rowset_index).id();
    if (_tablet_metadata->rowset_to_schema().empty() ||
        _tablet_metadata->rowset_to_schema().find(rowset_id) == _tablet_metadata->rowset_to_schema().end()) {
        _tablet_schema = GlobalTabletSchemaMap::Instance()->emplace(_tablet_metadata->schema()).first;
    } else {
        auto schema_id = _tablet_metadata->rowset_to_schema().at(rowset_id);
        CHECK(_tablet_metadata->historical_schemas().count(schema_id) > 0);
        _tablet_schema =
                GlobalTabletSchemaMap::Instance()->emplace(_tablet_metadata->historical_schemas().at(schema_id)).first;
    }
}

Rowset::~Rowset() {
    if (_tablet_metadata) {
        DCHECK_LT(_index, _tablet_metadata->rowsets_size())
                << "tablet metadata been modified before rowset been destroyed";
        DCHECK_EQ(_metadata, &_tablet_metadata->rowsets(_index))
                << "tablet metadata been modified during rowset been destroyed";
    }
}

StatusOr<std::optional<SeekRange>> Rowset::get_seek_range() const {
    const TabletRangePB* range_pb = nullptr;
    if (_metadata->has_range()) {
        range_pb = &_metadata->range();
    } else if (_tablet_metadata != nullptr && _tablet_metadata->has_range()) {
        range_pb = &_tablet_metadata->range();
    }

    if (range_pb == nullptr) {
        return std::optional<SeekRange>{};
    }

    // Do not use mem_pool here. SeekRange will reference the data owned by protobuf metadata,
    // and metadata lifetime is guaranteed to outlive rowset iterators.
    ASSIGN_OR_RETURN(auto range, TabletRangeHelper::create_seek_range_from(*range_pb, _tablet_schema, nullptr));
    return std::optional<SeekRange>(std::move(range));
}

Status Rowset::add_partial_compaction_segments_info(TxnLogPB_OpCompaction* op_compaction, TabletWriter* writer,
                                                    uint64_t& uncompacted_num_rows, uint64_t& uncompacted_data_size) {
    DCHECK(partial_segments_compaction());

    if (metadata().has_range()) {
        op_compaction->mutable_output_rowset()->mutable_range()->CopyFrom(metadata().range());
    }

    std::vector<SegmentPtr> segments;
    LakeIOOptions lake_io_opts{.fill_data_cache = true};
    SegmentReadOptions seg_options;
    seg_options.lake_io_opts = lake_io_opts;
    std::pair<std::vector<SegmentPtr>, std::vector<SegmentPtr>> not_used_segments;
    RETURN_IF_ERROR(load_segments(&segments, seg_options, &not_used_segments));

    bool clear_file_size_info = false;
    bool clear_encryption_meta = (metadata().segments_size() != metadata().segment_encryption_metas_size());

    // NOTE: segments order must be kept, always already compacted segments, then compacted new segments,
    //       at last uncompacted segments, otherwise it will have data consistency problem for tables like
    //       UNIQUE table

    // 1. add already compacted segments first
    auto& already_compacted_segments = not_used_segments.first;
    for (size_t i = 0; i < metadata().next_compaction_offset(); ++i) {
        op_compaction->mutable_output_rowset()->add_segments(metadata().segments(i));
        StatusOr<int64_t> size_or = already_compacted_segments[i]->get_data_size();
        int64_t file_size = 0;
        if (size_or.ok()) {
            file_size = *size_or;
            op_compaction->mutable_output_rowset()->add_segment_size(file_size);
        } else {
            clear_file_size_info = true;
            LOG(WARNING) << "unable to get file size for segment " << metadata().segments(i) << " for tablet "
                         << _tablet_id << " when collecting uncompacted segment info, error: " << size_or.status();
        }
        if (!clear_encryption_meta) {
            op_compaction->mutable_output_rowset()->add_segment_encryption_metas(
                    metadata().segment_encryption_metas(i));
        }
        if (i < metadata().shared_segments_size()) {
            op_compaction->mutable_output_rowset()->add_shared_segments(metadata().shared_segments(i));
        }
        if (i < metadata().segment_metas_size()) {
            op_compaction->mutable_output_rowset()->add_segment_metas()->CopyFrom(metadata().segment_metas(i));
        }
        uncompacted_num_rows += already_compacted_segments[i]->num_rows();
        uncompacted_data_size += file_size;
    }

    // 2. add compacted segments in this round
    op_compaction->set_new_segment_offset(op_compaction->output_rowset().segments_size());
    // For rowsets with sparse segment_idx, new compacted segments must use fresh ids that do not
    // collide with any remaining old segments copied into output_rowset.
    uint32_t next_segment_id = metadata().segments_size() > 0 ? get_max_segment_idx(metadata()) + 1
                                                              : op_compaction->output_rowset().segments_size();
    for (const auto& file : writer->segments()) {
        op_compaction->mutable_output_rowset()->add_segments(file.path);
        op_compaction->mutable_output_rowset()->add_segment_size(file.size.value());
        op_compaction->mutable_output_rowset()->add_segment_encryption_metas(file.encryption_meta);
        if (metadata().shared_segments_size() > 0) {
            op_compaction->mutable_output_rowset()->add_shared_segments(false);
        }
        if (metadata().segment_metas_size() > 0) {
            auto* segment_meta = op_compaction->mutable_output_rowset()->add_segment_metas();
            file.sort_key_min.to_proto(segment_meta->mutable_sort_key_min());
            file.sort_key_max.to_proto(segment_meta->mutable_sort_key_max());
            segment_meta->set_num_rows(file.num_rows);
            segment_meta->set_segment_idx(next_segment_id++);
        }
    }
    op_compaction->set_new_segment_count(writer->segments().size());

    // 3. set next compaction offset
    op_compaction->mutable_output_rowset()->set_next_compaction_offset(op_compaction->output_rowset().segments_size());

    // 4. add uncompacted segments
    auto& uncompacted_segments = not_used_segments.second;
    for (size_t i = metadata().next_compaction_offset() + _compaction_segment_limit, idx = 0;
         i < metadata().segments_size(); ++i, ++idx) {
        op_compaction->mutable_output_rowset()->add_segments(metadata().segments(i));
        StatusOr<int64_t> size_or = uncompacted_segments[idx]->get_data_size();
        int64_t file_size = 0;
        if (size_or.ok()) {
            file_size = *size_or;
            op_compaction->mutable_output_rowset()->add_segment_size(file_size);
        } else {
            clear_file_size_info = true;
            LOG(WARNING) << "unable to get file size for segment " << metadata().segments(i) << " for tablet "
                         << _tablet_id << " when collecting uncompacted segment info, error: " << size_or.status();
        }
        if (!clear_encryption_meta) {
            op_compaction->mutable_output_rowset()->add_segment_encryption_metas(
                    metadata().segment_encryption_metas(i));
        }
        if (i < metadata().shared_segments_size()) {
            op_compaction->mutable_output_rowset()->add_shared_segments(metadata().shared_segments(i));
        }
        if (i < metadata().segment_metas_size()) {
            op_compaction->mutable_output_rowset()->add_segment_metas()->CopyFrom(metadata().segment_metas(i));
        }

        uncompacted_num_rows += uncompacted_segments[idx]->num_rows();
        uncompacted_data_size += file_size;
    }
    if (clear_file_size_info) {
        op_compaction->mutable_output_rowset()->mutable_segment_size()->Clear();
    }
    if (clear_encryption_meta) {
        op_compaction->mutable_output_rowset()->mutable_segment_encryption_metas()->Clear();
    }
    return Status::OK();
}

StatusOr<std::vector<ChunkIteratorPtr>> Rowset::read(const Schema& schema, const RowsetReadOptions& options) {
    SegmentReadOptions seg_options;
    if (options.lake_io_opts.fs) {
        seg_options.fs = options.lake_io_opts.fs;
    } else {
        auto root_loc = _tablet_mgr->tablet_root_location(tablet_id());
        ASSIGN_OR_RETURN(seg_options.fs, FileSystem::CreateSharedFromString(root_loc));
    }
    seg_options.stats = options.stats;
    seg_options.ranges = options.ranges;
    seg_options.pred_tree = options.pred_tree;
    seg_options.pred_tree_for_zone_map = options.pred_tree_for_zone_map;
    seg_options.runtime_filter_preds = options.runtime_filter_preds;
    seg_options.enable_join_runtime_filter_pushdown = options.enable_join_runtime_filter_pushdown;
    seg_options.use_page_cache = options.use_page_cache;
    seg_options.profile = options.profile;
    seg_options.reader_type = options.reader_type;
    seg_options.chunk_size = options.chunk_size;
    seg_options.global_dictmaps = options.global_dictmaps;
    seg_options.unused_output_column_ids = options.unused_output_column_ids;
    seg_options.runtime_range_pruner = options.runtime_range_pruner;
    seg_options.tablet_schema = options.tablet_schema;
    seg_options.lake_io_opts = options.lake_io_opts;
    seg_options.asc_hint = options.asc_hint;
    seg_options.column_access_paths = options.column_access_paths;
    seg_options.has_preaggregation = options.has_preaggregation;
    seg_options.enable_predicate_col_late_materialize = options.enable_predicate_col_late_materialize;
    if (options.is_primary_keys) {
        seg_options.is_primary_keys = true;
        seg_options.delvec_loader = std::make_shared<LakeDelvecLoader>(
                _tablet_mgr, nullptr, seg_options.lake_io_opts.fill_data_cache, seg_options.lake_io_opts);
        seg_options.dcg_loader = std::make_shared<LakeDeltaColumnGroupLoader>(_tablet_metadata);
        seg_options.version = options.version;
        seg_options.tablet_id = tablet_id();
        seg_options.rowset_id = metadata().id();
    }
    if (options.delete_predicates != nullptr) {
        seg_options.delete_predicates = options.delete_predicates->get_predicates(_index);
    }

    if (options.short_key_ranges_option != nullptr) { // logical split.
        seg_options.short_key_ranges = options.short_key_ranges_option->short_key_ranges;
    }
    seg_options.reader_type = options.reader_type;
    seg_options.enable_gin_filter = options.enable_gin_filter;
    seg_options.prune_column_after_index_filter = options.prune_column_after_index_filter;
    seg_options.has_preaggregation = options.has_preaggregation;

    std::unique_ptr<Schema> segment_schema_guard;
    auto* segment_schema = const_cast<Schema*>(&schema);
    // Append the columns with delete condition to segment schema.
    std::set<ColumnId> need_added_column;
    seg_options.delete_predicates.get_column_ids(&need_added_column);

    for (ColumnId cid : need_added_column) {
        const TabletColumn& col = options.tablet_schema->column(cid);
        if (segment_schema->get_field_by_name(std::string(col.name())) != nullptr) {
            continue;
        }
        // copy on write
        if (segment_schema == &schema) {
            segment_schema = new Schema(schema);
            segment_schema_guard.reset(segment_schema);
        }
        auto f = ChunkHelper::convert_field(cid, col);
        segment_schema->append(std::make_shared<Field>(std::move(f)));
    }

    std::vector<ChunkIteratorPtr> segment_iterators;

    ASSIGN_OR_RETURN(auto shared_segment_range, get_seek_range());

    // Check if segment metadata filter can be used.
    // Apply metadata filter BEFORE loading segments to avoid loading unnecessary segment footers.
    bool use_metadata_filter = config::enable_lake_segment_metadata_filter && metadata().segment_metas_size() > 0 &&
                               !options.pred_tree_for_zone_map.empty() && options.tablet_schema != nullptr;

    std::unordered_set<int> skip_segment_idxs;
    if (use_metadata_filter) {
        for (int i = 0; i < metadata().segment_metas_size() && i < num_segments(); i++) {
            const auto& segment_meta = metadata().segment_metas(i);
            if (!SegmentMetadataFilter::may_contain(segment_meta, options.pred_tree_for_zone_map,
                                                    *options.tablet_schema)) {
                skip_segment_idxs.insert(i);
                if (options.stats) {
                    // Use num_rows from segment metadata if available, otherwise use 0
                    int64_t filtered_rows = segment_meta.has_num_rows() ? segment_meta.num_rows() : 0;
                    options.stats->segment_metadata_filtered += filtered_rows;
                    options.stats->segments_metadata_filtered++;
                }
                VLOG(2) << "Skipped segment " << i << " by metadata filter, rowset: " << metadata().id();
            }
        }
    }

    std::vector<SegmentPtr> segments;
    const std::unordered_set<int>* skip_ptr = skip_segment_idxs.empty() ? nullptr : &skip_segment_idxs;
    RETURN_IF_ERROR(load_segments(&segments, seg_options, nullptr, skip_ptr));

    // Update segments_read_count after filtering
    if (options.stats) {
        options.stats->segments_read_count += num_segments() - skip_segment_idxs.size();
    }

    for (int i = 0; i < segments.size(); i++) {
        auto& seg_ptr = segments[i];
        // Skip segments that were filtered by metadata filter (nullptr placeholders)
        if (seg_ptr == nullptr) {
            continue;
        }
        if (seg_ptr->num_rows() == 0) {
            continue;
        }

        seg_options.tablet_range = std::nullopt;
        if (i < _metadata->shared_segments_size() && _metadata->shared_segments(i) &&
            shared_segment_range.has_value()) {
            seg_options.tablet_range = *shared_segment_range;
        }

        if (options.rowid_range_option != nullptr) { // physical split.
            auto [rowid_range, is_first_split_of_segment] =
                    options.rowid_range_option->get_segment_rowid_range(this, seg_ptr.get());
            if (rowid_range == nullptr) {
                continue;
            }
            seg_options.rowid_range_option = std::move(rowid_range);
            seg_options.is_first_split_of_segment = is_first_split_of_segment;
        } else if (options.short_key_ranges_option != nullptr) { // logical split.
            seg_options.is_first_split_of_segment = options.short_key_ranges_option->is_first_split_of_tablet;
        } else {
            seg_options.is_first_split_of_segment = true;
        }

        auto res = seg_ptr->new_iterator(*segment_schema, seg_options);
        if (res.status().is_end_of_file()) {
            continue;
        }
        if (!res.ok()) {
            return res.status();
        }
        if (segment_schema != &schema) {
            segment_iterators.emplace_back(new_projection_iterator(schema, std::move(res).value()));
        } else {
            segment_iterators.emplace_back(std::move(res).value());
        }
    }
    if (segment_iterators.size() > 1 && !is_overlapped()) {
        // union non-overlapped segment iterators
        auto iter = new_union_iterator(std::move(segment_iterators));
        return std::vector<ChunkIteratorPtr>{iter};
    } else {
        return segment_iterators;
    }
}

StatusOr<size_t> Rowset::get_read_iterator_num() {
    std::vector<SegmentPtr> segments;
    RETURN_IF_ERROR(load_segments(&segments, false));

    size_t segment_num = 0;
    for (auto& seg_ptr : segments) {
        if (seg_ptr->num_rows() == 0) {
            continue;
        }
        ++segment_num;
    }

    if (segment_num > 1 && !is_overlapped()) {
        return 1;
    } else {
        return segment_num;
    }
}

StatusOr<std::vector<ChunkIteratorPtr>> Rowset::get_each_segment_iterator(const Schema& schema, bool file_data_cache,
                                                                          OlapReaderStatistics* stats) {
    TRACE_COUNTER_SCOPE_LATENCY_US("get_each_segment_us");
    std::vector<SegmentPtr> segments;
    RETURN_IF_ERROR(load_segments(&segments, file_data_cache));
    std::vector<ChunkIteratorPtr> seg_iterators;
    seg_iterators.reserve(segments.size());
    auto root_loc = _tablet_mgr->tablet_root_location(tablet_id());
    SegmentReadOptions seg_options;
    ASSIGN_OR_RETURN(seg_options.fs, FileSystem::CreateSharedFromString(root_loc));
    seg_options.stats = stats;

    ASSIGN_OR_RETURN(auto shared_segment_range, get_seek_range());

    for (int i = 0; i < segments.size(); i++) {
        auto& seg_ptr = segments[i];
        seg_options.tablet_range = std::nullopt;
        if (i < _metadata->shared_segments_size() && _metadata->shared_segments(i) &&
            shared_segment_range.has_value()) {
            seg_options.tablet_range = *shared_segment_range;
        }
        auto res = seg_ptr->new_iterator(schema, seg_options);
        if (res.status().is_end_of_file()) {
            continue;
        }
        if (!res.ok()) {
            return res.status();
        }
        seg_iterators.push_back(std::move(res).value());
    }
    return seg_iterators;
}

StatusOr<std::vector<ChunkIteratorPtr>> Rowset::get_each_segment_iterator_with_delvec(const Schema& schema,
                                                                                      int64_t version,
                                                                                      const MetaFileBuilder* builder,
                                                                                      OlapReaderStatistics* stats) {
    std::vector<SegmentPtr> segments;
    RETURN_IF_ERROR(load_segments(&segments, false));
    auto root_loc = _tablet_mgr->tablet_root_location(tablet_id());
    std::vector<ChunkIteratorPtr> seg_iterators;
    seg_iterators.reserve(segments.size());
    SegmentReadOptions seg_options;
    ASSIGN_OR_RETURN(seg_options.fs, FileSystem::CreateSharedFromString(root_loc));
    seg_options.stats = stats;
    seg_options.lake_io_opts.fs = seg_options.fs;
    seg_options.lake_io_opts.location_provider = _tablet_mgr->location_provider();
    seg_options.is_primary_keys = true;
    seg_options.delvec_loader =
            std::make_shared<LakeDelvecLoader>(_tablet_mgr, builder, true /*fill cache*/, seg_options.lake_io_opts);
    seg_options.dcg_loader = std::make_shared<LakeDeltaColumnGroupLoader>(_tablet_metadata);
    seg_options.version = version;
    seg_options.tablet_id = tablet_id();
    seg_options.rowset_id = metadata().id();

    ASSIGN_OR_RETURN(auto shared_segment_range, get_seek_range());

    for (int i = 0; i < segments.size(); i++) {
        auto& seg_ptr = segments[i];
        seg_options.tablet_range = std::nullopt;
        if (i < _metadata->shared_segments_size() && _metadata->shared_segments(i) &&
            shared_segment_range.has_value()) {
            seg_options.tablet_range = *shared_segment_range;
        }
        auto res = seg_ptr->new_iterator(schema, seg_options);
        if (res.status().is_end_of_file()) {
            continue;
        }
        if (!res.ok()) {
            return res.status();
        }
        seg_iterators.push_back(std::move(res).value());
    }
    return seg_iterators;
}

RowsetId Rowset::rowset_id() const {
    RowsetId rowset_id;
    rowset_id.init(id());
    return rowset_id;
}

std::vector<SegmentSharedPtr> Rowset::get_segments() {
    if (!_segments.empty()) {
        return _segments;
    }

    auto segments_or = segments(true);
    if (!segments_or.ok()) {
        return {};
    }
    _segments = std::move(segments_or.value());
    return _segments;
}

StatusOr<std::vector<SegmentPtr>> Rowset::segments(bool fill_cache) {
    LakeIOOptions lake_io_opts{.fill_data_cache = fill_cache, .fill_metadata_cache = fill_cache};
    return segments(lake_io_opts);
}

StatusOr<std::vector<SegmentPtr>> Rowset::segments(const LakeIOOptions& lake_io_opts) {
    std::vector<SegmentPtr> segments;
    SegmentReadOptions seg_options;
    seg_options.lake_io_opts = lake_io_opts;
    RETURN_IF_ERROR(load_segments(&segments, seg_options, nullptr));
    return segments;
}

Status Rowset::load_segments(std::vector<SegmentPtr>* segments, bool fill_cache, int64_t buffer_size) {
    SegmentReadOptions seg_options;
    seg_options.lake_io_opts.fill_data_cache = fill_cache;
    seg_options.lake_io_opts.fill_metadata_cache = fill_cache;
    seg_options.lake_io_opts.buffer_size = buffer_size;
    return load_segments(segments, seg_options, nullptr);
}

Status Rowset::load_segments(std::vector<SegmentPtr>* segments, SegmentReadOptions& seg_options,
                             std::pair<std::vector<SegmentPtr>, std::vector<SegmentPtr>>* not_used_segments,
                             const std::unordered_set<int>* skip_segment_idxs) {
#if !defined BE_TEST && !defined(BUILD_FORMAT_LIB)
    RETURN_IF_ERROR(tls_thread_status.mem_tracker()->check_mem_limit("LoadSegments"));
#endif

    segments->reserve(segments->size() + metadata().segments_size());

    size_t footer_size_hint = 16 * 1024;
    int32_t seg_idx = 0;
    bool ignore_lost_segment = config::experimental_lake_ignore_lost_segment;

    // RowsetMetaData upgrade from old version may not have the field of segment_size
    auto segment_size_size = metadata().segment_size_size();
    auto segment_file_size = metadata().segments_size();
    auto bundle_file_offsets_size = metadata().bundle_file_offsets_size();
    bool has_segment_size = segment_size_size == segment_file_size;
    LOG_IF(ERROR, segment_size_size > 0 && segment_size_size != segment_file_size)
            << "segment_size size != segment file size, tablet: " << _tablet_id << ", rowset: " << metadata().id()
            << ", segment file size: " << segment_file_size << ", segment_size size: " << segment_size_size;
    LOG_IF(ERROR, bundle_file_offsets_size > 0 && segment_size_size != bundle_file_offsets_size)
            << "segment_size size != bundle file offsets size, tablet: " << _tablet_id
            << ", rowset: " << metadata().id() << ", segment file size: " << segment_file_size
            << ", bundle file offsets size: " << bundle_file_offsets_size
            << ", segment_size size: " << segment_size_size;

    const auto& files_to_size = metadata().segment_size();
    const auto& files_to_offset = metadata().bundle_file_offsets();
    int index = 0;

    // Determine segment range based on mode
    int32_t seg_start = 0;
    int32_t seg_end = metadata().segments_size();
    if (is_segment_range_mode()) {
        seg_start = _segment_range_start;
        seg_end = _segment_range_end;
    }

    // When parallel loading is enabled, we need to preserve the index mapping between
    // segments vector and metadata. We use a vector of (index, future) pairs to track
    // which index each loaded segment should be placed at.
    struct SegmentLoadFuture {
        int target_idx;
        uint32_t segment_id;
        std::future<std::pair<StatusOr<SegmentPtr>, std::string>> future;
    };
    std::vector<SegmentLoadFuture> segment_futures;

    // Pre-allocate segments vector to maintain correct index mapping.
    // This is necessary because when parallel loading is enabled with skip_segment_idxs,
    // we need to ensure segments[i] corresponds to metadata segment i.
    bool use_index_mapping = _parallel_load && skip_segment_idxs != nullptr && !skip_segment_idxs->empty();
    int base_idx = segments->size();
    if (use_index_mapping) {
        segments->resize(base_idx + metadata().segments_size());
    }

    auto check_status_at_index = [&](StatusOr<SegmentPtr>& segment_or, const std::string& seg_name, int seg_id,
                                     int target_idx) -> Status {
        if (segment_or.ok()) {
            if (use_index_mapping) {
                (*segments)[target_idx] = std::move(segment_or.value());
            } else {
                segments->emplace_back(std::move(segment_or.value()));
            }
        } else if (segment_or.status().is_not_found() && ignore_lost_segment) {
            LOG(WARNING) << "Ignored lost segment " << seg_name;
        } else {
            return segment_or.status().clone_and_prepend(fmt::format(
                    "load_segments failed tablet:{} rowset:{} segid:{}", _tablet_id, metadata().id(), seg_id));
        }
        return Status::OK();
    };

    // For segment range mode, seg_idx should match the original segment index
    seg_idx = seg_start;
    for (index = seg_start; index < seg_end; index++) {
        const auto& seg_name = metadata().segments(index);
        int current_idx = base_idx + seg_idx;
        uint32_t segment_id = get_segment_idx(metadata(), index);

        // Skip segments that are filtered by metadata filter
        if (skip_segment_idxs != nullptr && skip_segment_idxs->count(seg_idx) > 0) {
            if (!use_index_mapping) {
                segments->emplace_back(nullptr);
            }
            // When use_index_mapping is true, the slot is already nullptr from resize
            seg_idx++;
            continue;
        }

        std::string segment_path;
        auto lake_io_opts = seg_options.lake_io_opts;
        if (lake_io_opts.location_provider) {
            segment_path = lake_io_opts.location_provider->segment_location(tablet_id(), seg_name);
        } else {
            segment_path = _tablet_mgr->segment_location(tablet_id(), seg_name);
        }
        auto segment_info = FileInfo{.path = segment_path, .fs = seg_options.fs};
        if (LIKELY(has_segment_size)) {
            segment_info.size = files_to_size.Get(index);
        }
        if (bundle_file_offsets_size > 0) {
            segment_info.bundle_file_offset = files_to_offset.Get(index);
        }
        auto segment_encryption_metas_size = metadata().segment_encryption_metas_size();
        if (segment_encryption_metas_size > 0) {
            if (index >= segment_encryption_metas_size) {
                string msg = fmt::format("tablet:{} rowset:{} index:{} >= segment_encryption_metas size:{}", _tablet_id,
                                         metadata().id(), index, segment_encryption_metas_size);
                LOG(ERROR) << msg;
                return Status::Corruption(msg);
            }
            segment_info.encryption_meta = metadata().segment_encryption_metas(index);
        }

        if (_parallel_load) {
            int captured_idx = current_idx;
            auto task = std::make_shared<std::packaged_task<std::pair<StatusOr<SegmentPtr>, std::string>()>>([=]() {
                auto result = _tablet_mgr->load_segment(segment_info, segment_id, lake_io_opts,
                                                        lake_io_opts.fill_metadata_cache, _tablet_schema);
                return std::make_pair(std::move(result), seg_name);
            });

            auto packaged_func = [task]() { (*task)(); };
            if (auto st = ExecEnv::GetInstance()->load_segment_thread_pool()->submit_func(std::move(packaged_func));
                !st.ok()) {
                // try load segment serially
                LOG(WARNING) << "sumbit_func failed: " << st.code_as_string()
                             << ", try to load segment serially, seg_id: " << segment_id;
                auto segment_or = _tablet_mgr->load_segment(segment_info, segment_id, &footer_size_hint, lake_io_opts,
                                                            lake_io_opts.fill_metadata_cache, _tablet_schema);
                if (auto status = check_status_at_index(segment_or, seg_name, segment_id, captured_idx); !status.ok()) {
                    return status;
                }
            } else {
                segment_futures.push_back({captured_idx, segment_id, task->get_future()});
            }
            seg_idx++;
        } else {
            auto segment_or = _tablet_mgr->load_segment(segment_info, segment_id, &footer_size_hint, lake_io_opts,
                                                        lake_io_opts.fill_metadata_cache, _tablet_schema);
            if (auto status = check_status_at_index(segment_or, seg_name, segment_id, current_idx); !status.ok()) {
                return status;
            }
            seg_idx++;
        }
    }

    for (auto& f : segment_futures) {
        auto result_pair = f.future.get();
        auto segment_or = result_pair.first;
        // In segment range mode, target_idx - base_idx gives the actual segment ID
        if (auto status = check_status_at_index(segment_or, result_pair.second, f.segment_id, f.target_idx);
            !status.ok()) {
            return status;
        }
    }

    // if this is for partial compaction, erase segments that are alreagy compacted and segments
    // that are not compacted but exceeds compaction limit
    if (partial_segments_compaction()) {
        if (not_used_segments) {
            not_used_segments->first.insert(not_used_segments->first.begin(), segments->begin(),
                                            segments->begin() + metadata().next_compaction_offset());
            not_used_segments->second.insert(
                    not_used_segments->second.begin(),
                    segments->begin() + metadata().next_compaction_offset() + _compaction_segment_limit,
                    segments->end());
            LOG(INFO) << "tablet: " << tablet_id() << ", version: " << _tablet_metadata->version()
                      << ", rowset: " << metadata().id() << ", total segments: " << metadata().segments_size()
                      << ", compacted segments: " << not_used_segments->first.size()
                      << ", uncompacted segments: " << not_used_segments->second.size();
        }

        // trim compacted segments
        segments->erase(segments->begin(), segments->begin() + metadata().next_compaction_offset());
        // trim uncompacted segments
        segments->resize(_compaction_segment_limit);
    }

    return Status::OK();
}

int64_t Rowset::data_size_after_deletion() const {
    // get data size after delete vector applied
    if (num_rows() == 0 || data_size() == 0) {
        return 0;
    }
    // data size * (num_rows - num_deleted_rows) / num_rows
    return (int64_t)(data_size() * ((double)(num_rows() - num_dels()) / num_rows()));
}

} // namespace starrocks::lake

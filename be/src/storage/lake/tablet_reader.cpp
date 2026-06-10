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

#include "storage/lake/tablet_reader.h"

#include <algorithm>
#include <future>
#include <limits>
#include <utility>

#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "column/datum_convert.h"
#include "common/config_ingest_fwd.h"
#include "common/config_json_flat_fwd.h"
#include "common/config_lake_fwd.h"
#include "common/config_scan_io_fwd.h"
#include "common/status.h"
#include "common/thread/threadpool.h"
#include "exec/pipeline/scan/morsel.h"
#include "exec/pipeline/scan/scan_morsel.h"
#include "exec/pipeline/scan/split_scan_morsel.h"
#include "fs/fs_factory.h"
#include "gutil/stl_util.h"
#include "runtime/env/global_env.h"
#include "runtime/runtime_state.h"
#include "storage/aggregate_iterator.h"
#include "storage/base/merge_iterator.h"
#include "storage/base/row_source_mask.h"
#include "storage/chunk_helper.h"
#include "storage/column_predicate_rewriter.h"
#include "storage/conjunctive_predicates.h"
#include "storage/lake/rowset.h"
#include "storage/lake/utils.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/predicate_parser.h"
#include "storage/primitive/empty_iterator.h"
#include "storage/primitive/union_iterator.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/short_key_range_option.h"
#include "storage/seek_range.h"
#include "storage/tablet_schema_map.h"
#include "storage/type_info_allocator_adapter.h"
#include "storage/types.h"
#include "util/json_flattener.h"

namespace starrocks::lake {

using ConjunctivePredicates = starrocks::ConjunctivePredicates;
using Datum = starrocks::Datum;
using Field = starrocks::Field;
using PredicateParser = starrocks::PredicateParser;
using ZonemapPredicatesRewriter = starrocks::ZonemapPredicatesRewriter;

static Schema build_prepare_pruning_schema(const RowsetPtr& rowset, const Schema& read_schema,
                                           const RowsetReadOptions& rowset_read_options,
                                           const DisjunctivePredicates& delete_predicates) {
    Schema prepare_schema = rowset->build_segment_schema(read_schema, rowset_read_options, delete_predicates);
    if (rowset_read_options.tablet_schema == nullptr) {
        return prepare_schema;
    }

    for (ColumnId cid : rowset_read_options.pred_tree_for_zone_map.column_ids()) {
        const TabletColumn& col = rowset_read_options.tablet_schema->column(cid);
        if (prepare_schema.get_field_by_name(std::string(col.name())) != nullptr) {
            continue;
        }
        prepare_schema.append(std::make_shared<Field>(ChunkHelper::convert_field(cid, col)));
    }
    return prepare_schema;
}

static Status prepare_segment_pruned_scan_range(const SegmentPtr& segment, const Schema& schema,
                                                const SegmentReadOptions& segment_read_options,
                                                const PreparedSegmentReadStatePtr& prepared_segment_read_state) {
    if (segment == nullptr || prepared_segment_read_state == nullptr) {
        return Status::InvalidArgument("invalid prepared segment read state");
    }

    auto range_or = get_prepared_pruned_row_ranges(segment, schema, segment_read_options);
    if (!range_or.ok()) {
        return range_or.status();
    }
    prepared_segment_read_state->publish_pruned_scan_range(std::make_shared<SparseRange<>>(std::move(range_or).value()),
                                                           true /* includes_page_filters */);
    return Status::OK();
}

static Status prepare_segment_pruned_scan_range_for_split(
        const RowsetPtr& rowset, size_t segment_idx, const SegmentPtr& segment,
        const PreparedSegmentReadStatePtr& segment_state, const Schema& read_schema,
        const RowsetReadOptions& rowset_read_options, const LakeIOOptions& lake_io_opts,
        const std::optional<SeekRange>& shared_segment_range, OlapReaderStatistics* prepare_stats) {
    const auto delete_predicates = rowset_read_options.delete_predicates != nullptr
                                           ? rowset_read_options.delete_predicates->get_predicates(rowset->index())
                                           : DisjunctivePredicates{};
    SegmentReadOptions segment_read_options;
    RETURN_IF_ERROR(rowset->init_segment_read_options(rowset_read_options, lake_io_opts, delete_predicates,
                                                      prepare_stats, &segment_read_options));
    RETURN_IF_ERROR(rowset->set_segment_tablet_range(segment_idx, shared_segment_range, &segment_read_options));
    std::vector<SeekRange> ranges_to_resolve = segment_read_options.ranges;
    std::optional<size_t> tablet_range_offset;
    if (segment_read_options.tablet_range.has_value() && !segment_read_options.tablet_range->all_range()) {
        tablet_range_offset = ranges_to_resolve.size();
        ranges_to_resolve.emplace_back(segment_read_options.tablet_range.value());
    }
    ASSIGN_OR_RETURN(auto resolved_ranges, segment_seek_ranges_to_rowid_ranges(segment, ranges_to_resolve,
                                                                               segment_read_options.lake_io_opts));
    std::vector<std::optional<Range<rowid_t>>> seek_ranges_rowid_bounds(
            resolved_ranges.begin(), resolved_ranges.begin() + segment_read_options.ranges.size());
    std::optional<Range<rowid_t>> tablet_range_rowid_bounds;
    if (tablet_range_offset.has_value()) {
        tablet_range_rowid_bounds = resolved_ranges[*tablet_range_offset];
    }
    segment_state->publish_rowid_bounds_cache(std::move(seek_ranges_rowid_bounds), tablet_range_rowid_bounds);
    segment_read_options.read_state_cache.seek_range_rowid_ranges = &segment_state->seek_ranges_rowid_bounds;
    segment_read_options.read_state_cache.tablet_rowid_range = &segment_state->tablet_range_rowid_bounds;
    auto prepare_schema = build_prepare_pruning_schema(rowset, read_schema, rowset_read_options, delete_predicates);
    return prepare_segment_pruned_scan_range(segment, prepare_schema, segment_read_options, segment_state);
}

static void append_refined_segment_split_tasks(const RowsetPtr& rowset, const SegmentPtr& segment,
                                               const PreparedTabletReadStatePtr& prepared_tablet_read_state,
                                               const PreparedSegmentReadStatePtr& segment_state, size_t rowset_idx,
                                               size_t segment_idx, const SparseRange<>& refined_scan_range,
                                               rowid_t rows_per_split,
                                               std::vector<pipeline::ScanSplitContextPtr>* split_tasks) {
    if (refined_scan_range.span_size() == 0) {
        return;
    }

    auto range_iter = refined_scan_range.new_iterator();
    bool is_first_split_of_segment = true;
    while (range_iter.has_more()) {
        SparseRange<> split_range;
        range_iter.next_range(rows_per_split, &split_range);
        if (split_range.span_size() == 0) {
            break;
        }

        auto rowid_range = std::make_shared<RowidRangeOption>();
        rowid_range->add(rowset.get(), segment.get(), std::make_shared<SparseRange<>>(std::move(split_range)),
                         is_first_split_of_segment);
        is_first_split_of_segment = false;

        auto ctx = std::make_unique<pipeline::LakeSplitContext>();
        ctx->rowid_range = std::move(rowid_range);
        ctx->rowid_range_source = pipeline::LakeSplitContext::RowidRangeSource::REFINED;
        ctx->prepared_tablet_read_state = prepared_tablet_read_state;
        ctx->prepared_segment_read_state = segment_state;
        ctx->rowset_index = rowset_idx;
        ctx->segment_index = segment_idx;
        split_tasks->emplace_back(std::move(ctx));
    }
}

static StatusOr<SparseRange<>> build_initial_coarse_scan_range(const SegmentPtr& segment,
                                                               const SegmentReadOptions& segment_read_options) {
    const bool need_short_key_index =
            !segment_read_options.ranges.empty() ||
            (segment_read_options.tablet_range.has_value() && !segment_read_options.tablet_range->all_range());
    if (need_short_key_index) {
        RETURN_IF_ERROR(segment->load_index(segment_read_options.lake_io_opts));
    }
    ASSIGN_OR_RETURN(auto scan_range,
                     block_aligned_rowid_range_from_seek_ranges(segment.get(), segment_read_options.ranges));
    if (segment_read_options.tablet_range.has_value() && !segment_read_options.tablet_range->all_range()) {
        ASSIGN_OR_RETURN(auto tablet_scan_range, block_aligned_rowid_range_from_seek_ranges(
                                                         segment.get(), {segment_read_options.tablet_range.value()}));
        scan_range &= tablet_scan_range;
    }
    return scan_range;
}

static SparseRange<> subtract_sparse_ranges(const SparseRange<>& lhs, const SparseRange<>& rhs) {
    SparseRange<> result;
    size_t rhs_idx = 0;
    for (size_t lhs_idx = 0; lhs_idx < lhs.size(); ++lhs_idx) {
        auto cur = lhs[lhs_idx];
        while (rhs_idx < rhs.size() && rhs[rhs_idx].end() <= cur.begin()) {
            ++rhs_idx;
        }
        size_t probe = rhs_idx;
        while (probe < rhs.size() && rhs[probe].begin() < cur.end()) {
            const auto& cut = rhs[probe];
            if (cut.begin() > cur.begin()) {
                result.add(Range<>(cur.begin(), std::min(cur.end(), cut.begin())));
            }
            if (cut.end() >= cur.end()) {
                cur = Range<>(cur.end(), cur.end());
                break;
            }
            cur = Range<>(std::max(cur.begin(), cut.end()), cur.end());
            ++probe;
        }
        if (!cur.empty()) {
            result.add(cur);
        }
    }
    return result;
}

static void init_coarse_split_allocation_state(const PreparedSegmentReadStatePtr& segment_state,
                                               const SparseRange<>& coarse_scan_range) {
    std::lock_guard<std::mutex> guard(segment_state->coarse_range_lock);
    segment_state->coarse_scan_range = coarse_scan_range;
    segment_state->coarse_scan_range_iter = segment_state->coarse_scan_range.new_iterator();
    segment_state->allocated_coarse_ranges.clear();
    segment_state->coarse_split_allocation_closed = false;
    segment_state->clear_pruned_scan_range();
    segment_state->clear_rowid_bounds_cache();
}

static bool allocate_initial_coarse_split(const PreparedSegmentReadStatePtr& segment_state, Rowset* rowset,
                                          Segment* segment, rowid_t rows_per_split, RowidRangeOptionPtr* rowid_range) {
    *rowid_range = nullptr;
    std::lock_guard<std::mutex> guard(segment_state->coarse_range_lock);
    if (segment_state->coarse_split_allocation_closed || !segment_state->coarse_scan_range_iter.has_more()) {
        return false;
    }

    auto result = std::make_shared<RowidRangeOption>();
    size_t num_taken_rows = 0;
    while (segment_state->coarse_scan_range_iter.has_more() && num_taken_rows < rows_per_split) {
        const size_t remaining_rows = segment_state->coarse_scan_range_iter.remaining_rows();
        size_t rows_to_take = std::min<size_t>(rows_per_split - num_taken_rows, remaining_rows);
        if (remaining_rows > rows_to_take && remaining_rows - rows_to_take < rows_per_split) {
            rows_to_take = remaining_rows;
        }

        SparseRange<> taken_range;
        segment_state->coarse_scan_range_iter.next_range(rows_to_take, &taken_range);
        if (taken_range.span_size() == 0) {
            break;
        }
        const bool is_first_split_of_segment = segment_state->allocated_coarse_ranges.span_size() == 0;
        segment_state->allocated_coarse_ranges |= taken_range;
        num_taken_rows += taken_range.span_size();
        result->add(rowset, segment, std::make_shared<SparseRange<>>(std::move(taken_range)),
                    is_first_split_of_segment);
    }

    if (result->rowid_range_per_segment_per_rowset.empty()) {
        return false;
    }
    *rowid_range = std::move(result);
    return true;
}

static rowid_t rows_per_split(const TabletReaderParams& params) {
    return params.splitted_scan_rows > 0 ? static_cast<rowid_t>(std::min<int64_t>(params.splitted_scan_rows,
                                                                                  std::numeric_limits<rowid_t>::max()))
                                         : std::numeric_limits<rowid_t>::max();
}

TabletReader::TabletReader(TabletManager* tablet_mgr, std::shared_ptr<const TabletMetadataPB> metadata, Schema schema)
        : ChunkIterator(std::move(schema)), _tablet_mgr(tablet_mgr), _tablet_metadata(std::move(metadata)) {}

TabletReader::TabletReader(TabletManager* tablet_mgr, std::shared_ptr<const TabletMetadataPB> metadata, Schema schema,
                           bool need_split, bool could_split_physically)
        : ChunkIterator(std::move(schema)),
          _tablet_mgr(tablet_mgr),
          _tablet_metadata(std::move(metadata)),
          _need_split(need_split),
          _could_split_physically(could_split_physically) {}

TabletReader::TabletReader(TabletManager* tablet_mgr, std::shared_ptr<const TabletMetadataPB> metadata, Schema schema,
                           bool need_split, bool could_split_physically, std::vector<RowsetPtr> rowsets)
        : ChunkIterator(std::move(schema)),
          _tablet_mgr(tablet_mgr),
          _tablet_metadata(std::move(metadata)),
          _need_split(need_split),
          _could_split_physically(could_split_physically) {
    if (!rowsets.empty()) {
        _rowsets_inited = true;
        _rowsets = std::move(rowsets);
    }
}

TabletReader::TabletReader(TabletManager* tablet_mgr, std::shared_ptr<const TabletMetadataPB> metadata, Schema schema,
                           std::vector<RowsetPtr> rowsets, std::shared_ptr<const TabletSchema> tablet_schema)
        : ChunkIterator(std::move(schema)),
          _tablet_mgr(tablet_mgr),
          _tablet_metadata(std::move(metadata)),
          _tablet_schema(std::move(tablet_schema)),
          _rowsets_inited(true),
          _rowsets(std::move(rowsets)) {}

TabletReader::TabletReader(TabletManager* tablet_mgr, std::shared_ptr<const TabletMetadataPB> metadata, Schema schema,
                           std::vector<RowsetPtr> rowsets, bool is_key, RowSourceMaskBuffer* mask_buffer,
                           std::shared_ptr<const TabletSchema> tablet_schema)
        : ChunkIterator(std::move(schema)),
          _tablet_mgr(tablet_mgr),
          _tablet_metadata(std::move(metadata)),
          _tablet_schema(std::move(tablet_schema)),
          _rowsets_inited(true),
          _rowsets(std::move(rowsets)),
          _is_vertical_merge(true),
          _is_key(is_key),
          _mask_buffer(mask_buffer) {
    DCHECK(_mask_buffer);
}

TabletReader::~TabletReader() {
    close();
}

Status TabletReader::prepare() {
    if (_tablet_schema == nullptr) {
        _tablet_schema = GlobalTabletSchemaMap::Instance()->emplace(_tablet_metadata->schema()).first;
    }
    if (UNLIKELY(_tablet_schema == nullptr)) {
        return Status::InternalError("failed to construct tablet schema");
    }
    if (!_rowsets_inited) {
        _rowsets = Rowset::get_rowsets(_tablet_mgr, _tablet_metadata);
        _rowsets_inited = true;
    }
    _stats.rowsets_read_count += _rowsets.size();
    return Status::OK();
}

Status TabletReader::open(const TabletReaderParams& read_params) {
    if (read_params.reader_type != ReaderType::READER_QUERY && read_params.reader_type != ReaderType::READER_CHECKSUM &&
        read_params.reader_type != ReaderType::READER_ALTER_TABLE && !is_compaction(read_params.reader_type) &&
        read_params.reader_type != ReaderType::READER_BYPASS_QUERY) {
        return Status::NotSupported("reader type not supported now");
    }
    RETURN_IF_ERROR(init_compaction_column_paths(read_params));
    if (_collect_iter != nullptr) {
        _collect_iter->close();
        _collect_iter.reset();
    }

    if (_need_split) {
        std::vector<BaseTabletSharedPtr> tablets;
        auto tablet_shared_ptr = std::make_shared<Tablet>(_tablet_mgr, _tablet_metadata->id());
        // to avoid list tablet metadata by set version_hint
        tablet_shared_ptr->set_version_hint(_tablet_metadata->version());
        tablets.emplace_back(tablet_shared_ptr);

        std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets;
        tablet_rowsets.emplace_back();
        auto& rss = tablet_rowsets.back();
        int64_t tablet_num_rows = 0;
        for (auto& rowset : _rowsets) {
            tablet_num_rows += rowset->num_rows();
            rss.emplace_back(rowset);
        }

        // Do not split if tablet has no rowsets (empty tablet)
        if (_rowsets.empty()) {
            _need_split = false;
            return init_collector(read_params);
        }

        // not split for data skew between tablet
        if (tablet_num_rows < read_params.splitted_scan_rows * config::lake_tablet_rows_splitted_ratio) {
            // set _need_split false to make iterator can get data this round if split do not happen,
            // otherwise, iterator will return empty.
            _need_split = false;
            return init_collector(read_params);
        }

        if (_could_split_physically && read_params.runtime_state != nullptr &&
            read_params.runtime_state->enable_lake_prepared_physical_split_scan()) {
            auto prepared_tablet_read_state = std::make_shared<PreparedTabletReadState>();
            RETURN_IF_ERROR(build_prepared_tablet_read_state(read_params, prepared_tablet_read_state.get()));
            RETURN_IF_ERROR(build_initial_coarse_split_tasks(read_params, prepared_tablet_read_state));
            if (_split_tasks.empty()) {
                _need_split = false;
                return init_collector(read_params);
            }
            return Status::OK();
        }

        pipeline::Morsels morsels;
        morsels.emplace_back(
                std::make_unique<pipeline::ScanMorsel>(read_params.plan_node_id, *(read_params.scan_range)));

        std::shared_ptr<pipeline::SplitMorselQueue> split_morsel_queue = nullptr;

        if (_could_split_physically) {
            split_morsel_queue = std::make_shared<pipeline::PhysicalSplitMorselQueue>(
                    std::move(morsels), read_params.scan_dop, read_params.splitted_scan_rows);
        } else {
            // logical
            split_morsel_queue = std::make_shared<pipeline::LogicalSplitMorselQueue>(
                    std::move(morsels), read_params.scan_dop, read_params.splitted_scan_rows);
        }

        // do prepare
        split_morsel_queue->set_tablets(tablets);
        split_morsel_queue->set_tablet_rowsets(tablet_rowsets);
        split_morsel_queue->set_key_ranges(read_params.range, read_params.end_range, read_params.start_key,
                                           read_params.end_key);
        split_morsel_queue->set_tablet_schema(_tablet_schema);

        while (true) {
            auto split_status_or = split_morsel_queue->try_get();
            if (UNLIKELY(!split_status_or.ok())) {
                LOG(WARNING) << "failed to get split morsel: " << split_status_or.status()
                             << ", query_id: " << print_id(read_params.runtime_state->query_id())
                             << ", tablet_id: " << tablet_shared_ptr->tablet_id();
                // clear split tasks, and fallback to non-split mode
                _split_tasks.clear();
                _need_split = false;
                return init_collector(read_params);
            }

            auto split = std::move(split_status_or.value());
            if (split != nullptr) {
                auto ctx = std::make_unique<pipeline::LakeSplitContext>();

                if (_could_split_physically) {
                    auto physical_split = dynamic_cast<pipeline::PhysicalSplitScanMorsel*>(split.get());
                    ctx->rowid_range = physical_split->get_rowid_range_option();
                } else {
                    auto logical_split = dynamic_cast<pipeline::LogicalSplitScanMorsel*>(split.get());
                    ctx->short_key_range = logical_split->get_short_key_ranges_option();
                }
                ctx->split_morsel_queue = split_morsel_queue;
                _split_tasks.emplace_back(std::move(ctx));
            } else {
                break;
            }
        }

    } else {
        TabletReaderParams effective_params = read_params;
        if (effective_params.refine_initial_coarse_split_and_append_refined_tasks) {
            RETURN_IF_ERROR(refine_initial_coarse_split_and_append_refined_tasks(effective_params,
                                                                                 &effective_params.rowid_range_option));
        }
        return init_collector(effective_params);
    }

    return Status::OK();
}

Status TabletReader::init_compaction_column_paths(const TabletReaderParams& read_params) {
    if (!config::enable_compaction_flat_json || !is_compaction(read_params.reader_type) ||
        read_params.column_access_paths == nullptr) {
        return Status::OK();
    }

    if (!read_params.column_access_paths->empty()) {
        VLOG(3) << "Lake Compaction flat json paths exists: " << read_params.column_access_paths->size();
        return Status::OK();
    }

    DCHECK(is_compaction(read_params.reader_type) && read_params.column_access_paths != nullptr &&
           read_params.column_access_paths->empty());
    int num_readers = 0;
    for (const auto& rowset : _rowsets) {
        auto segments = rowset->get_segments();
        std::for_each(segments.begin(), segments.end(),
                      [&](const auto& segment) { num_readers += segment->num_rows() > 0 ? 1 : 0; });
    }

    std::vector<const ColumnReader*> readers;
    for (size_t i = 0; i < _tablet_schema->num_columns(); i++) {
        const auto& col = _tablet_schema->column(i);
        auto col_name = std::string(col.name());
        if (_schema.get_field_by_name(col_name) == nullptr || col.type() != LogicalType::TYPE_JSON) {
            continue;
        }
        readers.clear();
        for (const auto& rowset : _rowsets) {
            for (const auto& segment : rowset->get_segments()) {
                if (segment->num_rows() == 0) {
                    continue;
                }
                auto reader = segment->column_with_uid(col.unique_id());
                if (reader != nullptr && reader->column_type() == LogicalType::TYPE_JSON &&
                    nullptr != reader->sub_readers() && !reader->sub_readers()->empty()) {
                    readers.emplace_back(reader);
                }
            }
        }
        if (readers.size() == num_readers) {
            // must all be flat json type
            JsonPathDeriver deriver;

            if (auto metadata = _tablet_mgr->get_latest_cached_tablet_metadata(_tablet_metadata->id());
                metadata && metadata->has_flat_json_config()) {
                auto flat_json_config = std::make_shared<FlatJsonConfig>();
                flat_json_config->update(metadata->flat_json_config());
                deriver.init_flat_json_config(flat_json_config.get());
            }

            deriver.derived(readers);
            auto paths = deriver.flat_paths();
            auto types = deriver.flat_types();
            VLOG(3) << "Lake Compaction flat json column: " << JsonFlatPath::debug_flat_json(paths, types, true);
            ASSIGN_OR_RETURN(auto res, ColumnAccessPath::create(TAccessPathType::ROOT, std::string(col.name()), i));
            for (size_t j = 0; j < paths.size(); j++) {
                ColumnAccessPath::insert_json_path(res.get(), types[j], paths[j]);
            }
            res->set_from_compaction(true);
            read_params.column_access_paths->emplace_back(std::move(res));
        }
    }
    return Status::OK();
}

void TabletReader::close() {
    if (_collect_iter != nullptr) {
        _collect_iter->close();
        _collect_iter.reset();
    }
    for (auto& rowset_iters : _reusable_rowset_iterators) {
        for (auto& iter : rowset_iters) {
            if (iter != nullptr) {
                iter->close();
                iter.reset();
            }
        }
    }
    _reusable_rowset_iterators.clear();
    STLDeleteElements(&_predicate_free_list);
    _rowsets.clear();
    _obj_pool.clear();
}

Status TabletReader::do_get_next(Chunk* chunk) {
    DCHECK(!_is_vertical_merge);
    if (_need_split) {
        // return eof
        return Status::EndOfFile("split morsel");
    }
    RETURN_IF_ERROR(_collect_iter->get_next(chunk));
    return Status::OK();
}

Status TabletReader::do_get_next(Chunk* chunk, std::vector<uint64_t>* rssid_rowids) {
    return _collect_iter->get_next(chunk, rssid_rowids);
}

Status TabletReader::do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks) {
    DCHECK(_is_vertical_merge);
    if (_need_split) {
        // return eof
        return Status::EndOfFile("split morsel");
    }
    RETURN_IF_ERROR(_collect_iter->get_next(chunk, source_masks));
    return Status::OK();
}

Status TabletReader::do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks,
                                 std::vector<uint64_t>* rssid_rowids) {
    DCHECK(_is_vertical_merge);
    RETURN_IF_ERROR(_collect_iter->get_next(chunk, source_masks, rssid_rowids));
    return Status::OK();
}

Status TabletReader::build_prepared_tablet_read_state(const TabletReaderParams& params,
                                                      PreparedTabletReadState* state) {
    if (state == nullptr) {
        return Status::InvalidArgument("prepared tablet read state is null");
    }
    if (!state->rowsets.empty()) {
        return Status::OK();
    }
    if (!_rowsets_inited) {
        _rowsets = Rowset::get_rowsets(_tablet_mgr, _tablet_metadata);
        _rowsets_inited = true;
    }

    std::vector<RowsetPtr> rowsets = _rowsets;
    std::vector<std::vector<SegmentPtr>> rowset_segments;
    std::vector<std::vector<PreparedSegmentReadStatePtr>> rowset_prepared_states;
    rowset_segments.reserve(rowsets.size());
    rowset_prepared_states.reserve(rowsets.size());
    for (const auto& rowset : rowsets) {
        ASSIGN_OR_RETURN(auto segments, rowset->segments(params.lake_io_opts));
        std::vector<PreparedSegmentReadStatePtr> prepared_states;
        prepared_states.reserve(segments.size());
        for (size_t i = 0; i < segments.size(); ++i) {
            prepared_states.emplace_back(std::make_shared<PreparedSegmentReadState>());
        }
        rowset_segments.emplace_back(std::move(segments));
        rowset_prepared_states.emplace_back(std::move(prepared_states));
    }

    state->rowsets = std::move(rowsets);
    state->rowset_segments = std::move(rowset_segments);
    state->rowset_prepared_states = std::move(rowset_prepared_states);
    return Status::OK();
}

Status TabletReader::init_rowset_read_options(const TabletReaderParams& params, RowsetReadOptions* options) {
    if (options == nullptr) {
        return Status::InvalidArgument("rowset read options is null");
    }
    KeysType keys_type = _tablet_schema->keys_type();
    RETURN_IF_ERROR(init_predicates(params));
    RETURN_IF_ERROR(init_delete_predicates(params, &_delete_predicates));
    RETURN_IF_ERROR(parse_seek_range(*_tablet_schema, params.range, params.end_range, params.start_key, params.end_key,
                                     &options->ranges, &_mempool));
    options->pred_tree = params.pred_tree;
    options->runtime_filter_preds = params.runtime_filter_preds;
    RETURN_IF_ERROR(ZonemapPredicatesRewriter::rewrite_predicate_tree(&_obj_pool, options->pred_tree,
                                                                      options->pred_tree_for_zone_map));
    options->sorted = ((keys_type != DUP_KEYS && keys_type != PRIMARY_KEYS) && !params.skip_aggregation) ||
                      is_compaction(params.reader_type) || params.sorted_by_keys_per_tablet;
    options->reader_type = params.reader_type;
    options->chunk_size = params.chunk_size;
    options->delete_predicates = &_delete_predicates;
    options->stats = &_stats;
    options->runtime_state = params.runtime_state;
    options->profile = params.profile;
    options->use_page_cache = params.use_page_cache;
    options->tablet_schema = _tablet_schema;
    options->global_dictmaps = params.global_dictmaps;
    options->unused_output_column_ids = params.unused_output_column_ids;
    options->runtime_range_pruner = params.runtime_range_pruner;
    options->lake_io_opts = params.lake_io_opts;
    options->enable_join_runtime_filter_pushdown = params.enable_join_runtime_filter_pushdown;
    options->prune_column_after_index_filter = params.prune_column_after_index_filter;
    options->enable_gin_filter = params.enable_gin_filter;
    options->enable_predicate_col_late_materialize = params.enable_predicate_col_late_materialize;

    if (keys_type == KeysType::PRIMARY_KEYS) {
        options->is_primary_keys = true;
        options->version = _tablet_metadata->version();
    }
    if (keys_type == PRIMARY_KEYS || keys_type == DUP_KEYS) {
        options->asc_hint = _is_asc_hint;
    }

    options->rowid_range_option = params.rowid_range_option;
    options->short_key_ranges_option = params.short_key_ranges_option;
    options->column_access_paths = params.column_access_paths;
    options->use_vector_index = params.use_vector_index;
    options->vector_search_option = params.vector_search_option;
    options->sample_options = params.sample_options;
    options->has_preaggregation = true;
    if ((is_compaction(params.reader_type) || params.sorted_by_keys_per_tablet)) {
        options->has_preaggregation = true;
    } else if (keys_type == PRIMARY_KEYS || keys_type == DUP_KEYS ||
               (keys_type == UNIQUE_KEYS && params.skip_aggregation)) {
        options->has_preaggregation = false;
    }
    return Status::OK();
}

Status TabletReader::init_rowset_read_options_for_split(const TabletReaderParams& params, RowsetReadOptions* options,
                                                        LakeIOOptions* lake_io_opts) {
    if (lake_io_opts == nullptr) {
        return Status::InvalidArgument("lake io options is null");
    }
    RETURN_IF_ERROR(init_rowset_read_options(params, options));

    *lake_io_opts = params.lake_io_opts;
    if (lake_io_opts->fs == nullptr) {
        auto root_loc = _tablet_mgr->tablet_root_location(_tablet_metadata->id());
        ASSIGN_OR_RETURN(lake_io_opts->fs, FileSystemFactory::CreateSharedFromString(root_loc));
    }
    if (lake_io_opts->location_provider == nullptr) {
        lake_io_opts->location_provider = _tablet_mgr->location_provider();
    }
    options->lake_io_opts = *lake_io_opts;
    return Status::OK();
}

Status TabletReader::build_initial_coarse_split_tasks(const TabletReaderParams& params,
                                                      const PreparedTabletReadStatePtr& prepared_tablet_read_state) {
    if (prepared_tablet_read_state == nullptr) {
        return Status::InvalidArgument("prepared tablet read state is null");
    }
    if (prepared_tablet_read_state->rowsets.size() != prepared_tablet_read_state->rowset_segments.size() ||
        prepared_tablet_read_state->rowsets.size() != prepared_tablet_read_state->rowset_prepared_states.size()) {
        return Status::InvalidArgument("prepared tablet read state has inconsistent rowset state");
    }

    RowsetReadOptions rowset_read_options;
    LakeIOOptions lake_io_opts;
    RETURN_IF_ERROR(init_rowset_read_options_for_split(params, &rowset_read_options, &lake_io_opts));

    OlapReaderStatistics prepare_stats;
    _split_tasks.clear();
    const auto split_rows = rows_per_split(params);
    for (size_t rowset_idx = 0; rowset_idx < prepared_tablet_read_state->rowsets.size(); ++rowset_idx) {
        const auto& rowset = prepared_tablet_read_state->rowsets[rowset_idx];
        if (rowset == nullptr) {
            continue;
        }
        const auto& segments = prepared_tablet_read_state->rowset_segments[rowset_idx];
        const auto& segment_states = prepared_tablet_read_state->rowset_prepared_states[rowset_idx];
        if (segments.size() != segment_states.size()) {
            return Status::InvalidArgument("prepared tablet read state has inconsistent segment state");
        }
        _stats.lake_prepared_rowsets++;
        ASSIGN_OR_RETURN(auto shared_segment_range, rowset->get_seek_range());

        for (size_t segment_idx = 0; segment_idx < segments.size(); ++segment_idx) {
            const auto& segment = segments[segment_idx];
            if (segment == nullptr || segment->num_rows() == 0) {
                continue;
            }
            const auto& segment_state = segment_states[segment_idx];
            const auto delete_predicates =
                    rowset_read_options.delete_predicates != nullptr
                            ? rowset_read_options.delete_predicates->get_predicates(rowset->index())
                            : DisjunctivePredicates{};
            SegmentReadOptions segment_read_options;
            RETURN_IF_ERROR(rowset->init_segment_read_options(rowset_read_options, lake_io_opts, delete_predicates,
                                                              &prepare_stats, &segment_read_options));
            RETURN_IF_ERROR(rowset->set_segment_tablet_range(segment_idx, shared_segment_range, &segment_read_options));
            ASSIGN_OR_RETURN(auto coarse_scan_range, build_initial_coarse_scan_range(segment, segment_read_options));
            if (coarse_scan_range.span_size() == 0) {
                continue;
            }
            init_coarse_split_allocation_state(segment_state, coarse_scan_range);

            RowidRangeOptionPtr seed_rowid_range;
            if (!allocate_initial_coarse_split(segment_state, rowset.get(), segment.get(), split_rows,
                                               &seed_rowid_range)) {
                continue;
            }

            const auto old_num_tasks = _split_tasks.size();
            auto ctx = std::make_unique<pipeline::LakeSplitContext>();
            ctx->rowid_range = std::move(seed_rowid_range);
            ctx->rowid_range_source = pipeline::LakeSplitContext::RowidRangeSource::INITIAL_COARSE;
            ctx->prepared_tablet_read_state = prepared_tablet_read_state;
            ctx->prepared_segment_read_state = segment_state;
            ctx->rowset_index = rowset_idx;
            ctx->segment_index = segment_idx;
            _split_tasks.emplace_back(std::move(ctx));
            _stats.lake_prepared_split_tasks += _split_tasks.size() - old_num_tasks;
        }
    }
    return Status::OK();
}

Status TabletReader::refine_initial_coarse_split_and_append_refined_tasks(const TabletReaderParams& params,
                                                                          RowidRangeOptionPtr* local_rowid_range) {
    if (local_rowid_range == nullptr) {
        return Status::InvalidArgument("local rowid range is null");
    }
    auto seed_rowid_range_option = params.rowid_range_option;
    *local_rowid_range = nullptr;
    if (params.prepared_tablet_read_state == nullptr || params.prepared_segment_read_state == nullptr ||
        params.prepared_rowset_index >= params.prepared_tablet_read_state->rowsets.size() ||
        params.prepared_rowset_index >= params.prepared_tablet_read_state->rowset_segments.size() ||
        params.prepared_rowset_index >= params.prepared_tablet_read_state->rowset_prepared_states.size()) {
        return Status::InvalidArgument("invalid initial coarse split context");
    }

    const auto& prepared_tablet_read_state = params.prepared_tablet_read_state;
    const auto& segment_state = params.prepared_segment_read_state;
    const auto rowset_idx = params.prepared_rowset_index;
    const auto segment_idx = params.prepared_segment_index;
    if (segment_idx >= prepared_tablet_read_state->rowset_segments[rowset_idx].size() ||
        segment_idx >= prepared_tablet_read_state->rowset_prepared_states[rowset_idx].size() ||
        prepared_tablet_read_state->rowset_prepared_states[rowset_idx][segment_idx] != segment_state) {
        return Status::InvalidArgument("initial coarse split context does not match prepared state");
    }

    const auto& rowset = prepared_tablet_read_state->rowsets[rowset_idx];
    const auto& segment = prepared_tablet_read_state->rowset_segments[rowset_idx][segment_idx];
    if (rowset == nullptr || segment == nullptr || segment->num_rows() == 0) {
        *local_rowid_range = std::make_shared<RowidRangeOption>();
        return Status::OK();
    }

    TabletReaderParams prepare_params = params;
    // The seed morsel carries only one coarse split. Segment prepare must compute the
    // final pruned range for the whole segment so refined children can cover the rest.
    prepare_params.rowid_range_option = nullptr;

    RowsetReadOptions rowset_read_options;
    LakeIOOptions lake_io_opts;
    RETURN_IF_ERROR(init_rowset_read_options_for_split(prepare_params, &rowset_read_options, &lake_io_opts));

    OlapReaderStatistics prepare_stats;
    ASSIGN_OR_RETURN(auto shared_segment_range, rowset->get_seek_range());
    auto st = prepare_segment_pruned_scan_range_for_split(rowset, segment_idx, segment, segment_state, schema(),
                                                          rowset_read_options, lake_io_opts, shared_segment_range,
                                                          &prepare_stats);
    {
        std::lock_guard<std::mutex> guard(segment_state->coarse_range_lock);
        segment_state->coarse_split_allocation_closed = true;
    }
    RETURN_IF_ERROR(st);

    SparseRange<> pruned_scan_range;
    if (segment_state->pruned_scan_range != nullptr) {
        pruned_scan_range = *segment_state->pruned_scan_range;
        _stats.lake_prepared_scan_rows += segment_state->pruned_scan_range->span_size();
        _stats.lake_prepared_scan_ranges += segment_state->pruned_scan_range->size();
    } else {
        pruned_scan_range.add(Range<>(0, segment->num_rows()));
    }
    if (pruned_scan_range.span_size() == 0) {
        *local_rowid_range = std::make_shared<RowidRangeOption>();
        return Status::OK();
    }

    if (seed_rowid_range_option != nullptr) {
        auto seed_split = seed_rowid_range_option->get_segment_rowid_range(rowset.get(), segment.get());
        if (seed_split.row_id_range != nullptr) {
            SparseRange<> seed_scan_range = *seed_split.row_id_range;
            seed_scan_range &= pruned_scan_range;
            if (seed_scan_range.span_size() > 0) {
                auto rowid_range = std::make_shared<RowidRangeOption>();
                rowid_range->add(rowset.get(), segment.get(),
                                 std::make_shared<SparseRange<>>(std::move(seed_scan_range)),
                                 seed_split.is_first_split_of_segment);
                *local_rowid_range = std::move(rowid_range);
            }
        }
    }

    SparseRange<> allocated_coarse_ranges;
    {
        std::lock_guard<std::mutex> guard(segment_state->coarse_range_lock);
        allocated_coarse_ranges = segment_state->allocated_coarse_ranges;
    }
    SparseRange<> remaining_scan_range = subtract_sparse_ranges(pruned_scan_range, allocated_coarse_ranges);
    const auto old_num_tasks = _split_tasks.size();
    append_refined_segment_split_tasks(rowset, segment, prepared_tablet_read_state, segment_state, rowset_idx,
                                       segment_idx, remaining_scan_range, rows_per_split(params), &_split_tasks);
    _stats.lake_prepared_segments++;
    _stats.lake_prepared_split_tasks += _split_tasks.size() - old_num_tasks;

    if (*local_rowid_range == nullptr) {
        // Keep this seed child as an empty read if its coarse range was fully pruned.
        *local_rowid_range = std::make_shared<RowidRangeOption>();
    }
    return Status::OK();
}

// TODO: support
//  1. rowid range and short key range
Status TabletReader::get_segment_iterators(const TabletReaderParams& params, std::vector<ChunkIteratorPtr>* iters) {
    RowsetReadOptions rs_opts;
    RETURN_IF_ERROR(init_rowset_read_options(params, &rs_opts));

    SCOPED_RAW_TIMER(&_stats.create_segment_iter_ns);

    const auto& prepared_tablet_read_state = params.prepared_tablet_read_state;
    const bool use_prepared_segments = prepared_tablet_read_state != nullptr;
    if (use_prepared_segments &&
        (prepared_tablet_read_state->rowsets.size() != _rowsets.size() ||
         prepared_tablet_read_state->rowset_segments.size() != prepared_tablet_read_state->rowsets.size() ||
         prepared_tablet_read_state->rowset_prepared_states.size() != prepared_tablet_read_state->rowsets.size())) {
        return Status::InvalidArgument("prepared tablet read state does not match reader rowsets");
    }
    const bool use_prepared_segment_target = use_prepared_segments && params.prepared_segment_read_state != nullptr;
    if (use_prepared_segment_target) {
        if (params.prepared_rowset_index >= _rowsets.size()) {
            return Status::InvalidArgument("prepared target rowset index exceeds reader rowsets");
        }
        const auto rowset_idx = params.prepared_rowset_index;
        const auto segment_idx = params.prepared_segment_index;
        auto& rowset = _rowsets[rowset_idx];
        const auto& prepared_rowset = prepared_tablet_read_state->rowsets[rowset_idx];
        if (prepared_rowset == nullptr || prepared_rowset->rowset_id() != rowset->rowset_id()) {
            return Status::InvalidArgument("prepared tablet read state rowset order does not match reader rowsets");
        }
        const auto& prepared_segments = prepared_tablet_read_state->rowset_segments[rowset_idx];
        const auto& prepared_segment_states = prepared_tablet_read_state->rowset_prepared_states[rowset_idx];
        if (segment_idx >= prepared_segments.size() || segment_idx >= prepared_segment_states.size()) {
            return Status::InvalidArgument("prepared target segment index exceeds prepared segment state");
        }
        if (prepared_segment_states[segment_idx] != params.prepared_segment_read_state) {
            return Status::InvalidArgument("prepared target segment state does not match split context");
        }
        if (_reusable_rowset_iterators.size() < _rowsets.size()) {
            _reusable_rowset_iterators.resize(_rowsets.size());
        }
        ASSIGN_OR_RETURN(auto seg_iters,
                         enhance_error_prompt(rowset->read_prepared_segment(
                                 schema(), rs_opts, prepared_segments, segment_idx, params.prepared_segment_read_state,
                                 &_reusable_rowset_iterators[rowset_idx])));
        iters->insert(iters->end(), seg_iters.begin(), seg_iters.end());
        return Status::OK();
    }
    if (use_prepared_segments) {
        return Status::InvalidArgument("prepared tablet read state requires a prepared segment target");
    }

    std::vector<std::future<StatusOr<std::vector<ChunkIteratorPtr>>>> futures;
    // The parallel tasks capture local variables and |this| by reference.
    // Must wait for all tasks to complete before returning, otherwise
    // dangling references to |rs_opts| and |this| cause use-after-free.
    DeferOp wait_futures([&futures]() {
        for (auto& future : futures) {
            if (future.valid()) {
                future.wait();
            }
        }
    });
    for (size_t rowset_idx = 0; rowset_idx < _rowsets.size(); ++rowset_idx) {
        auto& rowset = _rowsets[rowset_idx];
        if (params.rowid_range_option != nullptr && !params.rowid_range_option->contains_rowset(rowset.get())) {
            continue;
        }

        if (config::enable_load_segment_parallel) {
            auto task = std::make_shared<std::packaged_task<StatusOr<std::vector<ChunkIteratorPtr>>()>>(
                    [&, rowset_idx, rowset]() {
#ifdef BE_TEST
                        Status injected_st;
                        TEST_SYNC_POINT_CALLBACK("TabletReader::get_segment_iterators::parallel_read", &injected_st);
                        if (!injected_st.ok()) {
                            return StatusOr<std::vector<ChunkIteratorPtr>>(injected_st);
                        }
#endif
                        return enhance_error_prompt(rowset->read(schema(), rs_opts));
                    });

            auto packaged_func = [task]() { (*task)(); };
            if (auto st = GlobalEnv::GetInstance()->load_rowset_thread_pool()->submit_func(std::move(packaged_func));
                !st.ok()) {
                // try load rowset serially if sumbit_func failed
                LOG(WARNING) << "sumbit_func failed: " << st.code_as_string()
                             << ", try to load rowset serially, rowset_id: " << rowset->id();
                ASSIGN_OR_RETURN(auto seg_iters, enhance_error_prompt(rowset->read(schema(), rs_opts)));
                iters->insert(iters->end(), seg_iters.begin(), seg_iters.end());
            } else {
                futures.push_back(task->get_future());
            }
        } else {
            ASSIGN_OR_RETURN(auto seg_iters, enhance_error_prompt(rowset->read(schema(), rs_opts)));
            iters->insert(iters->end(), seg_iters.begin(), seg_iters.end());
        }
    }

    for (auto& future : futures) {
        auto seg_iters_or = future.get();
        if (!seg_iters_or.ok()) {
            return seg_iters_or.status();
        }
        iters->insert(iters->end(), seg_iters_or.value().begin(), seg_iters_or.value().end());
    }
    return Status::OK();
}

Status TabletReader::init_predicates(const TabletReaderParams& params) {
    return Status::OK();
}

Status TabletReader::init_delete_predicates(const TabletReaderParams& params, DeletePredicates* dels) {
    if (UNLIKELY(_tablet_metadata == nullptr)) {
        return Status::InternalError("tablet metadata is null. forget or fail to call prepare()");
    }
    if (UNLIKELY(_tablet_schema == nullptr)) {
        return Status::InternalError("tablet schema is null. forget or fail to call prepare()");
    }
    OlapPredicateParser pred_parser(_tablet_schema);

    for (int index = 0, size = _tablet_metadata->rowsets_size(); index < size; ++index) {
        const auto& rowset_metadata = _tablet_metadata->rowsets(index);
        if (!rowset_metadata.has_delete_predicate()) {
            continue;
        }

        const auto& pred_pb = rowset_metadata.delete_predicate();
        std::vector<TCondition> conds;
        for (int i = 0; i < pred_pb.binary_predicates_size(); ++i) {
            const auto& binary_predicate = pred_pb.binary_predicates(i);
            TCondition cond;
            cond.__set_column_name(binary_predicate.column_name());
            auto& op = binary_predicate.op();
            if (op == "<") {
                cond.__set_condition_op("<<");
            } else if (op == ">") {
                cond.__set_condition_op(">>");
            } else {
                cond.__set_condition_op(op);
            }
            cond.condition_values.emplace_back(binary_predicate.value());
            conds.emplace_back(std::move(cond));
        }

        for (int i = 0; i < pred_pb.is_null_predicates_size(); ++i) {
            const auto& is_null_predicate = pred_pb.is_null_predicates(i);
            TCondition cond;
            cond.__set_column_name(is_null_predicate.column_name());
            cond.__set_condition_op("IS");
            cond.condition_values.emplace_back(is_null_predicate.is_not_null() ? "NOT NULL" : "NULL");
            conds.emplace_back(std::move(cond));
        }

        for (int i = 0; i < pred_pb.in_predicates_size(); ++i) {
            TCondition cond;
            const InPredicatePB& in_predicate = pred_pb.in_predicates(i);
            cond.__set_column_name(in_predicate.column_name());
            if (in_predicate.is_not_in()) {
                cond.__set_condition_op("!*=");
            } else {
                cond.__set_condition_op("*=");
            }
            for (const auto& value : in_predicate.values()) {
                cond.condition_values.push_back(value);
            }
            conds.emplace_back(std::move(cond));
        }

        ConjunctivePredicates conjunctions;
        for (const auto& cond : conds) {
            auto pred_or = pred_parser.parse_thrift_cond(cond);
            if (!pred_or.ok()) {
                if (LIKELY(!config::lake_tablet_ignore_invalid_delete_predicate)) {
                    return pred_or.status();
                } else {
                    LOG(WARNING) << "failed to parse delete condition.column_name[" << cond.column_name
                                 << "], condition_op[" << cond.condition_op << "], condition_values["
                                 << (cond.condition_values.empty() ? "<empty>" : cond.condition_values[0]) << "].";
                    continue;
                }
            }
            conjunctions.add(pred_or.value());
            // save for memory release.
            _predicate_free_list.emplace_back(pred_or.value());
        }
        if (!conjunctions.empty()) {
            dels->add(index, conjunctions);
        }
    }

    return Status::OK();
}

Status TabletReader::init_collector(const TabletReaderParams& params) {
    std::vector<ChunkIteratorPtr> seg_iters;
    RETURN_IF_ERROR(get_segment_iterators(params, &seg_iters));

    // Put each SegmentIterator into a TimedChunkIterator, if a profile is provided.
    if (params.profile != nullptr) {
        RuntimeProfile::Counter* scan_timer = params.profile->total_time_counter();
        std::vector<ChunkIteratorPtr> children;
        children.reserve(seg_iters.size());
        for (auto& seg_iter : seg_iters) {
            children.emplace_back(timed_chunk_iterator(seg_iter, scan_timer));
        }
        seg_iters.swap(children);
    }

    // If |keys_type| is UNIQUE_KEYS and |params.skip_aggregation| is true, must disable aggregate totally.
    // If |keys_type| is AGG_KEYS and |params.skip_aggregation| is true, aggregate is an optional operation.
    KeysType keys_type = _tablet_schema->keys_type();
    const auto skip_aggr = params.skip_aggregation;
    const auto select_all_keys = _schema.num_key_fields() == _tablet_schema->num_key_columns();
    DCHECK_LE(_schema.num_key_fields(), _tablet_schema->num_key_columns());

    if (seg_iters.empty()) {
        _collect_iter = new_empty_iterator(_schema, params.chunk_size);
    } else if (is_compaction(params.reader_type) && (keys_type == DUP_KEYS || keys_type == PRIMARY_KEYS)) {
        //             MergeIterator
        //                   |
        //       +-----------+-----------+
        //       |           |           |
        //     Timer        ...        Timer
        //       |           |           |
        // SegmentIterator  ...    SegmentIterator
        //
        if (_is_vertical_merge && !_is_key) {
            _collect_iter = new_mask_merge_iterator(seg_iters, _mask_buffer);
        } else {
            _collect_iter = new_heap_merge_iterator(
                    seg_iters,
                    (keys_type == PRIMARY_KEYS) && StorageEngine::instance()->enable_light_pk_compaction_publish());
        }
    } else if (params.sorted_by_keys_per_tablet && (keys_type == DUP_KEYS || keys_type == PRIMARY_KEYS) &&
               seg_iters.size() > 1) {
        if (params.profile != nullptr && (params.is_pipeline || params.profile->parent() != nullptr)) {
            RuntimeProfile* p;
            if (params.is_pipeline) {
                p = params.profile;
            } else {
                p = params.profile->parent()->create_child("MERGE", true, true);
            }
            RuntimeProfile::Counter* sort_timer = ADD_TIMER(p, "Sort");
            _collect_iter = new_heap_merge_iterator(seg_iters);
            _collect_iter = timed_chunk_iterator(_collect_iter, sort_timer);
        } else {
            _collect_iter = new_heap_merge_iterator(seg_iters);
        }
    } else if (keys_type == PRIMARY_KEYS || keys_type == DUP_KEYS || (keys_type == UNIQUE_KEYS && skip_aggr) ||
               (select_all_keys && seg_iters.size() == 1)) {
        if (!_is_asc_hint) {
            std::reverse(seg_iters.begin(), seg_iters.end());
        }
        //             UnionIterator
        //                   |
        //       +-----------+-----------+
        //       |           |           |
        //     Timer        ...        Timer
        //       |           |           |
        // SegmentIterator  ...    SegmentIterator
        //
        _collect_iter = new_union_iterator(std::move(seg_iters));
    } else if ((keys_type == AGG_KEYS || keys_type == UNIQUE_KEYS) && !skip_aggr) {
        //                 Timer
        //                   |
        //           AggregateIterator (factor = 0)
        //                   |
        //                 Timer
        //                   |
        //             MergeIterator
        //                   |
        //       +-----------+-----------+
        //       |           |           |
        //     Timer        ...        Timer
        //       |           |           |
        // SegmentIterator  ...    SegmentIterator
        //
        if (params.profile != nullptr && (params.is_pipeline || params.profile->parent() != nullptr)) {
            RuntimeProfile* p;
            if (params.is_pipeline) {
                p = params.profile;
            } else {
                p = params.profile->parent()->create_child("MERGE", true, true);
            }
            RuntimeProfile::Counter* sort_timer = ADD_TIMER(p, "Sort");
            RuntimeProfile::Counter* aggr_timer = ADD_TIMER(p, "Aggr");

            if (_is_vertical_merge && !_is_key) {
                _collect_iter = new_mask_merge_iterator(seg_iters, _mask_buffer);
            } else {
                _collect_iter = new_heap_merge_iterator(seg_iters);
            }
            _collect_iter = timed_chunk_iterator(_collect_iter, sort_timer);
            if (!_is_vertical_merge) {
                _collect_iter = new_aggregate_iterator(std::move(_collect_iter), 0);
            } else {
                _collect_iter = new_aggregate_iterator(std::move(_collect_iter), _is_key);
            }
            _collect_iter = timed_chunk_iterator(_collect_iter, aggr_timer);
        } else {
            if (_is_vertical_merge && !_is_key) {
                _collect_iter = new_mask_merge_iterator(seg_iters, _mask_buffer);
            } else {
                _collect_iter = new_heap_merge_iterator(seg_iters);
            }
            if (!_is_vertical_merge) {
                _collect_iter = new_aggregate_iterator(std::move(_collect_iter), 0);
            } else {
                _collect_iter = new_aggregate_iterator(std::move(_collect_iter), _is_key);
            }
        }
    } else if (keys_type == AGG_KEYS) {
        CHECK(skip_aggr);
        //                 Timer
        //                   |
        //          AggregateIterator (factor = config::pre_aggregate_factor)
        //                   |
        //                 Timer
        //                   |
        //             UnionIterator
        //                   |
        //       +-----------+-----------+
        //       |           |           |
        //     Timer        ...        Timer
        //       |           |           |
        // SegmentIterator  ...    SegmentIterator
        //
        int f = config::pre_aggregate_factor;
        if (params.profile != nullptr && (params.is_pipeline || params.profile->parent() != nullptr)) {
            RuntimeProfile* p;
            if (params.is_pipeline) {
                p = params.profile;
            } else {
                p = params.profile->parent()->create_child("MERGE", true, true);
            }
            RuntimeProfile::Counter* union_timer = ADD_TIMER(p, "Union");
            RuntimeProfile::Counter* aggr_timer = ADD_TIMER(p, "Aggr");

            _collect_iter = new_union_iterator(std::move(seg_iters));
            _collect_iter = timed_chunk_iterator(_collect_iter, union_timer);
            _collect_iter = new_aggregate_iterator(std::move(_collect_iter), f);
            _collect_iter = timed_chunk_iterator(_collect_iter, aggr_timer);
        } else {
            _collect_iter = new_union_iterator(std::move(seg_iters));
            _collect_iter = new_aggregate_iterator(std::move(_collect_iter), f);
        }
    } else {
        return Status::InternalError("Unknown keys type");
    }

    if (_collect_iter != nullptr) {
        RETURN_IF_ERROR(_collect_iter->init_encoded_schema(*params.global_dictmaps));
        RETURN_IF_ERROR(_collect_iter->init_output_schema(*params.unused_output_column_ids));
    }

    return Status::OK();
}

// convert an OlapTuple to SeekTuple.
Status TabletReader::to_seek_tuple(const TabletSchema& tablet_schema, const OlapTuple& input, SeekTuple* tuple,
                                   MemPool* mempool) {
    const TypeInfoAllocator* allocator = nullptr;
    TypeInfoAllocator type_info_allocator;
    if (mempool != nullptr) {
        type_info_allocator = make_type_info_allocator(mempool);
        allocator = &type_info_allocator;
    }

    Schema schema;
    std::vector<Datum> values;
    values.reserve(input.size());
    const auto& sort_key_idxes = tablet_schema.sort_key_idxes();
    DCHECK(sort_key_idxes.empty() || sort_key_idxes.size() >= input.size());

    if (sort_key_idxes.size() > 0) {
        for (auto idx : sort_key_idxes) {
            schema.append_sort_key_idx(idx);
        }
    }

    for (size_t i = 0; i < input.size(); i++) {
        int idx = sort_key_idxes.empty() ? i : sort_key_idxes[i];
        auto f = std::make_shared<Field>(ChunkHelper::convert_field(idx, tablet_schema.column(idx)));
        schema.append(f);
        values.emplace_back();
        if (input.is_null(i)) {
            continue;
        }
        // If the type of the storage level is CHAR,
        // we treat it as VARCHAR, because the execution level CHAR is VARCHAR
        // CHAR type strings are truncated at the storage level after '\0'.
        if (f->type()->type() == TYPE_CHAR) {
            RETURN_IF_ERROR(datum_from_string(get_type_info(TYPE_VARCHAR).get(), &values.back(), input.get_value(i),
                                              allocator));
        } else {
            RETURN_IF_ERROR(datum_from_string(f->type().get(), &values.back(), input.get_value(i), allocator));
        }
    }
    *tuple = SeekTuple(std::move(schema), std::move(values));
    return Status::OK();
}

// convert vector<OlapTuple> to vector<SeekRange>
Status TabletReader::parse_seek_range(const TabletSchema& tablet_schema,
                                      TabletReaderParams::RangeStartOperation range_start_op,
                                      TabletReaderParams::RangeEndOperation range_end_op,
                                      const std::vector<OlapTuple>& range_start_key,
                                      const std::vector<OlapTuple>& range_end_key, std::vector<SeekRange>* ranges,
                                      MemPool* mempool) {
    if (range_start_key.empty()) {
        return {};
    }

    bool lower_inclusive = range_start_op == TabletReaderParams::RangeStartOperation::GE ||
                           range_start_op == TabletReaderParams::RangeStartOperation::EQ;
    bool upper_inclusive = range_end_op == TabletReaderParams::RangeEndOperation::LE ||
                           range_end_op == TabletReaderParams::RangeEndOperation::EQ;

    CHECK_EQ(range_start_key.size(), range_end_key.size());
    size_t n = range_start_key.size();
    ranges->reserve(n);
    for (size_t i = 0; i < n; i++) {
        SeekTuple lower;
        SeekTuple upper;
        RETURN_IF_ERROR(to_seek_tuple(tablet_schema, range_start_key[i], &lower, mempool));
        RETURN_IF_ERROR(to_seek_tuple(tablet_schema, range_end_key[i], &upper, mempool));
        ranges->emplace_back(std::move(lower), std::move(upper));
        ranges->back().set_inclusive_lower(lower_inclusive);
        ranges->back().set_inclusive_upper(upper_inclusive);
    }
    return Status::OK();
}

} // namespace starrocks::lake

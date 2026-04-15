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

#include <future>
#include <set>
#include <utility>

#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "column/datum_convert.h"
#include "common/config_ingest_fwd.h"
#include "common/config_json_flat_fwd.h"
#include "common/config_lake_fwd.h"
#include "common/config_scan_io_fwd.h"
#include "common/status.h"
#include "fs/fs_factory.h"
#include "gutil/stl_util.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/aggregate_iterator.h"
#include "storage/chunk_helper.h"
#include "storage/column_predicate_rewriter.h"
#include "storage/conjunctive_predicates.h"
#include "storage/empty_iterator.h"
#include "storage/lake/rowset.h"
#include "storage/lake/utils.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/merge_iterator.h"
#include "storage/predicate_parser.h"
#include "storage/row_source_mask.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/short_key_range_option.h"
#include "storage/seek_range.h"
#include "storage/tablet_schema_map.h"
#include "storage/type_info_allocator_adapter.h"
#include "storage/types.h"
#include "storage/union_iterator.h"
#include "util/json_flattener.h"

namespace starrocks::lake {

using ConjunctivePredicates = starrocks::ConjunctivePredicates;
using Datum = starrocks::Datum;
using Field = starrocks::Field;
using PredicateParser = starrocks::PredicateParser;
using ZonemapPredicatesRewriter = starrocks::ZonemapPredicatesRewriter;

namespace {

constexpr size_t kMinStaticPrepareFanout = 2;

Schema build_static_pruning_schema(const TabletSchemaCSPtr& tablet_schema, const PredicateTree& pred_tree_for_zone_map,
                                   const DisjunctivePredicates& delete_predicates) {
    std::set<ColumnId> column_ids;
    if (tablet_schema != nullptr) {
        for (ColumnId cid = 0; cid < tablet_schema->num_columns(); ++cid) {
            if (pred_tree_for_zone_map.contains_column(cid)) {
                column_ids.emplace(cid);
            }
        }
    }
    delete_predicates.get_column_ids(&column_ids);
    if (column_ids.empty()) {
        return Schema();
    }
    return ChunkHelper::convert_schema(tablet_schema, std::vector<ColumnId>(column_ids.begin(), column_ids.end()));
}

class LakeIndexPrunedPhysicalSplitPlanner {
public:
    struct Stats {
        size_t visited_segments = 0;
        size_t nonempty_segments = 0;
        size_t fully_pruned_segments = 0;
        size_t static_prepared_segments = 0;
        size_t matched_rows = 0;
        size_t flushed_split_tasks = 0;
    };

    LakeIndexPrunedPhysicalSplitPlanner(TabletSchemaCSPtr tablet_schema, int64_t tablet_id, bool is_asc_hint,
                                        OlapReaderStatistics* stats, bool allow_static_prepare,
                                        const TabletReaderParams& read_params,
                                        const RowsetReadOptions& rs_opts, const LakeIOOptions& lake_io_opts,
                                        TabletReader::PreparedReadState* prepared_state)
            : _tablet_schema(std::move(tablet_schema)),
              _tablet_id(tablet_id),
              _is_asc_hint(is_asc_hint),
              _reader_stats(stats),
              _allow_static_prepare(allow_static_prepare),
              _read_params(read_params),
              _rs_opts(rs_opts),
              _lake_io_opts(lake_io_opts),
              _prepared_state(prepared_state) {}

    Status build(std::vector<pipeline::ScanSplitContextPtr>* split_tasks) {
        split_tasks->clear();
        for (size_t rowset_idx = 0; rowset_idx < _prepared_state->rowsets.size(); ++rowset_idx) {
            auto& rowset = _prepared_state->rowsets[rowset_idx];
            const auto& segments = _prepared_state->rowset_segments[rowset_idx];
            auto& pruning_states = _prepared_state->rowset_pruning_states[rowset_idx];
            const auto delete_preds = _rs_opts.delete_predicates != nullptr
                                              ? _rs_opts.delete_predicates->get_predicates(rowset_idx)
                                              : DisjunctivePredicates{};
            const Schema segment_schema =
                    build_static_pruning_schema(_tablet_schema, _rs_opts.pred_tree_for_zone_map, delete_preds);

            for (size_t segment_idx = 0; segment_idx < segments.size(); ++segment_idx) {
                const auto& segment = segments[segment_idx];
                ++_plan_stats.visited_segments;
                if (segment == nullptr || segment->num_rows() == 0) {
                    continue;
                }
                ++_plan_stats.nonempty_segments;

                SegmentReadOptions seg_options;
                RETURN_IF_ERROR(_init_segment_options(rowset, segment_idx, delete_preds, &seg_options));
                auto& pruning_state = pruning_states[segment_idx];

                SparseRangePtr key_pruned_range;
                RETURN_IF_ERROR(prepare_segment_key_pruned_scan_range(segment, seg_options, pruning_state, &key_pruned_range));
                if (key_pruned_range == nullptr) {
                    key_pruned_range = std::make_shared<SparseRange<>>(Range<>(0, segment->num_rows()));
                }

                SparseRangePtr final_scan_range = key_pruned_range;
                if (_should_prepare_static_range(seg_options, key_pruned_range->span_size())) {
                    RETURN_IF_ERROR(prepare_segment_static_pruned_scan_range(segment_schema, segment, seg_options,
                                                                            pruning_state, &final_scan_range));
                    ++_plan_stats.static_prepared_segments;
                }

                if (final_scan_range == nullptr || final_scan_range->span_size() == 0) {
                    ++_plan_stats.fully_pruned_segments;
                    continue;
                }
                _plan_stats.matched_rows += final_scan_range->span_size();
                _append_segment_scan_range(rowset.get(), segment.get(), *final_scan_range, split_tasks);
            }
        }
        _flush_pending_task(split_tasks);
        return Status::OK();
    }

    const Stats& stats() const { return _plan_stats; }

private:
    Status _init_segment_options(const TabletReader::RowsetPtr& rowset, size_t segment_idx,
                                 const DisjunctivePredicates& delete_preds, SegmentReadOptions* seg_options) const {
        seg_options->fs = _lake_io_opts.fs;
        seg_options->stats = _reader_stats;
        seg_options->ranges = _rs_opts.ranges;
        seg_options->tablet_range = std::nullopt;
        seg_options->pred_tree = _rs_opts.pred_tree;
        seg_options->pred_tree_for_zone_map = _rs_opts.pred_tree_for_zone_map;
        seg_options->delete_predicates = delete_preds;
        seg_options->use_page_cache = _rs_opts.use_page_cache;
        seg_options->profile = _rs_opts.profile;
        seg_options->global_dictmaps = _rs_opts.global_dictmaps;
        seg_options->runtime_range_pruner = _rs_opts.runtime_range_pruner;
        seg_options->tablet_schema = _tablet_schema;
        seg_options->lake_io_opts = _lake_io_opts;
        seg_options->asc_hint = _is_asc_hint;
        seg_options->tablet_id = _tablet_id;
        seg_options->rowset_id = rowset->metadata().id();
        seg_options->rowsetid = rowset->rowset_id();
        seg_options->reader_type = _read_params.reader_type;
        seg_options->chunk_size = _read_params.chunk_size;
        seg_options->enable_join_runtime_filter_pushdown = _rs_opts.enable_join_runtime_filter_pushdown;
        seg_options->enable_predicate_col_late_materialize = _rs_opts.enable_predicate_col_late_materialize;
        seg_options->column_access_paths = _read_params.column_access_paths;
        seg_options->is_first_split_of_segment = true;

        ASSIGN_OR_RETURN(auto shared_segment_range, rowset->get_seek_range());
        if (segment_idx < static_cast<size_t>(rowset->metadata().shared_segments_size()) &&
            rowset->metadata().shared_segments(segment_idx) && shared_segment_range.has_value()) {
            seg_options->tablet_range = *shared_segment_range;
        }
        return Status::OK();
    }

    bool _should_prepare_static_range(const SegmentReadOptions& seg_options, size_t key_pruned_rows) const {
        if (!_allow_static_prepare) {
            return false;
        }
        if (key_pruned_rows < _read_params.splitted_scan_rows * kMinStaticPrepareFanout) {
            return false;
        }
        return seg_options.tablet_range.has_value() || !seg_options.pred_tree_for_zone_map.empty();
    }

    void _append_segment_scan_range(Rowset* rowset, Segment* segment, const SparseRange<>& segment_scan_range,
                                    std::vector<pipeline::ScanSplitContextPtr>* split_tasks) {
        bool is_first_split_of_segment = true;
        auto range_iter = segment_scan_range.new_iterator();
        while (range_iter.has_more()) {
            if (_rowid_range == nullptr) {
                _rowid_range = std::make_shared<RowidRangeOption>();
                _num_taken_rows = 0;
            }

            size_t remaining_in_segment = range_iter.remaining_rows();
            size_t rows_to_take = std::min<size_t>(_read_params.splitted_scan_rows - _num_taken_rows, remaining_in_segment);
            if (remaining_in_segment > rows_to_take &&
                remaining_in_segment - rows_to_take < _read_params.splitted_scan_rows) {
                rows_to_take = remaining_in_segment;
            }

            SparseRange<> taken_range;
            range_iter.next_range(rows_to_take, &taken_range);
            if (taken_range.span_size() == 0) {
                break;
            }

            _num_taken_rows += taken_range.span_size();
            _rowid_range->add(rowset, segment, std::make_shared<SparseRange<>>(std::move(taken_range)),
                              is_first_split_of_segment);
            is_first_split_of_segment = false;

            if (_num_taken_rows >= _read_params.splitted_scan_rows) {
                _flush_pending_task(split_tasks);
            }
        }
    }

    void _flush_pending_task(std::vector<pipeline::ScanSplitContextPtr>* split_tasks) {
        if (_rowid_range == nullptr) {
            return;
        }
        auto ctx = std::make_unique<pipeline::LakeSplitContext>();
        ctx->rowid_range = std::move(_rowid_range);
        split_tasks->emplace_back(std::move(ctx));
        ++_plan_stats.flushed_split_tasks;
        _num_taken_rows = 0;
    }

    TabletSchemaCSPtr _tablet_schema;
    int64_t _tablet_id;
    bool _is_asc_hint;
    OlapReaderStatistics* _reader_stats;
    bool _allow_static_prepare;
    const TabletReaderParams& _read_params;
    const RowsetReadOptions& _rs_opts;
    const LakeIOOptions& _lake_io_opts;
    TabletReader::PreparedReadState* _prepared_state;
    Stats _plan_stats;
    RowidRangeOptionPtr _rowid_range;
    size_t _num_taken_rows = 0;
};

} // namespace

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

        pipeline::Morsels morsels;
        morsels.emplace_back(
                std::make_unique<pipeline::ScanMorsel>(read_params.plan_node_id, *(read_params.scan_range)));

        std::shared_ptr<pipeline::SplitMorselQueue> split_morsel_queue = nullptr;

        if (_could_split_physically) {
            if (config::enable_lake_index_pruned_physical_split) {
                auto st = _build_index_pruned_physical_split_tasks(read_params);
                if (st.ok()) {
                    return Status::OK();
                }
                LOG(WARNING) << "failed to build index-pruned Lake split tasks, fallback to generic physical split"
                             << ", query_id: " << print_id(read_params.runtime_state->query_id())
                             << ", tablet_id: " << tablet_shared_ptr->tablet_id()
                             << ", rowsets: " << _rowsets.size()
                             << ", start_keys: " << read_params.start_key.size()
                             << ", end_keys: " << read_params.end_key.size()
                             << ", range_start_op: " << read_params.range
                             << ", range_end_op: " << read_params.end_range
                             << ", splitted_scan_rows: " << read_params.splitted_scan_rows
                             << ", lake_io_has_fs: " << (read_params.lake_io_opts.fs != nullptr)
                             << ", lake_io_has_location_provider: "
                             << (read_params.lake_io_opts.location_provider != nullptr) << ", status: " << st;
                _split_tasks.clear();
            }
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
        return init_collector(read_params);
    }

    return Status::OK();
}

Status TabletReader::_build_index_pruned_physical_split_tasks(const TabletReaderParams& read_params) {
    std::vector<SeekRange> seek_ranges;
    MemPool seek_range_mempool;
    RETURN_IF_ERROR(parse_seek_range(*_tablet_schema, read_params.range, read_params.end_range, read_params.start_key,
                                     read_params.end_key, &seek_ranges, &seek_range_mempool));

    LakeIOOptions lake_io_opts = read_params.lake_io_opts;
    bool resolved_fs_from_tablet_root = false;
    if (lake_io_opts.fs == nullptr) {
        auto root_loc = _tablet_mgr->tablet_root_location(_tablet_metadata->id());
        ASSIGN_OR_RETURN(lake_io_opts.fs, FileSystemFactory::CreateSharedFromString(root_loc));
        resolved_fs_from_tablet_root = true;
    }
    if (lake_io_opts.location_provider == nullptr) {
        lake_io_opts.location_provider = _tablet_mgr->location_provider();
    }

    PreparedReadState local_prepared_state;
    auto* prepared_state = _prepared_read_state != nullptr ? _prepared_read_state.get() : &local_prepared_state;
    RETURN_IF_ERROR(build_prepared_read_state(read_params, prepared_state));

    RowsetReadOptions rs_opts;
    RETURN_IF_ERROR(init_rowset_read_options(read_params, &rs_opts));
    rs_opts.lake_io_opts = lake_io_opts;

    LakeIndexPrunedPhysicalSplitPlanner planner(_tablet_schema, _tablet_metadata->id(), _is_asc_hint, &_stats,
                                                _prepared_read_state != nullptr,
                                                read_params, rs_opts, lake_io_opts, prepared_state);
    RETURN_IF_ERROR(planner.build(&_split_tasks));

    const auto& stats = planner.stats();
    LOG(INFO) << "Lake index-pruned physical split built split tasks"
              << ", query_id: " << print_id(read_params.runtime_state->query_id())
              << ", tablet_id: " << _tablet_metadata->id() << ", seek_ranges: " << seek_ranges.size()
              << ", resolved_fs_from_tablet_root: " << resolved_fs_from_tablet_root
              << ", visited_segments: " << stats.visited_segments
              << ", nonempty_segments: " << stats.nonempty_segments
              << ", fully_pruned_segments: " << stats.fully_pruned_segments
              << ", static_prepared_segments: " << stats.static_prepared_segments
              << ", matched_rows: " << stats.matched_rows << ", split_tasks: " << stats.flushed_split_tasks;
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
    if (_prepared_read_state != nullptr) {
        for (auto& rowset_iters : _prepared_read_state->rowset_iterators) {
            for (auto& iter : rowset_iters) {
                if (iter != nullptr) {
                    iter->close();
                    iter.reset();
                }
            }
        }
        _prepared_read_state->rowset_iterators.clear();
    }
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

Status TabletReader::build_prepared_read_state(const TabletReaderParams& params, PreparedReadState* state) {
    if (!state->rowsets.empty()) {
        return Status::OK();
    }

    state->rowsets = _rowsets;
    state->rowset_segments.reserve(state->rowsets.size());
    state->rowset_iterators.resize(state->rowsets.size());
    state->rowset_pruning_states.reserve(state->rowsets.size());
    for (const auto& rowset : state->rowsets) {
        ASSIGN_OR_RETURN(auto segments, rowset->segments(params.lake_io_opts));
        std::vector<std::shared_ptr<PreparedSegmentPruningState>> pruning_states;
        pruning_states.reserve(segments.size());
        for (size_t i = 0; i < segments.size(); ++i) {
            pruning_states.emplace_back(std::make_shared<PreparedSegmentPruningState>());
        }
        state->rowset_segments.emplace_back(std::move(segments));
        state->rowset_pruning_states.emplace_back(std::move(pruning_states));
    }
    return Status::OK();
}

Status TabletReader::init_rowset_read_options(const TabletReaderParams& params, RowsetReadOptions* rs_opts) {
    KeysType keys_type = _tablet_schema->keys_type();
    RETURN_IF_ERROR(init_predicates(params));
    RETURN_IF_ERROR(init_delete_predicates(params, &_delete_predicates));
    rs_opts->pred_tree = params.pred_tree;
    rs_opts->runtime_filter_preds = params.runtime_filter_preds;
    RETURN_IF_ERROR(ZonemapPredicatesRewriter::rewrite_predicate_tree(&_obj_pool, rs_opts->pred_tree,
                                                                      rs_opts->pred_tree_for_zone_map));
    rs_opts->sorted = ((keys_type != DUP_KEYS && keys_type != PRIMARY_KEYS) && !params.skip_aggregation) ||
                      is_compaction(params.reader_type) || params.sorted_by_keys_per_tablet;
    rs_opts->reader_type = params.reader_type;
    rs_opts->chunk_size = params.chunk_size;
    rs_opts->delete_predicates = &_delete_predicates;
    rs_opts->stats = &_stats;
    rs_opts->runtime_state = params.runtime_state;
    rs_opts->profile = params.profile;
    rs_opts->use_page_cache = params.use_page_cache;
    rs_opts->tablet_schema = _tablet_schema;
    rs_opts->global_dictmaps = params.global_dictmaps;
    rs_opts->unused_output_column_ids = params.unused_output_column_ids;
    rs_opts->runtime_range_pruner = params.runtime_range_pruner;
    rs_opts->lake_io_opts = params.lake_io_opts;
    rs_opts->enable_join_runtime_filter_pushdown = params.enable_join_runtime_filter_pushdown;
    rs_opts->prune_column_after_index_filter = params.prune_column_after_index_filter;
    rs_opts->enable_gin_filter = params.enable_gin_filter;
    rs_opts->enable_predicate_col_late_materialize = params.enable_predicate_col_late_materialize;

    if (keys_type == KeysType::PRIMARY_KEYS) {
        rs_opts->is_primary_keys = true;
        rs_opts->version = _tablet_metadata->version();
    }
    rs_opts->reader_type = params.reader_type;

    if (keys_type == PRIMARY_KEYS || keys_type == DUP_KEYS) {
        rs_opts->asc_hint = _is_asc_hint;
    }

    rs_opts->column_access_paths = params.column_access_paths;
    rs_opts->has_preaggregation = true;
    if ((is_compaction(params.reader_type) || params.sorted_by_keys_per_tablet)) {
        rs_opts->has_preaggregation = true;
    } else if (keys_type == PRIMARY_KEYS || keys_type == DUP_KEYS ||
               (keys_type == UNIQUE_KEYS && params.skip_aggregation)) {
        rs_opts->has_preaggregation = false;
    }
    RETURN_IF_ERROR(parse_seek_range(*_tablet_schema, params.range, params.end_range, params.start_key, params.end_key,
                                     &rs_opts->ranges, &_mempool));
    rs_opts->rowid_range_option = params.rowid_range_option;
    rs_opts->short_key_ranges_option = params.short_key_ranges_option;
    return Status::OK();
}

// TODO: support
//  1. rowid range and short key range
Status TabletReader::get_segment_iterators(const TabletReaderParams& params, std::vector<ChunkIteratorPtr>* iters) {
    PreparedReadState local_prepared_state;
    auto* prepared_state = _prepared_read_state != nullptr ? _prepared_read_state.get() : &local_prepared_state;
    const bool enable_reusable_segment_iters = _prepared_read_state != nullptr;
    RETURN_IF_ERROR(build_prepared_read_state(params, prepared_state));

    RowsetReadOptions rs_opts;
    RETURN_IF_ERROR(init_rowset_read_options(params, &rs_opts));

    SCOPED_RAW_TIMER(&_stats.create_segment_iter_ns);

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
    DCHECK_EQ(prepared_state->rowsets.size(), prepared_state->rowset_segments.size());
    DCHECK_EQ(prepared_state->rowsets.size(), prepared_state->rowset_iterators.size());
    DCHECK_EQ(prepared_state->rowsets.size(), prepared_state->rowset_pruning_states.size());
    for (size_t rowset_idx = 0; rowset_idx < prepared_state->rowsets.size(); ++rowset_idx) {
        auto& rowset = prepared_state->rowsets[rowset_idx];
        if (params.rowid_range_option != nullptr && !params.rowid_range_option->contains_rowset(rowset.get())) {
            continue;
        }

        if (config::enable_load_segment_parallel) {
            auto task = std::make_shared<std::packaged_task<StatusOr<std::vector<ChunkIteratorPtr>>()>>(
                    [&, rowset, rowset_idx]() {
#ifdef BE_TEST
                        Status injected_st;
                        TEST_SYNC_POINT_CALLBACK("TabletReader::get_segment_iterators::parallel_read", &injected_st);
                        if (!injected_st.ok()) {
                            return StatusOr<std::vector<ChunkIteratorPtr>>(injected_st);
                        }
#endif
                        if (enable_reusable_segment_iters) {
                            return enhance_error_prompt(
                                    rowset->read(schema(), rs_opts, prepared_state->rowset_segments[rowset_idx],
                                                 &prepared_state->rowset_iterators[rowset_idx],
                                                 &prepared_state->rowset_pruning_states[rowset_idx]));
                        }
                        return enhance_error_prompt(rowset->read(schema(), rs_opts, prepared_state->rowset_segments[rowset_idx]));
                    });

            auto packaged_func = [task]() { (*task)(); };
            if (auto st = ExecEnv::GetInstance()->load_rowset_thread_pool()->submit_func(std::move(packaged_func));
                !st.ok()) {
                // try load rowset serially if sumbit_func failed
                LOG(WARNING) << "sumbit_func failed: " << st.code_as_string()
                             << ", try to load rowset serially, rowset_id: " << rowset->id();
                ASSIGN_OR_RETURN(auto seg_iters,
                                 enable_reusable_segment_iters
                                         ? enhance_error_prompt(rowset->read(
                                                   schema(), rs_opts, prepared_state->rowset_segments[rowset_idx],
                                                   &prepared_state->rowset_iterators[rowset_idx],
                                                   &prepared_state->rowset_pruning_states[rowset_idx]))
                                         : enhance_error_prompt(rowset->read(schema(), rs_opts,
                                                                             prepared_state->rowset_segments[rowset_idx])));
                iters->insert(iters->end(), seg_iters.begin(), seg_iters.end());
            } else {
                futures.push_back(task->get_future());
            }
        } else {
            ASSIGN_OR_RETURN(auto seg_iters,
                             enable_reusable_segment_iters
                                     ? enhance_error_prompt(rowset->read(
                                               schema(), rs_opts, prepared_state->rowset_segments[rowset_idx],
                                               &prepared_state->rowset_iterators[rowset_idx],
                                               &prepared_state->rowset_pruning_states[rowset_idx]))
                                     : enhance_error_prompt(rowset->read(schema(), rs_opts,
                                                                         prepared_state->rowset_segments[rowset_idx])));
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

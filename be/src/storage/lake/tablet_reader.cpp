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
#include <utility>

#include "column/datum_convert.h"
#include "common/status.h"
#include "gutil/stl_util.h"
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
#include "storage/rowset/short_key_range_option.h"
#include "storage/seek_range.h"
#include "storage/tablet_schema_map.h"
#include "storage/types.h"
#include "storage/union_iterator.h"
#include "util/json_flattener.h"

namespace starrocks::lake {

using ConjunctivePredicates = starrocks::ConjunctivePredicates;
using Datum = starrocks::Datum;
using Field = starrocks::Field;
using PredicateParser = starrocks::PredicateParser;
using ZonemapPredicatesRewriter = starrocks::ZonemapPredicatesRewriter;

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
            split_morsel_queue = std::make_shared<pipeline::PhysicalSplitMorselQueue>(
                    std::move(morsels), read_params.scan_dop, read_params.splitted_scan_rows);
        } else {
            // logical
            split_morsel_queue = std::make_shared<pipeline::LogicalSplitMorselQueue>(
                    std::move(morsels), read_params.scan_dop, read_params.splitted_scan_rows);
        }

        // do prepare
        split_morsel_queue->set_tablets(std::move(tablets));
        split_morsel_queue->set_tablet_rowsets(std::move(tablet_rowsets));
        split_morsel_queue->set_key_ranges(read_params.range, read_params.end_range, read_params.start_key,
                                           read_params.end_key);
        split_morsel_queue->set_tablet_schema(_tablet_schema);

        while (true) {
            auto split = split_morsel_queue->try_get().value();
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

// TODO: support
//  1. rowid range and short key range
Status TabletReader::get_segment_iterators(const TabletReaderParams& params, std::vector<ChunkIteratorPtr>* iters) {
    RowsetReadOptions rs_opts;
    KeysType keys_type = _tablet_schema->keys_type();
    RETURN_IF_ERROR(init_predicates(params));
    RETURN_IF_ERROR(init_delete_predicates(params, &_delete_predicates));
    RETURN_IF_ERROR(parse_seek_range(*_tablet_schema, params.range, params.end_range, params.start_key, params.end_key,
                                     &rs_opts.ranges, &_mempool));
    rs_opts.pred_tree = params.pred_tree;
    PredicateTree pred_tree_for_zone_map;
    RETURN_IF_ERROR(ZonemapPredicatesRewriter::rewrite_predicate_tree(&_obj_pool, rs_opts.pred_tree,
                                                                      rs_opts.pred_tree_for_zone_map));
    rs_opts.sorted = ((keys_type != DUP_KEYS && keys_type != PRIMARY_KEYS) && !params.skip_aggregation) ||
                     is_compaction(params.reader_type) || params.sorted_by_keys_per_tablet;
    rs_opts.reader_type = params.reader_type;
    rs_opts.chunk_size = params.chunk_size;
    rs_opts.delete_predicates = &_delete_predicates;
    rs_opts.stats = &_stats;
    rs_opts.runtime_state = params.runtime_state;
    rs_opts.profile = params.profile;
    rs_opts.use_page_cache = params.use_page_cache;
    rs_opts.tablet_schema = _tablet_schema;
    rs_opts.global_dictmaps = params.global_dictmaps;
    rs_opts.unused_output_column_ids = params.unused_output_column_ids;
    rs_opts.runtime_range_pruner = params.runtime_range_pruner;
    rs_opts.lake_io_opts = params.lake_io_opts;

    if (keys_type == KeysType::PRIMARY_KEYS) {
        rs_opts.is_primary_keys = true;
        rs_opts.version = _tablet_metadata->version();
    }
    rs_opts.reader_type = params.reader_type;

    if (keys_type == PRIMARY_KEYS || keys_type == DUP_KEYS) {
        rs_opts.asc_hint = _is_asc_hint;
    }

    rs_opts.rowid_range_option = params.rowid_range_option;
    rs_opts.short_key_ranges_option = params.short_key_ranges_option;

    rs_opts.column_access_paths = params.column_access_paths;
    rs_opts.has_preaggregation = true;
    if ((is_compaction(params.reader_type) || params.sorted_by_keys_per_tablet)) {
        rs_opts.has_preaggregation = true;
    } else if (keys_type == PRIMARY_KEYS || keys_type == DUP_KEYS ||
               (keys_type == UNIQUE_KEYS && params.skip_aggregation)) {
        rs_opts.has_preaggregation = false;
    }

    SCOPED_RAW_TIMER(&_stats.create_segment_iter_ns);

    std::vector<std::future<StatusOr<std::vector<ChunkIteratorPtr>>>> futures;
    for (auto& rowset : _rowsets) {
        if (params.rowid_range_option != nullptr && !params.rowid_range_option->contains_rowset(rowset.get())) {
            continue;
        }

        if (config::enable_load_segment_parallel) {
            auto task = std::make_shared<std::packaged_task<StatusOr<std::vector<ChunkIteratorPtr>>()>>(
                    [&, rowset]() { return enhance_error_prompt(rowset->read(schema(), rs_opts)); });

            auto packaged_func = [task]() { (*task)(); };
            if (auto st = ExecEnv::GetInstance()->load_rowset_thread_pool()->submit_func(std::move(packaged_func));
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
            ColumnPredicate* pred = pred_parser.parse_thrift_cond(cond);
            if (pred == nullptr) {
                LOG(WARNING) << "failed to parse delete condition.column_name[" << cond.column_name
                             << "], condition_op[" << cond.condition_op << "], condition_values["
                             << cond.condition_values[0] << "].";
                continue;
            }
            conjunctions.add(pred);
            // save for memory release.
            _predicate_free_list.emplace_back(pred);
        }

        dels->add(index, conjunctions);
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
            RETURN_IF_ERROR(
                    datum_from_string(get_type_info(TYPE_VARCHAR).get(), &values.back(), input.get_value(i), mempool));
        } else {
            RETURN_IF_ERROR(datum_from_string(f->type().get(), &values.back(), input.get_value(i), mempool));
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
        ranges->emplace_back(SeekRange{std::move(lower), std::move(upper)});
        ranges->back().set_inclusive_lower(lower_inclusive);
        ranges->back().set_inclusive_upper(upper_inclusive);
    }
    return Status::OK();
}

} // namespace starrocks::lake

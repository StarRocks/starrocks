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

#include "storage/tablet_reader.h"

#include <algorithm>
#include <cstddef>
#include <utility>
#include <vector>

#include "column/column_access_path.h"
#include "column/datum_convert.h"
#include "common/status.h"
#include "gen_cpp/tablet_schema.pb.h"
#include "gutil/stl_util.h"
#include "primary_key_encoder.h"
#include "service/backend_options.h"
#include "storage/aggregate_iterator.h"
#include "storage/chunk_helper.h"
#include "storage/column_predicate.h"
#include "storage/column_predicate_rewriter.h"
#include "storage/conjunctive_predicates.h"
#include "storage/delete_predicates.h"
#include "storage/empty_iterator.h"
#include "storage/merge_iterator.h"
#include "storage/olap_common.h"
#include "storage/predicate_parser.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/seek_range.h"
#include "storage/tablet.h"
#include "storage/tablet_updates.h"
#include "storage/types.h"
#include "storage/union_iterator.h"
#include "types/logical_type.h"
#include "util/json_flattener.h"

namespace starrocks {

TabletReader::TabletReader(TabletSharedPtr tablet, const Version& version, Schema schema,
                           const TabletSchemaCSPtr& tablet_schema)
        : ChunkIterator(std::move(schema)),
          _tablet(std::move(tablet)),
          _version(version),
          _delete_predicates_version(version) {
    _tablet_schema = !tablet_schema ? _tablet->tablet_schema() : tablet_schema;
}

TabletReader::TabletReader(TabletSharedPtr tablet, const Version& version, Schema schema,
                           std::vector<RowsetSharedPtr> captured_rowsets, const TabletSchemaCSPtr* tablet_schema)
        : ChunkIterator(std::move(schema)),
          _tablet(std::move(tablet)),
          _version(version),
          _delete_predicates_version(version),
          _rowsets(std::move(captured_rowsets)) {
    _tablet_schema = tablet_schema ? *tablet_schema : _tablet->tablet_schema();
}

TabletReader::TabletReader(TabletSharedPtr tablet, const Version& version, Schema schema, bool is_key,
                           RowSourceMaskBuffer* mask_buffer, const TabletSchemaCSPtr& tablet_schema)
        : ChunkIterator(std::move(schema)),
          _tablet(std::move(tablet)),
          _version(version),
          _delete_predicates_version(version),
          _is_vertical_merge(true),
          _is_key(is_key),
          _mask_buffer(mask_buffer) {
    DCHECK(_mask_buffer);
    _tablet_schema = !tablet_schema ? _tablet->tablet_schema() : tablet_schema;
}

TabletReader::TabletReader(TabletSharedPtr tablet, const Version& version, const TabletSchemaCSPtr& tablet_schema,
                           Schema schema)
        : ChunkIterator(std::move(schema)), _tablet(std::move(tablet)), _version(version) {
    _tablet_schema = tablet_schema;
}

void TabletReader::close() {
    if (_collect_iter != nullptr) {
        _collect_iter->close();
        _collect_iter.reset();
    }
    STLDeleteElements(&_predicate_free_list);
    Rowset::release_readers(_rowsets);
    _rowsets.clear();
    _obj_pool.clear();
    _tablet_schema.reset();
}

Status TabletReader::prepare() {
    SCOPED_RAW_TIMER(&_stats.get_rowsets_ns);
    Status st = Status::OK();
    // Non-empty rowsets indicate that it is captured before creating this TabletReader.
    // _use_gtid is used to indicate that the rowsets are captured by gtid.
    if (_rowsets.empty() && !_use_gtid) {
        std::shared_lock l(_tablet->get_header_lock());
        st = _tablet->capture_consistent_rowsets(_version, &_rowsets);
        if (!st.ok()) {
            _rowsets.clear();
            std::stringstream ss;
            ss << "fail to init reader. tablet=" << _tablet->full_name() << "res=" << st;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str().c_str());
        }
    }
    _stats.rowsets_read_count += _rowsets.size();
    Rowset::acquire_readers(_rowsets);

    // ensure all input rowsets are loaded into memory
    for (const auto& rowset : _rowsets) {
        RETURN_IF_ERROR(rowset->load());
    }
    return st;
}

Status TabletReader::open(const TabletReaderParams& read_params) {
    if (read_params.reader_type != ReaderType::READER_QUERY && read_params.reader_type != ReaderType::READER_CHECKSUM &&
        read_params.reader_type != ReaderType::READER_ALTER_TABLE && !is_compaction(read_params.reader_type)) {
        return Status::NotSupported("reader type not supported now");
    }
    if (read_params.use_pk_index) {
        // defer init collector to IO scanner thread when calling do_get_next()
        _reader_params = &read_params;
        return Status::OK();
    }

    RETURN_IF_ERROR(_init_compaction_column_paths(read_params));
    Status st = _init_collector(read_params);
    return st;
}

Status TabletReader::_init_compaction_column_paths(const TabletReaderParams& read_params) {
    if (!config::enable_compaction_flat_json || !is_compaction(read_params.reader_type) ||
        read_params.column_access_paths == nullptr) {
        return Status::OK();
    }

    if (!read_params.column_access_paths->empty()) {
        VLOG(3) << "Compaction flat json paths exists: " << read_params.column_access_paths->size();
        return Status::OK();
    }

    DCHECK(is_compaction(read_params.reader_type) && read_params.column_access_paths != nullptr &&
           read_params.column_access_paths->empty());
    int num_readers = 0;
    for (const auto& rowset : _rowsets) {
        auto segments = rowset->segments();
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
            for (const auto& segment : rowset->segments()) {
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

            VLOG(3) << "Compaction flat json column: " << JsonFlatPath::debug_flat_json(paths, types, true);
            ASSIGN_OR_RETURN(auto res, ColumnAccessPath::create(TAccessPathType::ROOT, col_name, i));
            for (size_t j = 0; j < paths.size(); j++) {
                ColumnAccessPath::insert_json_path(res.get(), types[j], paths[j]);
            }
            res->set_from_compaction(true);
            read_params.column_access_paths->emplace_back(std::move(res));
        }
    }
    return Status::OK();
}

Status TabletReader::_init_collector_for_pk_index_read() {
    DCHECK(_reader_params != nullptr);
    // get pk eq predicates, and convert these predicates to encoded pk column
    const auto& tablet_schema = _tablet_schema;
    vector<ColumnId> pk_column_ids;
    for (size_t i = 0; i < tablet_schema->num_key_columns(); i++) {
        pk_column_ids.emplace_back(i);
    }
    auto pk_schema = ChunkHelper::convert_schema(tablet_schema, pk_column_ids);
    auto keys = ChunkHelper::new_chunk(pk_schema, 1);
    size_t num_pk_eq_predicates = 0;

    PredicateAndNode pushdown_pred_root;
    overloaded visitor{
            [&](const PredicateColumnNode& child_node) {
                const auto* col_pred = child_node.col_pred();
                const auto cid = col_pred->column_id();
                if (cid < tablet_schema->num_key_columns() && col_pred->type() == PredicateType::kEQ) {
                    auto& column = keys->get_column_by_id(cid);
                    if (column->size() != 0) {
                        return Status::NotSupported(
                                strings::Substitute("multiple eq predicates on same pk column columnId=$0", cid));
                    }
                    column->append_datum(col_pred->value());
                    num_pk_eq_predicates++;
                } else {
                    pushdown_pred_root.add_child(child_node);
                }
                return Status::OK();
            },
            [&]<CompoundNodeType Type>(const PredicateCompoundNode<Type>& child_node) {
                pushdown_pred_root.add_child(child_node);
                return Status::OK();
            },
    };
    for (const auto& child : _reader_params->pred_tree.root().children()) {
        RETURN_IF_ERROR(child.visit(visitor));
    }

    if (num_pk_eq_predicates != tablet_schema->num_key_columns()) {
        return Status::NotSupported(strings::Substitute("should have eq predicates on all pk columns current: $0 < $1",
                                                        num_pk_eq_predicates, tablet_schema->num_key_columns()));
    }
    MutableColumnPtr pk_column;
    RETURN_IF_ERROR(PrimaryKeyEncoder::create_column(*tablet_schema->schema(), &pk_column));
    PrimaryKeyEncoder::encode(*tablet_schema->schema(), *keys, 0, keys->num_rows(), pk_column.get());

    // get rowid using pk index
    std::vector<uint64_t> rowids(1);
    {
        SCOPED_RAW_TIMER(&_stats.read_pk_index_ns);
        EditVersion read_version;
        RETURN_IF_ERROR(
                _tablet->updates()->get_rss_rowids_by_pk(_tablet.get(), *pk_column, &read_version, &rowids, 3000));
        if (rowids.size() != 1) {
            return Status::InternalError(strings::Substitute("get rowid size not match tablet:$0 $1 != $2",
                                                             _tablet->tablet_id(), rowids.size(), 1));
        }
    }
    // do not check read version in use_pk_index mode
    uint32_t rssid = rowids[0] >> 32;
    uint32_t rowid = rowids[0] & 0xffffffff;
    if (rssid == (uint32_t)-1) {
        _collect_iter = new_empty_iterator(_schema, _reader_params->chunk_size);
        return Status::OK();
    }

    RowsetSharedPtr rowset;
    uint32_t segment_idx = 0;
    RETURN_IF_ERROR(_tablet->updates()->get_rowset_and_segment_idx_by_rssid(rssid, &rowset, &segment_idx));

    RowsetReadOptions rs_opts;
    rs_opts.pred_tree = PredicateTree::create(std::move(pushdown_pred_root));
    rs_opts.sorted = false;
    rs_opts.reader_type = _reader_params->reader_type;
    rs_opts.chunk_size = _reader_params->chunk_size;
    rs_opts.delete_predicates = &_delete_predicates;
    rs_opts.stats = &_stats;
    rs_opts.runtime_state = _reader_params->runtime_state;
    rs_opts.profile = _reader_params->profile;
    rs_opts.use_page_cache = _reader_params->use_page_cache;
    rs_opts.tablet_schema = _tablet_schema;
    rs_opts.global_dictmaps = _reader_params->global_dictmaps;
    rs_opts.unused_output_column_ids = _reader_params->unused_output_column_ids;
    rs_opts.runtime_range_pruner = _reader_params->runtime_range_pruner;
    // single row fetch, no need to use delvec
    rs_opts.is_primary_keys = false;
    rs_opts.use_vector_index = _reader_params->use_vector_index;
    rs_opts.vector_search_option = _reader_params->vector_search_option;
    rs_opts.enable_join_runtime_filter_pushdown = _reader_params->enable_join_runtime_filter_pushdown;

    rs_opts.rowid_range_option = std::make_shared<RowidRangeOption>();
    auto rowid_range = std::make_shared<SparseRange<>>();
    rowid_range->add({rowid, rowid + 1});
    if (segment_idx >= rowset->num_segments()) {
        return Status::InternalError(strings::Substitute("segment_idx out of range tablet:$0 $1 >= $2",
                                                         _tablet->tablet_id(), segment_idx, rowset->num_segments()));
    }
    rs_opts.rowid_range_option->add(rowset.get(), rowset->segments()[segment_idx].get(), rowid_range, true);

    std::vector<ChunkIteratorPtr> iters;
    RETURN_IF_ERROR(rowset->get_segment_iterators(schema(), rs_opts, &iters));

    if (iters.size() != 1) {
        return Status::InternalError(
                strings::Substitute("get_segment_iterators for pointer query should return single iter tablet:$0 $1",
                                    _tablet->tablet_id(), iters.size()));
    }

    _collect_iter = iters[0];

    // other collector setup
    RETURN_IF_ERROR(_collect_iter->init_encoded_schema(*_reader_params->global_dictmaps));
    RETURN_IF_ERROR(_collect_iter->init_output_schema(*_reader_params->unused_output_column_ids));

    return Status::OK();
}

Status TabletReader::do_get_next(Chunk* chunk) {
    DCHECK(!_is_vertical_merge);
    if (UNLIKELY(_collect_iter == nullptr)) {
        auto st = _init_collector_for_pk_index_read();
        if (!st.ok()) {
            LOG(WARNING) << "using pk index for pointer read failed, fallback to normal read " << st
                         << " tablet:" << _tablet->tablet_id();
            RETURN_IF_ERROR(_init_collector(*_reader_params));
        }
    }
    RETURN_IF_ERROR(_collect_iter->get_next(chunk));
    return Status::OK();
}

Status TabletReader::do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks) {
    DCHECK(_is_vertical_merge);
    RETURN_IF_ERROR(_collect_iter->get_next(chunk, source_masks));
    return Status::OK();
}

Status TabletReader::get_segment_iterators(const TabletReaderParams& params, std::vector<ChunkIteratorPtr>* iters) {
    RowsetReadOptions rs_opts;
    KeysType keys_type = _tablet_schema->keys_type();
    RETURN_IF_ERROR(_init_predicates(params));
    RETURN_IF_ERROR(_init_delete_predicates(params, &_delete_predicates));
    RETURN_IF_ERROR(parse_seek_range(_tablet_schema, params.range, params.end_range, params.start_key, params.end_key,
                                     &rs_opts.ranges, &_mempool));
    rs_opts.pred_tree = params.pred_tree;
    rs_opts.runtime_filter_preds = params.runtime_filter_preds;
    PredicateTree pred_tree_for_zone_map;
    RETURN_IF_ERROR(ZonemapPredicatesRewriter::rewrite_predicate_tree(&_obj_pool, rs_opts.pred_tree,
                                                                      rs_opts.pred_tree_for_zone_map));
    rs_opts.sorted = (keys_type != DUP_KEYS && keys_type != PRIMARY_KEYS) && !params.skip_aggregation;
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
    rs_opts.column_access_paths = params.column_access_paths;
    rs_opts.use_vector_index = params.use_vector_index;
    rs_opts.vector_search_option = params.vector_search_option;
    rs_opts.sample_options = params.sample_options;
    rs_opts.enable_join_runtime_filter_pushdown = params.enable_join_runtime_filter_pushdown;
    if (keys_type == KeysType::PRIMARY_KEYS) {
        rs_opts.is_primary_keys = true;
        rs_opts.version = _version.second;
    }
    rs_opts.meta = _tablet->data_dir()->get_meta();
    rs_opts.rowid_range_option = params.rowid_range_option;
    rs_opts.short_key_ranges_option = params.short_key_ranges_option;
    if (keys_type == PRIMARY_KEYS || keys_type == DUP_KEYS) {
        rs_opts.asc_hint = _is_asc_hint;
    }
    rs_opts.prune_column_after_index_filter = params.prune_column_after_index_filter;
    rs_opts.enable_gin_filter = params.enable_gin_filter;
    rs_opts.has_preaggregation = true;
    if ((is_compaction(params.reader_type) || params.sorted_by_keys_per_tablet)) {
        rs_opts.has_preaggregation = true;
    } else if (keys_type == PRIMARY_KEYS || keys_type == DUP_KEYS ||
               (keys_type == UNIQUE_KEYS && params.skip_aggregation)) {
        rs_opts.has_preaggregation = false;
    }

    SCOPED_RAW_TIMER(&_stats.create_segment_iter_ns);
    for (auto& rowset : _rowsets) {
        if (params.rowid_range_option != nullptr && !params.rowid_range_option->contains_rowset(rowset.get())) {
            continue;
        }

        RETURN_IF_ERROR(rowset->get_segment_iterators(schema(), rs_opts, iters));
    }
    return Status::OK();
}

Status TabletReader::_init_collector(const TabletReaderParams& params) {
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
    } else if (is_compaction(params.reader_type) && keys_type == DUP_KEYS) {
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
            _collect_iter = new_heap_merge_iterator(seg_iters);
        }
    } else if (params.sorted_by_keys_per_tablet && (keys_type == DUP_KEYS || keys_type == PRIMARY_KEYS) &&
               seg_iters.size() > 1) {
        // when enable sorted by keys. we need call heap merge for DUP KEYS and PKS
        // but for UNIQ KEYS or AGG KEYS we need build new_aggregate_iterator for them.
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
        // The segments may be in order after compaction. At this time, we prefer to read the later segments first.
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

Status TabletReader::_init_predicates(const TabletReaderParams& params) {
    return Status::OK();
}

Status TabletReader::_init_delete_predicates(const TabletReaderParams& params, DeletePredicates* dels) {
    OlapPredicateParser pred_parser(_tablet_schema);

    std::shared_lock header_lock(_tablet->get_header_lock());
    for (const DeletePredicatePB& pred_pb : _tablet->delete_predicates()) {
        if (pred_pb.version() > _delete_predicates_version.second) {
            continue;
        }

        ConjunctivePredicates conjunctions;
        for (int i = 0; i != pred_pb.sub_predicates_size(); ++i) {
            TCondition cond;
            if (!DeleteHandler::parse_condition(pred_pb.sub_predicates(i), &cond)) {
                LOG(WARNING) << "invalid delete condition: " << pred_pb.sub_predicates(i) << "]";
                return Status::InternalError("invalid delete condition string");
            }
            if (_tablet_schema->field_index(cond.column_name) >= _tablet_schema->num_key_columns() &&
                _tablet_schema->keys_type() != DUP_KEYS) {
                LOG(WARNING) << "ignore delete condition of non-key column: " << pred_pb.sub_predicates(i);
                continue;
            }
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

        for (int i = 0; i != pred_pb.in_predicates_size(); ++i) {
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

        if (conjunctions.empty()) {
            continue;
        }

        dels->add(pred_pb.version(), conjunctions);
    }

    return Status::OK();
}

// convert an OlapTuple to SeekTuple.
Status TabletReader::_to_seek_tuple(const TabletSchemaCSPtr& tablet_schema, const OlapTuple& input, SeekTuple* tuple,
                                    MemPool* mempool) {
    Schema schema;
    std::vector<Datum> values;
    values.reserve(input.size());
    const auto& sort_key_idxes = tablet_schema->sort_key_idxes();
    DCHECK(sort_key_idxes.empty() || sort_key_idxes.size() >= input.size());

    if (sort_key_idxes.size() > 0) {
        for (auto idx : sort_key_idxes) {
            schema.append_sort_key_idx(idx);
        }
    }
    for (size_t i = 0; i < input.size(); i++) {
        int idx = sort_key_idxes.empty() ? i : sort_key_idxes[i];
        auto f = std::make_shared<Field>(ChunkHelper::convert_field(idx, tablet_schema->column(idx)));
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
Status TabletReader::parse_seek_range(const TabletSchemaCSPtr& tablet_schema,
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
        RETURN_IF_ERROR(_to_seek_tuple(tablet_schema, range_start_key[i], &lower, mempool));
        RETURN_IF_ERROR(_to_seek_tuple(tablet_schema, range_end_key[i], &upper, mempool));
        ranges->emplace_back(SeekRange{std::move(lower), std::move(upper)});
        ranges->back().set_inclusive_lower(lower_inclusive);
        ranges->back().set_inclusive_upper(upper_inclusive);
    }
    return Status::OK();
}

} // namespace starrocks

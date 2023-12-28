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
#include "storage/seek_range.h"
#include "storage/tablet_schema_map.h"
#include "storage/types.h"
#include "storage/union_iterator.h"

namespace starrocks::lake {

using ConjunctivePredicates = starrocks::ConjunctivePredicates;
using Datum = starrocks::Datum;
using Field = starrocks::Field;
using PredicateParser = starrocks::PredicateParser;
using ZonemapPredicatesRewriter = starrocks::ZonemapPredicatesRewriter;

TabletReader::TabletReader(TabletManager* tablet_mgr, std::shared_ptr<const TabletMetadataPB> metadata, Schema schema)
        : ChunkIterator(std::move(schema)), _tablet_mgr(tablet_mgr), _tablet_metadata(std::move(metadata)) {}

TabletReader::TabletReader(TabletManager* tablet_mgr, std::shared_ptr<const TabletMetadataPB> metadata, Schema schema,
                           std::vector<RowsetPtr> rowsets)
        : ChunkIterator(std::move(schema)),
          _tablet_mgr(tablet_mgr),
          _tablet_metadata(std::move(metadata)),
          _rowsets_inited(true),
          _rowsets(std::move(rowsets)) {}

TabletReader::TabletReader(TabletManager* tablet_mgr, std::shared_ptr<const TabletMetadataPB> metadata, Schema schema,
                           std::vector<RowsetPtr> rowsets, bool is_key, RowSourceMaskBuffer* mask_buffer)
        : ChunkIterator(std::move(schema)),
          _tablet_mgr(tablet_mgr),
          _tablet_metadata(std::move(metadata)),
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
    _tablet_schema = GlobalTabletSchemaMap::Instance()->emplace(_tablet_metadata->schema()).first;
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
        read_params.reader_type != ReaderType::READER_ALTER_TABLE && !is_compaction(read_params.reader_type)) {
        return Status::NotSupported("reader type not supported now");
    }
    Status st = init_collector(read_params);
    return st;
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
    RETURN_IF_ERROR(_collect_iter->get_next(chunk));
    return Status::OK();
}

Status TabletReader::do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks) {
    DCHECK(_is_vertical_merge);
    RETURN_IF_ERROR(_collect_iter->get_next(chunk, source_masks));
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
    rs_opts.predicates = _pushdown_predicates;
    RETURN_IF_ERROR(ZonemapPredicatesRewriter::rewrite_predicate_map(&_obj_pool, rs_opts.predicates,
                                                                     &rs_opts.predicates_for_zone_map));
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
    rs_opts.fill_data_cache = params.fill_data_cache;
    if (keys_type == KeysType::PRIMARY_KEYS) {
        rs_opts.is_primary_keys = true;
        rs_opts.version = _tablet_metadata->version();
    }

    SCOPED_RAW_TIMER(&_stats.create_segment_iter_ns);
    for (auto& rowset : _rowsets) {
        ASSIGN_OR_RETURN(auto seg_iters, enhance_error_prompt(rowset->read(schema(), rs_opts)));
        iters->insert(iters->end(), seg_iters.begin(), seg_iters.end());
    }
    return Status::OK();
}

Status TabletReader::init_predicates(const TabletReaderParams& params) {
    for (const ColumnPredicate* pred : params.predicates) {
        _pushdown_predicates[pred->column_id()].emplace_back(pred);
    }
    return Status::OK();
}

Status TabletReader::init_delete_predicates(const TabletReaderParams& params, DeletePredicates* dels) {
    if (UNLIKELY(_tablet_metadata == nullptr)) {
        return Status::InternalError("tablet metadata is null. forget or fail to call prepare()");
    }
    if (UNLIKELY(_tablet_schema == nullptr)) {
        return Status::InternalError("tablet schema is null. forget or fail to call prepare()");
    }
    PredicateParser pred_parser(_tablet_schema);

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
            _collect_iter = new_heap_merge_iterator(seg_iters);
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

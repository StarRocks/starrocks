// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/tablet_reader.h"

#include <column/datum_convert.h>

#include "common/status.h"
#include "gutil/stl_util.h"
#include "service/backend_options.h"
#include "storage/tablet.h"
#include "storage/vectorized/aggregate_iterator.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/vectorized/column_predicate.h"
#include "storage/vectorized/conjunctive_predicates.h"
#include "storage/vectorized/delete_predicates.h"
#include "storage/vectorized/empty_iterator.h"
#include "storage/vectorized/merge_iterator.h"
#include "storage/vectorized/predicate_parser.h"
#include "storage/vectorized/seek_range.h"
#include "storage/vectorized/union_iterator.h"

namespace starrocks::vectorized {

TabletReader::TabletReader(TabletSharedPtr tablet, const Version& version, Schema schema)
        : ChunkIterator(std::move(schema)), _tablet(tablet), _version(version) {
    _delete_predicates_version = version;
}

void TabletReader::close() {
    if (_collect_iter != nullptr) {
        _collect_iter->close();
        _collect_iter.reset();
    }
    STLDeleteElements(&_predicate_free_list);
}

Status TabletReader::prepare() {
    std::shared_lock l(_tablet->get_header_lock());
    auto st = _tablet->capture_consistent_rowsets(_version, &_rowsets);
    return st;
}

Status TabletReader::open(const TabletReaderParams& read_params) {
    if (read_params.reader_type != ReaderType::READER_QUERY && read_params.reader_type != ReaderType::READER_CHECKSUM &&
        read_params.reader_type != ReaderType::READER_ALTER_TABLE && !is_compaction(read_params.reader_type)) {
        return Status::NotSupported("reader type not supported now");
    }
    Status st = _init_collector(read_params);
    _rowsets.clear(); // unused anymore.
    return st;
}

Status TabletReader::do_get_next(Chunk* chunk) {
    return _collect_iter->get_next(chunk);
}

Status TabletReader::_get_segment_iterators(const RowsetReadOptions& options, std::vector<ChunkIteratorPtr>* iters) {
    SCOPED_RAW_TIMER(&_stats.create_segment_iter_ns);
    for (auto& rowset : _rowsets) {
        RETURN_IF_ERROR(rowset->get_segment_iterators(schema(), options, iters));
    }
    return Status::OK();
}

Status TabletReader::_init_collector(const TabletReaderParams& params) {
    RowsetReadOptions rs_opts;
    KeysType keys_type = _tablet->tablet_schema().keys_type();
    RETURN_IF_ERROR(_init_predicates(params));
    RETURN_IF_ERROR(_init_delete_predicates(params, &_delete_predicates));
    RETURN_IF_ERROR(_parse_seek_range(params, &rs_opts.ranges));
    rs_opts.predicates = _pushdown_predicates;
    rs_opts.sorted = (keys_type != DUP_KEYS && keys_type != PRIMARY_KEYS) && !params.skip_aggregation;
    rs_opts.reader_type = params.reader_type;
    rs_opts.chunk_size = params.chunk_size;
    rs_opts.delete_predicates = &_delete_predicates;
    rs_opts.stats = &_stats;
    rs_opts.runtime_state = params.runtime_state;
    rs_opts.profile = params.profile;
    rs_opts.use_page_cache = params.use_page_cache;
    rs_opts.tablet_schema = &(_tablet->tablet_schema());
    rs_opts.global_dictmaps = params.global_dictmaps;
    if (keys_type == KeysType::PRIMARY_KEYS) {
        rs_opts.is_primary_keys = true;
        rs_opts.version = _version.second;
        rs_opts.meta = _tablet->data_dir()->get_meta();
    }

    std::vector<ChunkIteratorPtr> seg_iters;
    RETURN_IF_ERROR(_get_segment_iterators(rs_opts, &seg_iters));

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
    const auto skip_aggr = params.skip_aggregation;
    const auto select_all_keys = _schema.num_key_fields() == _tablet->num_key_columns();
    DCHECK_LE(_schema.num_key_fields(), _tablet->num_key_columns());

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
        _collect_iter = new_merge_iterator(seg_iters);
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
        if (params.profile != nullptr && params.profile->parent() != nullptr) {
            RuntimeProfile* p = params.profile->parent()->create_child("MERGE", true, true);
            RuntimeProfile::Counter* sort_timer = ADD_TIMER(p, "sort");
            RuntimeProfile::Counter* aggr_timer = ADD_TIMER(p, "aggr");

            _collect_iter = new_merge_iterator(seg_iters);
            _collect_iter = timed_chunk_iterator(_collect_iter, sort_timer);
            _collect_iter = new_aggregate_iterator(std::move(_collect_iter), 0);
            _collect_iter = timed_chunk_iterator(_collect_iter, aggr_timer);
        } else {
            _collect_iter = new_merge_iterator(seg_iters);
            _collect_iter = new_aggregate_iterator(std::move(_collect_iter), 0);
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
        if (params.profile != nullptr && params.profile->parent() != nullptr) {
            RuntimeProfile* p = params.profile->parent()->create_child("MERGE", true, true);
            RuntimeProfile::Counter* union_timer = ADD_TIMER(p, "union");
            RuntimeProfile::Counter* aggr_timer = ADD_TIMER(p, "aggr");

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
    }

    return Status::OK();
}

Status TabletReader::_init_predicates(const TabletReaderParams& params) {
    for (const ColumnPredicate* pred : params.predicates) {
        _pushdown_predicates[pred->column_id()].emplace_back(pred);
    }
    return Status::OK();
}

Status TabletReader::_init_delete_predicates(const TabletReaderParams& params, DeletePredicates* dels) {
    PredicateParser pred_parser(_tablet->tablet_schema());

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
            size_t idx = _tablet->tablet_schema().field_index(cond.column_name);
            if (idx >= _tablet->num_key_columns() && _tablet->keys_type() != DUP_KEYS) {
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

        dels->add(pred_pb.version(), conjunctions);
    }

    return Status::OK();
}

// convert an OlapTuple to SeekTuple.
Status TabletReader::_to_seek_tuple(const TabletSchema& tablet_schema, const OlapTuple& input, SeekTuple* tuple) {
    Schema schema;
    std::vector<Datum> values;
    values.reserve(input.size());
    for (size_t i = 0; i < input.size(); i++) {
        auto f = std::make_shared<Field>(ChunkHelper::convert_field_to_format_v2(i, tablet_schema.column(i)));
        schema.append(f);
        values.emplace_back(Datum());
        if (input.is_null(i)) {
            continue;
        }
        RETURN_IF_ERROR(datum_from_string(f->type().get(), &values.back(), input.get_value(i), &_mempool));
    }
    *tuple = SeekTuple(std::move(schema), std::move(values));
    return Status::OK();
}

// convert vector<OlapTuple> to vector<SeekRange>
Status TabletReader::_parse_seek_range(const TabletReaderParams& read_params, std::vector<SeekRange>* ranges) {
    if (read_params.start_key.empty()) {
        return {};
    }

    bool inc_lower = read_params.range == "ge" || read_params.range == "eq";
    bool inc_upper = read_params.end_range == "le" || read_params.end_range == "eq";

    CHECK_EQ(read_params.start_key.size(), read_params.end_key.size());
    size_t n = read_params.start_key.size();

    ranges->reserve(n);
    for (size_t i = 0; i < n; i++) {
        SeekTuple lower;
        SeekTuple upper;
        RETURN_IF_ERROR(_to_seek_tuple(_tablet->tablet_schema(), read_params.start_key[i], &lower));
        RETURN_IF_ERROR(_to_seek_tuple(_tablet->tablet_schema(), read_params.end_key[i], &upper));
        ranges->emplace_back(SeekRange{std::move(lower), std::move(upper)});
        ranges->back().set_inclusive_lower(inc_lower);
        ranges->back().set_inclusive_upper(inc_upper);
    }
    return Status::OK();
}

} // namespace starrocks::vectorized

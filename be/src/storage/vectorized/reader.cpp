// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/reader.h"

#include <column/datum_convert.h>

#include "gutil/stl_util.h"
#include "service/backend_options.h"
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

Reader::Reader(Schema schema) : ChunkIterator(std::move(schema)), _mempool(&_memtracker) {}

void Reader::close() {
    if (_collect_iter != nullptr) {
        _collect_iter->close();
        _collect_iter.reset();
    }
    STLDeleteElements(&_predicate_free_list);
}

Status Reader::init(const ReaderParams& read_params) {
    read_params.check_validation();
    if (read_params.reader_type != ReaderType::READER_QUERY && !is_compaction(read_params.reader_type)) {
        return Status::NotSupported("reader type not supported now");
    }
    RETURN_IF_ERROR(_init_load_bf_columns(read_params));

    Status status = _init_collector(read_params);
    LOG_IF(WARNING, !status.ok() && !status.is_end_of_file())
            << "fail to init reader when _capture_rs_readers. res" << status.to_string()
            << ", tablet_id :" << read_params.tablet->tablet_id()
            << ", schema_hash:" << read_params.tablet->schema_hash() << ", reader_type:" << read_params.reader_type
            << ", version:" << read_params.version.first << "-" << read_params.version.second;
    return status;
}

Status Reader::do_get_next(Chunk* chunk) {
    return _collect_iter->get_next(chunk);
}

Status Reader::_get_segment_iterators(const TabletSharedPtr& tablet, const Version& version,
                                      const RowsetReadOptions& options, std::vector<ChunkIteratorPtr>* iters) {
    SCOPED_RAW_TIMER(&_stats.capture_rowset_ns);

    StatusOr<Tablet::IteratorList> res;
    res = tablet->capture_segment_iterators(version, schema(), options);
    if (!res.ok()) {
        std::stringstream ss;
        ss << "failed to capture rowset iterators, tablet=" << tablet->full_name()
           << ", res=" << res.status().to_string() << ", backend=" << BackendOptions::get_localhost();
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str().c_str());
    }
    *iters = std::move(res).value();
    return Status::OK();
}

Status Reader::_init_collector(const ReaderParams& params) {
    RowsetReadOptions rs_opts;
    KeysType keys_type = params.tablet->tablet_schema().keys_type();
    RETURN_IF_ERROR(_init_predicates(params));
    RETURN_IF_ERROR(_init_delete_predicates(params, &_delete_predicates));
    RETURN_IF_ERROR(_parse_seek_range(params, &rs_opts.ranges));
    rs_opts.predicates = _pushdown_predicates;
    rs_opts.sorted = (keys_type != DUP_KEYS && keys_type != PRIMARY_KEYS) && !params.skip_aggregation;
    rs_opts.load_bf_columns = &_load_bf_columns;
    rs_opts.reader_type = params.reader_type;
    rs_opts.chunk_size = params.chunk_size;
    rs_opts.delete_predicates = &_delete_predicates;
    rs_opts.stats = &_stats;
    rs_opts.runtime_state = params.runtime_state;
    rs_opts.profile = params.profile;
    rs_opts.use_page_cache = params.use_page_cache;
    rs_opts.tablet_schema = &(params.tablet->tablet_schema());
    if (keys_type == KeysType::PRIMARY_KEYS) {
        rs_opts.is_primary_keys = true;
        rs_opts.version = params.version.second;
        rs_opts.meta = params.tablet->data_dir()->get_meta();
    }

    std::vector<ChunkIteratorPtr> seg_iters;
    RETURN_IF_ERROR(_get_segment_iterators(params.tablet, params.version, rs_opts, &seg_iters));

    // Put each SegmentIterator into a TimedChunkIterator, if a profile is provided.
    if (params.profile != nullptr) {
        RuntimeProfile::Counter* scan_timer = params.profile->total_time_counter();
        std::vector<ChunkIteratorPtr> children;
        children.reserve(seg_iters.size());
        for (auto& seg_iter : seg_iters) {
            children.emplace_back(timed_chunk_iterator(std::move(seg_iter), scan_timer));
        }
        seg_iters.swap(children);
    }

    // If |keys_type| is UNIQUE_KEYS and |params.skip_aggregation| is true, must disable aggregate totally.
    // If |keys_type| is AGG_KEYS and |params.skip_aggregation| is true, aggregate is an optional operation.
    const auto skip_aggr = params.skip_aggregation;
    const auto select_all_keys = _schema.num_key_fields() == params.tablet->num_key_columns();
    DCHECK_LE(_schema.num_key_fields(), params.tablet->num_key_columns());

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
        _collect_iter = new_merge_iterator(std::move(seg_iters));
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
            _collect_iter = timed_chunk_iterator(std::move(_collect_iter), sort_timer);
            _collect_iter = new_aggregate_iterator(std::move(_collect_iter), 0);
            _collect_iter = timed_chunk_iterator(std::move(_collect_iter), aggr_timer);
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
            _collect_iter = timed_chunk_iterator(std::move(_collect_iter), union_timer);
            _collect_iter = new_aggregate_iterator(std::move(_collect_iter), f);
            _collect_iter = timed_chunk_iterator(std::move(_collect_iter), aggr_timer);
        } else {
            _collect_iter = new_union_iterator(std::move(seg_iters));
            _collect_iter = new_aggregate_iterator(std::move(_collect_iter), f);
        }
    } else {
        return Status::InternalError("Unknown keys type");
    }
    return Status::OK();
}

Status Reader::_init_predicates(const ReaderParams& params) {
    for (const ColumnPredicate* pred : params.predicates) {
        _pushdown_predicates[pred->column_id()].emplace_back(pred);
    }
    return Status::OK();
}

Status Reader::_init_load_bf_columns(const ReaderParams& read_params) {
    // TODO:
    return Status::OK();
}

Status Reader::_init_delete_predicates(const ReaderParams& params, DeletePredicates* dels) {
    PredicateParser pred_parser(params.tablet->tablet_schema());

    Status st;

    params.tablet->obtain_header_rdlock();

    for (const DeletePredicatePB& pred_pb : params.tablet->delete_predicates()) {
        if (pred_pb.version() > params.version.second) {
            continue;
        }

        ConjunctivePredicates conjunctions;
        for (int i = 0; i != pred_pb.sub_predicates_size(); ++i) {
            TCondition cond;
            if (!DeleteHandler::parse_condition(pred_pb.sub_predicates(i), &cond)) {
                LOG(WARNING) << "invalid delete condition: " << pred_pb.sub_predicates(i) << "]";
                st = Status::InternalError("invalid delete condition string");
                break;
            }
            size_t idx = params.tablet->tablet_schema().field_index(cond.column_name);
            if (idx >= params.tablet->num_key_columns() && params.tablet->keys_type() != DUP_KEYS) {
                LOG(WARNING) << "ignore delete condition of non-key column: " << pred_pb.sub_predicates(i);
                continue;
            }
            ColumnPredicate* pred = pred_parser.parse(cond);
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
            ColumnPredicate* pred = pred_parser.parse(cond);
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

    params.tablet->release_header_lock();
    return st;
}

// convert an OlapTuple to SeekTuple.
Status Reader::_to_seek_tuple(const TabletSchema& tablet_schema, const OlapTuple& input, SeekTuple* tuple) {
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
Status Reader::_parse_seek_range(const ReaderParams& read_params, std::vector<SeekRange>* ranges) {
    if (read_params.start_key.empty()) {
        return {};
    }
    const TabletSharedPtr& tablet = read_params.tablet;

    bool inc_lower = read_params.range == "ge" || read_params.range == "eq";
    bool inc_upper = read_params.end_range == "le" || read_params.end_range == "eq";

    CHECK_EQ(read_params.start_key.size(), read_params.end_key.size());
    size_t n = read_params.start_key.size();

    ranges->reserve(n);
    for (size_t i = 0; i < n; i++) {
        SeekTuple lower;
        SeekTuple upper;
        RETURN_IF_ERROR(_to_seek_tuple(tablet->tablet_schema(), read_params.start_key[i], &lower));
        RETURN_IF_ERROR(_to_seek_tuple(tablet->tablet_schema(), read_params.end_key[i], &upper));
        ranges->emplace_back(SeekRange{std::move(lower), std::move(upper)});
        ranges->back().set_inclusive_lower(inc_lower);
        ranges->back().set_inclusive_upper(inc_upper);
    }
    return Status::OK();
}

} // namespace starrocks::vectorized

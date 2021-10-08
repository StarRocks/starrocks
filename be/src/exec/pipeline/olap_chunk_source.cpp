// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/olap_chunk_source.h"

#include "column/column_helper.h"
#include "exec/vectorized/olap_scan_prepare.h"
#include "exprs/vectorized/in_const_predicate.hpp"
#include "exprs/vectorized/runtime_filter.h"
#include "gutil/map_util.h"
#include "runtime/current_mem_tracker.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "storage/storage_engine.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/vectorized/predicate_parser.h"
#include "storage/vectorized/projection_iterator.h"

namespace starrocks::pipeline {
using namespace vectorized;
Status OlapChunkSource::prepare(RuntimeState* state) {
    _runtime_state = state;
    _scan_profile = state->runtime_profile();

    for (const auto& ctx_iter : _conjunct_ctxs) {
        // if conjunct is constant, compute direct and set eos = true
        if (ctx_iter->root()->is_constant()) {
            ColumnPtr value = ctx_iter->root()->evaluate_const(ctx_iter);

            if (value == nullptr || value->only_null() || value->is_null(0)) {
                _status = Status::EndOfFile("conjuncts evaluated to null");
            } else if (value->is_constant() && !vectorized::ColumnHelper::get_const_value<TYPE_BOOLEAN>(value)) {
                _status = Status::EndOfFile("conjuncts evaluated to false");
            }
        }
    }

    const TupleDescriptor* tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    _slots = &tuple_desc->slots();
    // 1. Convert conjuncts to ColumnValueRange in each column
    RETURN_IF_ERROR(details::normalize_conjuncts(*_slots, _obj_pool, _conjunct_ctxs, _normalized_conjuncts,
                                                 _runtime_filters, _is_null_vector, _column_value_ranges, &_status));

    // 2. Using ColumnValueRange to Build StorageEngine filters
    RETURN_IF_ERROR(details::build_olap_filters(_column_value_ranges, _olap_filter));

    const TQueryOptions& query_options = state->query_options();
    int32_t max_scan_key_num;
    if (query_options.__isset.max_scan_key_num && query_options.max_scan_key_num > 0) {
        max_scan_key_num = query_options.max_scan_key_num;
    } else {
        max_scan_key_num = config::doris_max_scan_key_num;
    }

    // 3. Using `Key Column`'s ColumnValueRange to split ScanRange to sererval `Sub ScanRange`
    RETURN_IF_ERROR(
            details::build_scan_key(_key_column_names, _column_value_ranges, _scan_keys, true, max_scan_key_num));

    // 4. Build olap scanner range
    RETURN_IF_ERROR(_build_scan_range(_runtime_state));

    // 5. Init olap reader
    RETURN_IF_ERROR(_init_olap_reader(_runtime_state));
    return Status::OK();
}

Status OlapChunkSource::_build_scan_range(RuntimeState* state) {
    RETURN_IF_ERROR(_scan_keys.get_key_range(&_cond_ranges));
    if (_cond_ranges.empty()) {
        _cond_ranges.emplace_back(new OlapScanRange());
    }

    DCHECK_EQ(_conjunct_ctxs.size(), _normalized_conjuncts.size());
    for (size_t i = 0; i < _normalized_conjuncts.size(); i++) {
        if (!_normalized_conjuncts[i]) {
            _un_push_down_conjuncts.push_back(_conjunct_ctxs[i]);
        }
    }

    // FixMe(kks): Ensure this logic is right.
    int scanners_per_tablet = 64;
    int num_ranges = _cond_ranges.size();
    int ranges_per_scanner = std::max(1, num_ranges / scanners_per_tablet);
    for (int i = 0; i < num_ranges;) {
        _scanner_ranges.push_back(_cond_ranges[i].get());
        i++;
        for (int j = 1; i < num_ranges && j < ranges_per_scanner &&
                        _cond_ranges[i]->end_include == _cond_ranges[i - 1]->end_include;
             ++j, ++i) {
            _scanner_ranges.push_back(_cond_ranges[i].get());
        }
    }
    return Status::OK();
}

Status OlapChunkSource::_get_tablet(const TInternalScanRange* scan_range) {
    TTabletId tablet_id = scan_range->tablet_id;
    SchemaHash schema_hash = strtoul(scan_range->schema_hash.c_str(), nullptr, 10);
    _version = strtoul(scan_range->version.c_str(), nullptr, 10);

    std::string err;
    _tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, schema_hash, true, &err);
    if (!_tablet) {
        std::stringstream ss;
        ss << "failed to get tablet. tablet_id=" << tablet_id << ", with schema_hash=" << schema_hash
           << ", reason=" << err;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

Status OlapChunkSource::_init_reader_params(const std::vector<OlapScanRange*>& key_ranges,
                                            const std::vector<uint32_t>& scanner_columns,
                                            std::vector<uint32_t>& reader_columns,
                                            vectorized::TabletReaderParams* params) {
    params->reader_type = READER_QUERY;
    params->skip_aggregation = _skip_aggregation;
    params->profile = _scan_profile;
    params->runtime_state = _runtime_state;
    params->use_page_cache = !config::disable_storage_page_cache;
    params->chunk_size = config::vector_chunk_size;

    PredicateParser parser(_tablet->tablet_schema());

    // Condition
    for (auto& filter : _olap_filter) {
        vectorized::ColumnPredicate* p = parser.parse(filter);
        p->set_index_filter_only(filter.is_index_filter_only);
        _predicate_free_pool.emplace_back(p);
        if (parser.can_pushdown(p)) {
            params->predicates.push_back(p);
        } else {
            _un_push_down_predicates.add(p);
        }
    }
    for (auto& is_null_str : _is_null_vector) {
        vectorized::ColumnPredicate* p = parser.parse(is_null_str);
        _predicate_free_pool.emplace_back(p);
        if (parser.can_pushdown(p)) {
            params->predicates.push_back(p);
        } else {
            _un_push_down_predicates.add(p);
        }
    }

    // Range
    for (auto key_range : key_ranges) {
        if (key_range->begin_scan_range.size() == 1 && key_range->begin_scan_range.get_value(0) == NEGATIVE_INFINITY) {
            continue;
        }

        params->range = key_range->begin_include ? "ge" : "gt";
        params->end_range = key_range->end_include ? "le" : "lt";

        params->start_key.push_back(key_range->begin_scan_range);
        params->end_key.push_back(key_range->end_scan_range);
    }

    // Return columns
    if (_skip_aggregation) {
        reader_columns = scanner_columns;
    } else {
        for (size_t i = 0; i < _tablet->num_key_columns(); i++) {
            reader_columns.push_back(i);
        }
        for (auto index : scanner_columns) {
            if (!_tablet->tablet_schema().column(index).is_key()) {
                reader_columns.push_back(index);
            }
        }
    }
    // Actually only the key columns need to be sorted by id, here we check all
    // for simplicity.
    DCHECK(std::is_sorted(reader_columns.begin(), reader_columns.end()));
    return Status::OK();
}

Status OlapChunkSource::_init_scanner_columns(std::vector<uint32_t>& scanner_columns) {
    for (auto slot : *_slots) {
        DCHECK(slot->is_materialized());
        int32_t index = _tablet->field_index(slot->col_name());
        if (index < 0) {
            std::stringstream ss;
            ss << "invalid field name: " << slot->col_name();
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        scanner_columns.push_back(index);
    }
    // Put key columns before non-key columns, as the `MergeIterator` and `AggregateIterator`
    // required.
    std::sort(scanner_columns.begin(), scanner_columns.end());
    if (scanner_columns.empty()) {
        return Status::InternalError("failed to build storage scanner, no materialized slot!");
    }
    return Status::OK();
}

Status OlapChunkSource::_init_olap_reader(RuntimeState* runtime_state) {
    // output columns of `this` OlapScanner, i.e, the final output columns of `get_chunk`.
    std::vector<uint32_t> scanner_columns;
    // columns fetched from |_reader|.
    std::vector<uint32_t> reader_columns;
    vectorized::TabletReaderParams params;

    RETURN_IF_ERROR(_get_tablet(_scan_range));
    RETURN_IF_ERROR(_init_scanner_columns(scanner_columns));
    RETURN_IF_ERROR(_init_reader_params(_scanner_ranges, scanner_columns, reader_columns, &params));
    const TabletSchema& tablet_schema = _tablet->tablet_schema();
    starrocks::vectorized::Schema child_schema =
            ChunkHelper::convert_schema_to_format_v2(tablet_schema, reader_columns);
    _reader = std::make_shared<TabletReader>(_tablet, Version(0, _version), std::move(child_schema));
    if (reader_columns.size() == scanner_columns.size()) {
        _prj_iter = _reader;
    } else {
        starrocks::vectorized::Schema output_schema =
                ChunkHelper::convert_schema_to_format_v2(tablet_schema, scanner_columns);
        _prj_iter = new_projection_iterator(output_schema, _reader);
    }

    if (!_un_push_down_conjuncts.empty() || !_un_push_down_predicates.empty()) {
        _expr_filter_timer = ADD_TIMER(_scan_profile, "ExprFilterTime");
    }
    RETURN_IF_ERROR(_reader->prepare());
    RETURN_IF_ERROR(_reader->open(params));
    return Status::OK();
}

bool OlapChunkSource::has_next_chunk() const {
    // If we need and could get next chunk from storage engine,
    // the _status must be ok.
    return _status.ok();
}

StatusOr<vectorized::ChunkUniquePtr> OlapChunkSource::get_next_chunk() {
    if (!_status.ok()) {
        return _status;
    }
    using namespace vectorized;
    ChunkUniquePtr chunk(ChunkHelper::new_chunk_pooled(_prj_iter->schema(), config::vector_chunk_size, true));
    _status = _read_chunk_from_storage(_runtime_state, chunk.get());
    if (!_status.ok()) {
        return _status;
    }
    return std::move(chunk);
}

void OlapChunkSource::cache_next_chunk_blocking() {
    _chunk = get_next_chunk();
}

StatusOr<vectorized::ChunkUniquePtr> OlapChunkSource::get_next_chunk_nonblocking() {
    return std::move(_chunk);
}

Status OlapChunkSource::_read_chunk_from_storage(RuntimeState* state, vectorized::Chunk* chunk) {
    if (state->is_cancelled()) {
        return Status::Cancelled("canceled state");
    }
    SCOPED_TIMER(_scan_timer);
    do {
        if (Status status = _prj_iter->get_next(chunk); !status.ok()) {
            return status;
        }

        for (auto slot : *_slots) {
            size_t column_index = chunk->schema()->get_field_index_by_name(slot->col_name());
            chunk->set_slot_id_to_index(slot->id(), column_index);
        }

        if (!_un_push_down_predicates.empty()) {
            int64_t old_mem_usage = chunk->memory_usage();
            SCOPED_TIMER(_expr_filter_timer);
            size_t nrows = chunk->num_rows();
            _selection.resize(nrows);
            _un_push_down_predicates.evaluate(chunk, _selection.data(), 0, nrows);
            chunk->filter(_selection);
            CurrentMemTracker::consume((int64_t)chunk->memory_usage() - old_mem_usage);
            DCHECK_CHUNK(chunk);
        }
        if (!_un_push_down_conjuncts.empty()) {
            int64_t old_mem_usage = chunk->memory_usage();
            SCOPED_TIMER(_expr_filter_timer);
            ExecNode::eval_conjuncts(_un_push_down_conjuncts, chunk);
            CurrentMemTracker::consume((int64_t)chunk->memory_usage() - old_mem_usage);
            DCHECK_CHUNK(chunk);
        }
    } while (chunk->num_rows() == 0);
    return Status::OK();
}

Status OlapChunkSource::close(RuntimeState* state) {
    _prj_iter->close();
    _reader.reset();
    return Status::OK();
}

} // namespace starrocks::pipeline

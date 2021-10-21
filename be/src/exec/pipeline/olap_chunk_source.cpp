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
    const TupleDescriptor* tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    _slots = &tuple_desc->slots();

    OlapScanConjunctsManager::eval_const_conjuncts(_conjunct_ctxs, &_status);
    OlapScanConjunctsManager& cm = _conjuncts_manager;
    cm.conjunct_ctxs_ptr = &_conjunct_ctxs;
    cm.tuple_desc = tuple_desc;
    cm.obj_pool = &_obj_pool;
    cm.key_column_names = &_key_column_names;
    cm.runtime_filters = &_runtime_filters;

    const TQueryOptions& query_options = state->query_options();
    int32_t max_scan_key_num;
    if (query_options.__isset.max_scan_key_num && query_options.max_scan_key_num > 0) {
        max_scan_key_num = query_options.max_scan_key_num;
    } else {
        max_scan_key_num = config::doris_max_scan_key_num;
    }

    cm.normalize_conjuncts();
    cm.build_olap_filters();
    cm.build_scan_keys(true, max_scan_key_num);

    // 4. Build olap scanner range
    RETURN_IF_ERROR(_build_scan_range(_runtime_state));

    // 5. Init olap reader
    RETURN_IF_ERROR(_init_olap_reader(_runtime_state));
    return Status::OK();
}

Status OlapChunkSource::_build_scan_range(RuntimeState* state) {
    std::vector<std::unique_ptr<OlapScanRange>> key_ranges;
    RETURN_IF_ERROR(_conjuncts_manager.get_key_ranges(&key_ranges));
    _conjuncts_manager.get_not_push_down_conjuncts(&_not_push_down_conjuncts);

    // FixMe(kks): Ensure this logic is right.
    int scanners_per_tablet = 64;
    int num_ranges = key_ranges.size();
    int ranges_per_scanner = std::max(1, num_ranges / scanners_per_tablet);
    for (int i = 0; i < num_ranges;) {
        _scanner_ranges.push_back(key_ranges[i].get());
        i++;
        for (int j = 1;
             i < num_ranges && j < ranges_per_scanner && key_ranges[i]->end_include == key_ranges[i - 1]->end_include;
             ++j, ++i) {
            _scanner_ranges.push_back(key_ranges[i].get());
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
    std::vector<vectorized::ColumnPredicate*> preds;
    _conjuncts_manager.parse_to_column_predicates(&parser, &preds);
    for (auto* p : preds) {
        _predicate_free_pool.emplace_back(p);
        if (parser.can_pushdown(p)) {
            params->predicates.push_back(p);
        } else {
            _not_push_down_predicates.add(p);
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

    if (!_not_push_down_conjuncts.empty() || !_not_push_down_predicates.empty()) {
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

        if (!_not_push_down_predicates.empty()) {
            int64_t old_mem_usage = chunk->memory_usage();
            SCOPED_TIMER(_expr_filter_timer);
            size_t nrows = chunk->num_rows();
            _selection.resize(nrows);
            _not_push_down_predicates.evaluate(chunk, _selection.data(), 0, nrows);
            chunk->filter(_selection);
            CurrentMemTracker::consume((int64_t)chunk->memory_usage() - old_mem_usage);
            DCHECK_CHUNK(chunk);
        }
        if (!_not_push_down_conjuncts.empty()) {
            int64_t old_mem_usage = chunk->memory_usage();
            SCOPED_TIMER(_expr_filter_timer);
            ExecNode::eval_conjuncts(_not_push_down_conjuncts, chunk);
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

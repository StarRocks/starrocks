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

#include "exec/tablet_scanner.h"

#include <memory>
#include <utility>

#include "column/column_pool.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exec/olap_scan_node.h"
#include "storage/chunk_helper.h"
#include "storage/column_predicate_rewriter.h"
#include "storage/predicate_parser.h"
#include "storage/projection_iterator.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

TabletScanner::TabletScanner(OlapScanNode* parent) : _parent(parent) {}

TabletScanner::~TabletScanner() {
    if (_runtime_state != nullptr) {
        close(_runtime_state);
    }
}

Status TabletScanner::init(RuntimeState* runtime_state, const TabletScannerParams& params) {
    _runtime_state = runtime_state;
    _skip_aggregation = params.skip_aggregation;
    _need_agg_finalize = params.need_agg_finalize;
    _update_num_scan_range = params.update_num_scan_range;

    RETURN_IF_ERROR(Expr::clone_if_not_exists(runtime_state, &_pool, *params.conjunct_ctxs, &_conjunct_ctxs));
    RETURN_IF_ERROR(_get_tablet(params.scan_range));

    auto tablet_schema_ptr = _tablet->tablet_schema();
    _tablet_schema = TabletSchema::copy(tablet_schema_ptr);

    // if column_desc come from fe, reset tablet schema
    if (_parent->_olap_scan_node.__isset.columns_desc && !_parent->_olap_scan_node.columns_desc.empty() &&
        _parent->_olap_scan_node.columns_desc[0].col_unique_id >= 0) {
        _tablet_schema->clear_columns();
        for (const auto& column_desc : _parent->_olap_scan_node.columns_desc) {
            _tablet_schema->append_column(TabletColumn(column_desc));
        }
    }

    RETURN_IF_ERROR(_init_unused_output_columns(*params.unused_output_columns));
    RETURN_IF_ERROR(_init_return_columns());
    RETURN_IF_ERROR(_init_global_dicts());
    RETURN_IF_ERROR(_init_reader_params(params.key_ranges));
    Schema child_schema = ChunkHelper::convert_schema(_tablet_schema, _reader_columns);
    _reader = std::make_shared<TabletReader>(_tablet, Version(0, _version), std::move(child_schema));
    if (_reader_columns.size() == _scanner_columns.size()) {
        _prj_iter = _reader;
    } else {
        Schema output_schema = ChunkHelper::convert_schema(_tablet_schema, _scanner_columns);
        _prj_iter = new_projection_iterator(output_schema, _reader);
    }

    if (!_conjunct_ctxs.empty() || !_predicates.empty()) {
        _expr_filter_timer = ADD_TIMER(_parent->_runtime_profile, "ExprFilterTime");
    }

    DCHECK(_params.global_dictmaps != nullptr);
    RETURN_IF_ERROR(_prj_iter->init_encoded_schema(*_params.global_dictmaps));
    RETURN_IF_ERROR(_prj_iter->init_output_schema(*_params.unused_output_column_ids));

    Status st = _reader->prepare();
    if (!st.ok()) {
        std::string msg = strings::Substitute("Fail to scan tablet. error: $0, backend: $1", st.get_error_msg(),
                                              BackendOptions::get_localhost());
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    } else {
        return Status::OK();
    }
}

Status TabletScanner::open([[maybe_unused]] RuntimeState* runtime_state) {
    if (_is_open) {
        return Status::OK();
    } else {
        _is_open = true;
        Status st = _reader->open(_params);
        if (!st.ok()) {
            auto msg = strings::Substitute("Fail to scan tablet. error: $0, backend: $1", st.get_error_msg(),
                                           BackendOptions::get_localhost());
            st = Status::InternalError(msg);
            LOG(WARNING) << st;
        } else {
            RETURN_IF_ERROR(runtime_state->check_mem_limit("tablet_scanner"));
        }
        return st;
    }
}

void TabletScanner::close(RuntimeState* state) {
    if (_is_closed) {
        return;
    }
    if (_prj_iter) {
        _prj_iter->close();
    }
    update_counter();
    _reader.reset();
    _predicate_free_pool.clear();
    Expr::close(_conjunct_ctxs, state);
    // Reduce the memory usage if the the average string size is greater than 512.
    release_large_columns<BinaryColumn>(state->chunk_size() * 512);
    _is_closed = true;
}

Status TabletScanner::_get_tablet(const TInternalScanRange* scan_range) {
    TTabletId tablet_id = scan_range->tablet_id;
    _version = strtoul(scan_range->version.c_str(), nullptr, 10);

    std::string err;
    _tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, true, &err);
    if (!_tablet) {
        auto msg = strings::Substitute("Not found tablet. tablet_id: $0, error: $1", tablet_id, err);
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }
    return Status::OK();
}

Status TabletScanner::_init_reader_params(const std::vector<OlapScanRange*>* key_ranges) {
    _params.reader_type = READER_QUERY;
    _params.skip_aggregation = _skip_aggregation;
    _params.profile = _parent->_scan_profile;
    _params.runtime_state = _runtime_state;
    // If a agg node is this scan node direct parent
    // we will not call agg object finalize method in scan node,
    // to avoid the unnecessary SerDe and improve query performance
    _params.need_agg_finalize = _need_agg_finalize;
    _params.use_page_cache = _runtime_state->use_page_cache();
    auto parser = _pool.add(new PredicateParser(_tablet->tablet_schema()));
    std::vector<PredicatePtr> preds;
    RETURN_IF_ERROR(_parent->_conjuncts_manager.get_column_predicates(parser, &preds));

    // Improve for select * from table limit x, x is small
    if (preds.empty() && _parent->_limit != -1 && _parent->_limit < runtime_state()->chunk_size()) {
        _params.chunk_size = _parent->_limit;
    } else {
        _params.chunk_size = runtime_state()->chunk_size();
    }

    for (auto& p : preds) {
        if (parser->can_pushdown(p.get())) {
            _params.predicates.push_back(p.get());
        } else {
            _predicates.add(p.get());
        }
        _predicate_free_pool.emplace_back(std::move(p));
    }

    GlobalDictPredicatesRewriter not_pushdown_predicate_rewriter(_predicates, *_params.global_dictmaps);
    RETURN_IF_ERROR(not_pushdown_predicate_rewriter.rewrite_predicate(&_pool));

    // Range
    for (auto key_range : *key_ranges) {
        if (key_range->begin_scan_range.size() == 1 && key_range->begin_scan_range.get_value(0) == NEGATIVE_INFINITY) {
            continue;
        }

        _params.range = key_range->begin_include ? TabletReaderParams::RangeStartOperation::GE
                                                 : TabletReaderParams::RangeStartOperation::GT;
        _params.end_range = key_range->end_include ? TabletReaderParams::RangeEndOperation::LE
                                                   : TabletReaderParams::RangeEndOperation::LT;

        _params.start_key.push_back(key_range->begin_scan_range);
        _params.end_key.push_back(key_range->end_scan_range);
    }

    // Return columns
    if (_skip_aggregation) {
        _reader_columns = _scanner_columns;
    } else {
        for (size_t i = 0; i < _tablet_schema->num_key_columns(); i++) {
            _reader_columns.push_back(i);
        }
        for (auto index : _scanner_columns) {
            if (!_tablet_schema->column(index).is_key()) {
                _reader_columns.push_back(index);
            }
        }
    }
    // Actually only the key columns need to be sorted by id, here we check all
    // for simplicity.
    DCHECK(std::is_sorted(_reader_columns.begin(), _reader_columns.end()));

    return Status::OK();
}

Status TabletScanner::_init_return_columns() {
    for (auto slot : _parent->_tuple_desc->slots()) {
        if (!slot->is_materialized()) {
            continue;
        }
        int32_t index = _tablet_schema->field_index(slot->col_name());
        if (index < 0) {
            auto msg = strings::Substitute("Invalid column name: $0", slot->col_name());
            LOG(WARNING) << msg;
            return Status::InvalidArgument(msg);
        }
        _scanner_columns.push_back(index);
        if (!_unused_output_column_ids.count(index)) {
            _query_slots.push_back(slot);
        }
    }
    // Put key columns before non-key columns, as the `MergeIterator` and `AggregateIterator`
    // required.
    std::sort(_scanner_columns.begin(), _scanner_columns.end());
    if (_scanner_columns.empty()) {
        return Status::InternalError("failed to build storage scanner, no materialized slot!");
    }
    return Status::OK();
}

Status TabletScanner::_init_unused_output_columns(const std::vector<std::string>& unused_output_columns) {
    for (const auto& col_name : unused_output_columns) {
        int32_t index = _tablet_schema->field_index(col_name);
        if (index < 0) {
            auto msg = strings::Substitute("Invalid column name: $0", col_name);
            LOG(WARNING) << msg;
            return Status::InvalidArgument(msg);
        }
        _unused_output_column_ids.insert(index);
    }
    _params.unused_output_column_ids = &_unused_output_column_ids;
    return Status::OK();
}

// mapping a slot-column-id to schema-columnid
Status TabletScanner::_init_global_dicts() {
    const auto& global_dict_map = _runtime_state->get_query_global_dict_map();
    auto global_dict = _pool.add(new ColumnIdToGlobalDictMap());
    // mapping column id to storage column ids
    for (auto slot : _parent->_tuple_desc->slots()) {
        if (!slot->is_materialized()) {
            continue;
        }
        auto iter = global_dict_map.find(slot->id());
        if (iter != global_dict_map.end()) {
            auto& dict_map = iter->second.first;
            int32_t index = _tablet_schema->field_index(slot->col_name());
            DCHECK(index >= 0);
            global_dict->emplace(index, const_cast<GlobalDictMap*>(&dict_map));
        }
    }
    _params.global_dictmaps = global_dict;

    return Status::OK();
}

Status TabletScanner::get_chunk(RuntimeState* state, Chunk* chunk) {
    if (state->is_cancelled()) {
        return Status::Cancelled("canceled state");
    }
    SCOPED_TIMER(_parent->_scan_timer);
    do {
        if (Status status = _prj_iter->get_next(chunk); !status.ok()) {
            return status;
        }
        TRY_CATCH_ALLOC_SCOPE_START()
        for (auto slot : _query_slots) {
            size_t column_index = chunk->schema()->get_field_index_by_name(slot->col_name());
            chunk->set_slot_id_to_index(slot->id(), column_index);
        }

        if (!_predicates.empty()) {
            SCOPED_TIMER(_expr_filter_timer);
            size_t nrows = chunk->num_rows();
            _selection.resize(nrows);
            RETURN_IF_ERROR(_predicates.evaluate(chunk, _selection.data(), 0, nrows));
            chunk->filter(_selection);
            DCHECK_CHUNK(chunk);
        }
        if (!_conjunct_ctxs.empty()) {
            SCOPED_TIMER(_expr_filter_timer);
            RETURN_IF_ERROR(ExecNode::eval_conjuncts(_conjunct_ctxs, chunk));
            DCHECK_CHUNK(chunk);
        }
        TRY_CATCH_ALLOC_SCOPE_END()
    } while (chunk->num_rows() == 0);

    _update_realtime_counter(chunk);
    return Status::OK();
}

void TabletScanner::_update_realtime_counter(Chunk* chunk) {
    long num_rows = chunk->num_rows();
    const TQueryOptions& query_options = runtime_state()->query_options();
    if (query_options.__isset.load_job_type && query_options.load_job_type == TLoadJobType::INSERT_QUERY) {
        size_t bytes_usage = chunk->bytes_usage();
        runtime_state()->update_num_rows_load_from_source(num_rows);
        runtime_state()->update_num_bytes_load_from_source(bytes_usage);
    }
    COUNTER_UPDATE(_parent->_read_compressed_counter, _reader->stats().compressed_bytes_read);
    _compressed_bytes_read += _reader->stats().compressed_bytes_read;
    _reader->mutable_stats()->compressed_bytes_read = 0;

    COUNTER_UPDATE(_parent->_raw_rows_counter, _reader->stats().raw_rows_read);
    _raw_rows_read += _reader->stats().raw_rows_read;
    _reader->mutable_stats()->raw_rows_read = 0;
    _num_rows_read += num_rows;
}

void TabletScanner::update_counter() {
    if (_has_update_counter) {
        return;
    }
    if (!_reader) {
        return;
    }
    COUNTER_UPDATE(_parent->_create_seg_iter_timer, _reader->stats().create_segment_iter_ns);
    COUNTER_UPDATE(_parent->_rows_read_counter, _num_rows_read);

    COUNTER_UPDATE(_parent->_io_timer, _reader->stats().io_ns);
    COUNTER_UPDATE(_parent->_read_compressed_counter, _reader->stats().compressed_bytes_read);
    _compressed_bytes_read += _reader->stats().compressed_bytes_read;
    COUNTER_UPDATE(_parent->_decompress_timer, _reader->stats().decompress_ns);
    COUNTER_UPDATE(_parent->_read_uncompressed_counter, _reader->stats().uncompressed_bytes_read);
    COUNTER_UPDATE(_parent->bytes_read_counter(), _reader->stats().bytes_read);

    COUNTER_UPDATE(_parent->_block_load_timer, _reader->stats().block_load_ns);
    COUNTER_UPDATE(_parent->_block_load_counter, _reader->stats().blocks_load);
    COUNTER_UPDATE(_parent->_block_fetch_timer, _reader->stats().block_fetch_ns);
    COUNTER_UPDATE(_parent->_block_seek_timer, _reader->stats().block_seek_ns);

    COUNTER_UPDATE(_parent->_raw_rows_counter, _reader->stats().raw_rows_read);
    _raw_rows_read += _reader->mutable_stats()->raw_rows_read;
    COUNTER_UPDATE(_parent->_chunk_copy_timer, _reader->stats().vec_cond_chunk_copy_ns);

    COUNTER_UPDATE(_parent->_get_rowsets_timer, _reader->stats().get_rowsets_ns);
    COUNTER_UPDATE(_parent->_get_delvec_timer, _reader->stats().get_delvec_ns);
    COUNTER_UPDATE(_parent->_get_delta_column_group_timer, _reader->stats().get_delta_column_group_ns);
    COUNTER_UPDATE(_parent->_seg_init_timer, _reader->stats().segment_init_ns);

    int64_t cond_evaluate_ns = 0;
    cond_evaluate_ns += _reader->stats().vec_cond_evaluate_ns;
    cond_evaluate_ns += _reader->stats().branchless_cond_evaluate_ns;
    cond_evaluate_ns += _reader->stats().expr_cond_evaluate_ns;
    // In order to avoid exposing too detailed metrics, we still record these infos on `_pred_filter_timer`
    // When we support metric classification, we can disassemble it again.
    COUNTER_UPDATE(_parent->_pred_filter_timer, cond_evaluate_ns);
    COUNTER_UPDATE(_parent->_pred_filter_counter, _reader->stats().rows_vec_cond_filtered);
    COUNTER_UPDATE(_parent->_del_vec_filter_counter, _reader->stats().rows_del_vec_filtered);
    COUNTER_UPDATE(_parent->_seg_zm_filtered_counter, _reader->stats().segment_stats_filtered);
    COUNTER_UPDATE(_parent->_seg_rt_filtered_counter, _reader->stats().runtime_stats_filtered);

    COUNTER_UPDATE(_parent->_zm_filtered_counter, _reader->stats().rows_stats_filtered);
    COUNTER_UPDATE(_parent->_bf_filtered_counter, _reader->stats().rows_bf_filtered);
    COUNTER_UPDATE(_parent->_sk_filtered_counter, _reader->stats().rows_key_range_filtered);

    COUNTER_UPDATE(_parent->_read_pages_num_counter, _reader->stats().total_pages_num);
    COUNTER_UPDATE(_parent->_cached_pages_num_counter, _reader->stats().cached_pages_num);

    COUNTER_UPDATE(_parent->_bi_filtered_counter, _reader->stats().rows_bitmap_index_filtered);
    COUNTER_UPDATE(_parent->_bi_filter_timer, _reader->stats().bitmap_index_filter_timer);
    COUNTER_UPDATE(_parent->_block_seek_counter, _reader->stats().block_seek_num);

    COUNTER_UPDATE(_parent->_rowsets_read_count, _reader->stats().rowsets_read_count);
    COUNTER_UPDATE(_parent->_segments_read_count, _reader->stats().segments_read_count);
    COUNTER_UPDATE(_parent->_total_columns_data_page_count, _reader->stats().total_columns_data_page_count);

    COUNTER_SET(_parent->_pushdown_predicates_counter, (int64_t)_params.predicates.size());

    StarRocksMetrics::instance()->query_scan_bytes.increment(_compressed_bytes_read);
    StarRocksMetrics::instance()->query_scan_rows.increment(_raw_rows_read);

    if (_reader->stats().decode_dict_ns > 0) {
        RuntimeProfile::Counter* c = ADD_TIMER(_parent->_scan_profile, "DictDecode");
        COUNTER_UPDATE(c, _reader->stats().decode_dict_ns);
    }
    if (_reader->stats().late_materialize_ns > 0) {
        RuntimeProfile::Counter* c = ADD_TIMER(_parent->_scan_profile, "LateMaterialize");
        COUNTER_UPDATE(c, _reader->stats().late_materialize_ns);
    }
    if (_reader->stats().del_filter_ns > 0) {
        RuntimeProfile::Counter* c1 = ADD_TIMER(_parent->_scan_profile, "DeleteFilter");
        RuntimeProfile::Counter* c2 = ADD_COUNTER(_parent->_scan_profile, "DeleteFilterRows", TUnit::UNIT);
        COUNTER_UPDATE(c1, _reader->stats().del_filter_ns);
        COUNTER_UPDATE(c2, _reader->stats().rows_del_filtered);
    }
    _has_update_counter = true;
}

} // namespace starrocks

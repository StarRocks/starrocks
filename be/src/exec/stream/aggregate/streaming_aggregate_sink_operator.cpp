// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "streaming_aggregate_sink_operator.h"

#include "runtime/current_thread.h"
#include "simd/simd.h"
#include "storage/datum_row.h"

namespace starrocks::pipeline {

Status StreamingAggregateSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(_aggregator->prepare(state, state->obj_pool(), _unique_metrics.get(), _mem_tracker.get()));
    if (_imt_detail) {
        RETURN_IF_ERROR(_imt_detail->prepare(state));
        RETURN_IF_ERROR(_imt_detail->open(state));
    }
    RETURN_IF_ERROR(_aggregator->open(state));
    _num_groupby_columns = _aggregator->group_by_expr_ctxs().size();
    _num_agg_func_columns = _aggregator->agg_expr_ctxs().size();
    if (_imt_agg_result) {
        auto schema = _imt_agg_result->schema();
        _imt_agg_result_reader_params.version = _imt_agg_result->version();
        // imt's result must be: groupby columns | agg columns
        for (int i = 0; i < _num_groupby_columns; i++) {
            _imt_agg_result_reader_params.sort_key_schema.append(schema->field(i));
        }
        for (int i = 0; i < _num_agg_func_columns; i++) {
            _imt_agg_result_reader_params.output_schema.append(schema->field(i + _num_groupby_columns));
        }
        _imt_agg_result_reader = _imt_agg_result->get_table_reader(_imt_agg_result_reader_params);
    }
    return Status::OK();
}

void StreamingAggregateSinkOperator::close(RuntimeState* state) {
    _aggregator->unref(state);
    if (_imt_detail) {
        _imt_detail->close(state);
    }
    if (_imt_agg_result_reader) {
        _imt_agg_result_reader->close();
    }
    Operator::close(state);
}

Status StreamingAggregateSinkOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;

    if (_aggregator->hash_map_variant().size() == 0) {
        _aggregator->set_ht_eos();
    }

    _aggregator->sink_complete();
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> StreamingAggregateSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Not support");
}

Status StreamingAggregateSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    size_t chunk_size = chunk->num_rows();
    // step1: evaluate group-by/agg exprs
    _aggregator->update_num_input_rows(chunk_size);
    COUNTER_SET(_aggregator->input_row_count(), _aggregator->num_input_rows());
    RETURN_IF_ERROR(_aggregator->evaluate_exprs(chunk.get()));
//    if (_imt_detail) {
//        VLOG(1) << "write chunk.";
//        _imt_detail->send_chunk(state, chunk.get());
//    }

    RETURN_IF_ERROR(_push_chunk_by_force_preaggregation(chunk, chunk->num_rows()));
    return Status::OK();
}

Status StreamingAggregateSinkOperator::_push_chunk_by_force_preaggregation(const vectorized::ChunkPtr& chunk,
                                                                           const size_t chunk_size) {
    SCOPED_TIMER(_aggregator->agg_compute_timer());
    if (false) {
    }
#define HASH_MAP_METHOD(NAME)                                                                                          \
    else if (_aggregator->hash_map_variant().type == vectorized::AggHashMapVariant::Type::NAME) {                      \
        TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_map_with_selection<decltype(_aggregator->hash_map_variant().NAME)::element_type>( \
                *_aggregator->hash_map_variant().NAME, chunk_size));                                                   \
    }
    APPLY_FOR_AGG_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
    else {
        DCHECK(false);
    }

    // step2: Load state from IMT
    if (_imt_agg_result) {
        auto& not_found = _aggregator->streaming_selection();
        // TODO: Maybe push down to the aggregator?
        std::vector<vectorized::ColumnPtr> agg_columns;
        for (int i = 0; i < chunk_size; i++) {
            if (not_found[i]) {
                VLOG(1) << "key not exists:" << i << ", t_chunk output: " << chunk->debug_row(i);

                DatumRow row(_num_groupby_columns);
                ReadOption read_option;
                for (int j = 0; j < _num_groupby_columns; j++) {
                    auto groupby_column = chunk->get_column_by_id(j);
                    row.set_datum(j, groupby_column->get(i));
                    VLOG(1) << " key:" << groupby_column->get(i).get_int32();
                }

                auto imt_status_or = _imt_agg_result_reader->get_chunk(row, read_option);
                RETURN_IF_ERROR(imt_status_or.status());
                ChunkIteratorPtr iterator = imt_status_or.value();
                // Because group by is pk, the result must only one chunk.
                auto t_chunk = ChunkHelper::new_chunk(iterator->schema(), 1);
                Status status = iterator->get_next(t_chunk.get());
                VLOG(1) << "t_chunk result: " << t_chunk->num_rows();
                DCHECK_EQ(1, t_chunk->num_rows());
                for (size_t j  = 0; j < t_chunk->num_rows(); j++) {
                    VLOG(2) << "t_chunk output: " << t_chunk->debug_row(j);
                }
                DCHECK(status.is_end_of_file());
                iterator->close();
                DCHECK_EQ(_num_agg_func_columns, t_chunk->num_columns());
                if (agg_columns.empty()) {
                    for (int j = 0; j < _num_agg_func_columns; j++) {
                        agg_columns.push_back(t_chunk->get_column_by_id(j));
                    }
                } else {
                    for (int j = 0; j < _num_agg_func_columns; j++) {
                        agg_columns[j]->append(*(t_chunk->get_column_by_id(j)));
                    }
                }
            } else {
                VLOG(1) << "key exists" << i;
                for (int j = 0; j < _num_agg_func_columns; j++) {
                    agg_columns[j]->append_nulls(1);
                }
            }
        }
        _aggregator->restore_agg_states_with_selection(chunk_size, agg_columns);
    }

    // step3: Update new input datas
    if (_aggregator->is_none_group_by_exprs()) {
        _aggregator->compute_single_agg_state(chunk_size);
    } else {
        _aggregator->compute_batch_agg_states(chunk_size);
    }

    _mem_tracker->set(_aggregator->hash_map_variant().reserved_memory_usage(_aggregator->mem_pool()));
    TRY_CATCH_BAD_ALLOC(_aggregator->try_convert_to_two_level_map());

    COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());
    RETURN_IF_ERROR(_aggregator->check_has_error());
    return Status::OK();
}

} // namespace starrocks::pipeline

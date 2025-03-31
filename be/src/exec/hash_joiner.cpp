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

#include "exec/hash_joiner.h"

#include <runtime/runtime_state.h>

#include <memory>

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exec/hash_join_components.h"
#include "exec/join_hash_map.h"
#include "exec/spill/spiller.hpp"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "exprs/runtime_filter.h"
#include "gen_cpp/Metrics_types.h"
#include "pipeline/hashjoin/hash_joiner_fwd.h"
#include "runtime/current_thread.h"
#include "simd/simd.h"
#include "util/runtime_profile.h"

namespace starrocks {

void HashJoinProbeMetrics::prepare(RuntimeProfile* runtime_profile) {
    search_ht_timer = ADD_TIMER(runtime_profile, "SearchHashTableTime");
    output_build_column_timer = ADD_TIMER(runtime_profile, "OutputBuildColumnTime");
    output_probe_column_timer = ADD_TIMER(runtime_profile, "OutputProbeColumnTime");
    probe_conjunct_evaluate_timer = ADD_TIMER(runtime_profile, "ProbeConjunctEvaluateTime");
    other_join_conjunct_evaluate_timer = ADD_TIMER(runtime_profile, "OtherJoinConjunctEvaluateTime");
    where_conjunct_evaluate_timer = ADD_TIMER(runtime_profile, "WhereConjunctEvaluateTime");
    probe_counter = ADD_COUNTER(runtime_profile, "probeCount", TUnit::UNIT);
    partition_probe_overhead = ADD_TIMER(runtime_profile, "PartitionProbeOverhead");
}

void HashJoinBuildMetrics::prepare(RuntimeProfile* runtime_profile) {
    copy_right_table_chunk_timer = ADD_TIMER(runtime_profile, "CopyRightTableChunkTime");
    build_ht_timer = ADD_TIMER(runtime_profile, "BuildHashTableTime");
    build_runtime_filter_timer = ADD_TIMER(runtime_profile, "RuntimeFilterBuildTime");
    build_conjunct_evaluate_timer = ADD_TIMER(runtime_profile, "BuildConjunctEvaluateTime");
    build_buckets_counter = ADD_COUNTER(runtime_profile, "BuildBuckets", TUnit::UNIT);
    runtime_filter_num = ADD_COUNTER(runtime_profile, "RuntimeFilterNum", TUnit::UNIT);
    build_keys_per_bucket = ADD_COUNTER(runtime_profile, "BuildKeysPerBucket%", TUnit::UNIT);
    hash_table_memory_usage = ADD_COUNTER(runtime_profile, "HashTableMemoryUsage", TUnit::BYTES);
    partial_runtime_bloom_filter_bytes =
            ADD_COUNTER(runtime_profile, "PartialRuntimeMembershipFilterBytes", TUnit::BYTES);
    partition_nums = ADD_COUNTER(runtime_profile, "PartitionNums", TUnit::UNIT);
}

HashJoiner::HashJoiner(const HashJoinerParam& param)
        : _hash_join_node(param._hash_join_node),
          _pool(param._pool),
          _join_type(param._hash_join_node.join_op),
          _is_null_safes(param._is_null_safes),
          _build_expr_ctxs(param._build_expr_ctxs),
          _probe_expr_ctxs(param._probe_expr_ctxs),
          _other_join_conjunct_ctxs(param._other_join_conjunct_ctxs),
          _conjunct_ctxs(param._conjunct_ctxs),
          _build_row_descriptor(param._build_row_descriptor),
          _probe_row_descriptor(param._probe_row_descriptor),
          _build_node_type(param._build_node_type),
          _probe_node_type(param._probe_node_type),
          _build_conjunct_ctxs_is_empty(param._build_conjunct_ctxs_is_empty),
          _build_output_slots(param._build_output_slots),
          _probe_output_slots(param._probe_output_slots),
          _build_runtime_filters(param._build_runtime_filters.begin(), param._build_runtime_filters.end()),
          _mor_reader_mode(param._mor_reader_mode),
          _enable_late_materialization(param._enable_late_materialization),
          _is_skew_join(param._is_skew_join) {
    _is_push_down = param._hash_join_node.is_push_down;
    if (_join_type == TJoinOp::LEFT_ANTI_JOIN && param._hash_join_node.is_rewritten_from_not_in) {
        _join_type = TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN;
    }

    _build_runtime_filters_from_planner = false;
    if (param._hash_join_node.__isset.build_runtime_filters_from_planner) {
        _build_runtime_filters_from_planner = param._hash_join_node.build_runtime_filters_from_planner;
    }

    HashJoinBuildOptions build_options;
    build_options.enable_partitioned_hash_join = param._enable_partition_hash_join;

    _hash_join_builder = HashJoinBuilderFactory::create(_pool, build_options, *this);
    _hash_join_prober = _pool->add(new HashJoinProber(*this));
    _build_metrics = _pool->add(new HashJoinBuildMetrics());
    _probe_metrics = _pool->add(new HashJoinProbeMetrics());
}

Status HashJoiner::prepare_builder(RuntimeState* state, RuntimeProfile* runtime_profile) {
    if (_runtime_state == nullptr) {
        _runtime_state = state;
    }

    if (_hash_join_node.__isset.sql_join_predicates) {
        runtime_profile->add_info_string("JoinPredicates", _hash_join_node.sql_join_predicates);
    }
    if (_hash_join_node.__isset.sql_predicates) {
        runtime_profile->add_info_string("Predicates", _hash_join_node.sql_predicates);
    }

    runtime_profile->add_info_string("DistributionMode", to_string(_hash_join_node.distribution_mode));
    runtime_profile->add_info_string("JoinType", to_string(_join_type));

    _build_metrics->prepare(runtime_profile);

    _init_hash_table_param(&_hash_table_param);
    _hash_join_builder->create(hash_table_param());

    _output_probe_column_count = _hash_join_builder->get_output_probe_column_count();
    _output_build_column_count = _hash_join_builder->get_output_build_column_count();

    return Status::OK();
}

// it is ok that prepare_builder is done whether before this or not
Status HashJoiner::prepare_prober(RuntimeState* state, RuntimeProfile* runtime_profile) {
    if (_runtime_state == nullptr) {
        _runtime_state = state;
    }

    runtime_profile->add_info_string("DistributionMode", to_string(_hash_join_node.distribution_mode));
    runtime_profile->add_info_string("JoinType", to_string(_join_type));
    _probe_metrics->prepare(runtime_profile);

    _hash_table_param.search_ht_timer = probe_metrics().search_ht_timer;
    _hash_table_param.output_build_column_timer = probe_metrics().output_build_column_timer;
    _hash_table_param.output_probe_column_timer = probe_metrics().output_probe_column_timer;
    _hash_table_param.probe_counter = probe_metrics().probe_counter;

    return Status::OK();
}

void HashJoiner::_init_hash_table_param(HashTableParam* param) {
    param->with_other_conjunct = !_other_join_conjunct_ctxs.empty();
    param->join_type = _join_type;
    param->build_row_desc = &_build_row_descriptor;
    param->probe_row_desc = &_probe_row_descriptor;
    param->build_output_slots = _build_output_slots;
    param->probe_output_slots = _probe_output_slots;
    param->mor_reader_mode = _mor_reader_mode;
    param->enable_late_materialization = _enable_late_materialization;

    std::set<SlotId> predicate_slots;
    for (ExprContext* expr_context : _conjunct_ctxs) {
        std::vector<SlotId> expr_slots;
        expr_context->root()->get_slot_ids(&expr_slots);
        predicate_slots.insert(expr_slots.begin(), expr_slots.end());
    }
    for (ExprContext* expr_context : _other_join_conjunct_ctxs) {
        std::vector<SlotId> expr_slots;
        expr_context->root()->get_slot_ids(&expr_slots);
        predicate_slots.insert(expr_slots.begin(), expr_slots.end());
    }
    param->predicate_slots = std::move(predicate_slots);

    for (auto i = 0; i < _build_expr_ctxs.size(); i++) {
        Expr* expr = _build_expr_ctxs[i]->root();
        if (expr->is_slotref()) {
            param->join_keys.emplace_back(JoinKeyDesc{&expr->type(), _is_null_safes[i], down_cast<ColumnRef*>(expr)});
        } else {
            param->join_keys.emplace_back(JoinKeyDesc{&expr->type(), _is_null_safes[i], nullptr});
        }
    }
}
Status HashJoiner::append_chunk_to_ht(const ChunkPtr& chunk) {
    if (_phase != HashJoinPhase::BUILD) {
        return Status::OK();
    }
    if (!chunk || chunk->is_empty()) {
        return Status::OK();
    }

    update_build_rows(chunk->num_rows());
    return _hash_join_builder->append_chunk(chunk);
}

Status HashJoiner::append_chunk_to_spill_buffer(RuntimeState* state, const ChunkPtr& chunk) {
    update_build_rows(chunk->num_rows());
    RETURN_IF_ERROR(spiller()->spill(state, chunk, TRACKER_WITH_SPILLER_GUARD(state, spiller())));
    return Status::OK();
}

Status HashJoiner::append_spill_task(RuntimeState* state, std::function<StatusOr<ChunkPtr>()>& spill_task) {
    Status st;
    while (!spiller()->is_full()) {
        auto chunk_st = spill_task();
        if (chunk_st.ok()) {
            RETURN_IF_ERROR(spiller()->spill(state, chunk_st.value(), TRACKER_WITH_SPILLER_GUARD(state, spiller())));
        } else if (chunk_st.status().is_end_of_file()) {
            return Status::OK();
        } else {
            return chunk_st.status();
        }
    }
    // status
    _spill_channel->add_spill_task({spill_task});
    return Status::OK();
}

Status HashJoiner::build_ht(RuntimeState* state) {
    if (_phase == HashJoinPhase::BUILD) {
        RETURN_IF_ERROR(_hash_join_builder->build(state));

        size_t bucket_size = 0;
        float avg_keys_per_bucket = 0;
        _hash_join_builder->get_build_info(&bucket_size, &avg_keys_per_bucket);
        COUNTER_SET(build_metrics().build_buckets_counter, static_cast<int64_t>(bucket_size));
        COUNTER_SET(build_metrics().build_keys_per_bucket, static_cast<int64_t>(100 * avg_keys_per_bucket));
    }

    return Status::OK();
}

bool HashJoiner::need_input() const {
    // when _buffered_chunk accumulates several chunks to form into a large enough chunk, it is moved into
    // _probe_chunk for probe operations.
    return _phase == HashJoinPhase::PROBE && _hash_join_prober->need_input();
}

bool HashJoiner::has_output() const {
    if (_phase == HashJoinPhase::BUILD) {
        return false;
    }

    if (_phase == HashJoinPhase::PROBE) {
        return !_hash_join_prober->probe_chunk_empty();
    }

    if (_phase == HashJoinPhase::POST_PROBE) {
        // Only RIGHT ANTI-JOIN, RIGHT OUTER-JOIN, FULL OUTER-JOIN has HashJoinPhase::POST_PROBE,
        // in this phase, has_output() returns true until HashJoiner enters into HashJoinPhase::DONE.
        return true;
    }

    return false;
}

Status HashJoiner::push_chunk(RuntimeState* state, ChunkPtr&& chunk) {
    DCHECK(chunk && !chunk->is_empty());
    return _hash_join_prober->push_probe_chunk(state, std::move(chunk));
}

Status HashJoiner::probe_input_finished(RuntimeState* state) {
    return _hash_join_prober->on_input_finished(state);
}

StatusOr<ChunkPtr> HashJoiner::pull_chunk(RuntimeState* state) {
    DCHECK(_phase != HashJoinPhase::BUILD);
    return _pull_probe_output_chunk(state);
}

StatusOr<ChunkPtr> HashJoiner::_pull_probe_output_chunk(RuntimeState* state) {
    DCHECK(_phase != HashJoinPhase::BUILD);

    auto chunk = std::make_shared<Chunk>();

    if (_phase == HashJoinPhase::PROBE || !_hash_join_prober->probe_chunk_empty()) {
        ASSIGN_OR_RETURN(chunk, _hash_join_prober->probe_chunk(state))
        return chunk;
    }

    if (_phase == HashJoinPhase::POST_PROBE) {
        if (!_need_post_probe()) {
            enter_eos_phase();
            return chunk;
        }

        bool has_remain = false;
        ASSIGN_OR_RETURN(chunk, _hash_join_prober->probe_remain(state, &has_remain))

        if (!has_remain) {
            enter_eos_phase();
        }

        return chunk;
    }

    return chunk;
}

void HashJoiner::close(RuntimeState* state) {
    _hash_join_builder->close();
}

Status HashJoiner::create_runtime_filters(RuntimeState* state) {
    if (_phase != HashJoinPhase::BUILD) {
        return Status::OK();
    }

    uint64_t runtime_join_filter_pushdown_limit = runtime_bloom_filter_row_limit();
    size_t ht_row_count = _hash_join_builder->hash_table_row_count();

    if (_is_push_down) {
        if (_probe_node_type == TPlanNodeType::EXCHANGE_NODE && _build_node_type == TPlanNodeType::EXCHANGE_NODE) {
            _is_push_down = false;
        } else if (ht_row_count > runtime_join_filter_pushdown_limit) {
            _is_push_down = false;
        }

        if (_is_push_down || !_build_conjunct_ctxs_is_empty) {
            // In filter could be used to fast compute segment row range in storage engine
            RETURN_IF_ERROR(_create_runtime_in_filters(state));
        }
    }

    // it's quite critical to put publish runtime filters before short-circuit of
    // "inner-join with empty right table". because for global runtime filter
    // merge node is waiting for all partitioned runtime filter, so even hash row count is zero
    // we still have to build it.
    return _create_runtime_bloom_filters(state, runtime_join_filter_pushdown_limit);
}

void HashJoiner::reference_hash_table(HashJoiner* src_join_builder_cntl) {
    if (this == src_join_builder_cntl) {
        _hash_join_prober->attach(src_join_builder_cntl->hash_join_builder(), probe_metrics());
    } else {
        src_join_builder_cntl->hash_join_builder()->clone_readable(this->hash_join_builder());
        _hash_join_prober->attach(this->hash_join_builder(), probe_metrics());
        _hash_table_param = src_join_builder_cntl->hash_table_param();

        // _hash_table_build_rows is root truth, it used to by _short_circuit_break().
        _hash_table_build_rows = src_join_builder_cntl->_hash_table_build_rows;
        _output_probe_column_count = src_join_builder_cntl->_output_probe_column_count;
        _output_build_column_count = src_join_builder_cntl->_output_build_column_count;

        // _phase may be EOS.
        auto old_phase = HashJoinPhase::BUILD;
        _phase.compare_exchange_strong(old_phase, src_join_builder_cntl->_phase.load());
    }
    _has_referenced_hash_table = true;
}

void HashJoiner::set_prober_finished() {
    if (++_num_finished_probers == _num_probers) {
        (void)set_finished();
    }
}
void HashJoiner::decr_prober(RuntimeState* state) {
    // HashJoinProbeOperator may be instantiated lazily, so join_builder is ref for prober
    // in HashJoinBuildOperator::prepare and unref when all the probers are closed here.
    if (++_num_closed_probers == _num_probers) {
        unref(state);
    }
}

float HashJoiner::avg_keys_per_bucket() const {
    size_t bucket_size = 0;
    float avg_keys_per_bucket = 0;
    _hash_join_builder->get_build_info(&bucket_size, &avg_keys_per_bucket);
    return avg_keys_per_bucket;
}

Status HashJoiner::reset_probe(starrocks::RuntimeState* state) {
    _phase = HashJoinPhase::PROBE;
    // _short_circuit_break maybe set _phase to HashJoinPhase::EOS
    _short_circuit_break();
    if (_phase == HashJoinPhase::EOS) {
        return Status::OK();
    }

    _hash_join_prober->reset(state);

    return Status::OK();
}

Status HashJoiner::_calc_filter_for_other_conjunct(ChunkPtr* chunk, Filter& filter, bool& filter_all, bool& hit_all) {
    filter_all = false;
    hit_all = false;
    filter.assign((*chunk)->num_rows(), 1);

    for (auto* ctx : _other_join_conjunct_ctxs) {
        ASSIGN_OR_RETURN(ColumnPtr column, ctx->evaluate((*chunk).get()))
        size_t true_count = ColumnHelper::count_true_with_notnull(column);

        if (true_count == column->size()) {
            // all hit, skip
            continue;
        } else if (0 == true_count) {
            // all not hit, return
            filter_all = true;
            filter.assign((*chunk)->num_rows(), 0);
            break;
        } else {
            bool all_zero = false;
            ColumnHelper::merge_two_filters(column, &filter, &all_zero);
            if (all_zero) {
                filter_all = true;
                break;
            }
        }
    }

    if (!filter_all) {
        int zero_count = SIMD::count_zero(filter.data(), filter.size());
        if (zero_count == 0) {
            hit_all = true;
        }
    }

    return Status::OK();
}

void HashJoiner::_process_row_for_other_conjunct(ChunkPtr* chunk, size_t start_column, size_t column_count,
                                                 bool filter_all, bool hit_all, const Filter& filter) {
    if (filter_all) {
        for (size_t i = start_column; i < start_column + column_count; i++) {
            auto* null_column = ColumnHelper::as_raw_column<NullableColumn>((*chunk)->columns()[i]);
            auto& null_data = null_column->mutable_null_column()->get_data();
            for (size_t j = 0; j < (*chunk)->num_rows(); j++) {
                null_data[j] = 1;
                null_column->set_has_null(true);
            }
        }
    } else {
        if (hit_all) {
            return;
        }

        for (size_t i = start_column; i < start_column + column_count; i++) {
            auto* null_column = ColumnHelper::as_raw_column<NullableColumn>((*chunk)->columns()[i]);
            auto& null_data = null_column->mutable_null_column()->get_data();
            for (size_t j = 0; j < filter.size(); j++) {
                if (filter[j] == 0) {
                    null_data[j] = 1;
                    null_column->set_has_null(true);
                }
            }
        }
    }
}

Status HashJoiner::_process_outer_join_with_other_conjunct(ChunkPtr* chunk, size_t start_column, size_t column_count,
                                                           JoinHashTable& hash_table) {
    bool filter_all = false;
    bool hit_all = false;
    Filter filter;

    RETURN_IF_ERROR(_calc_filter_for_other_conjunct(chunk, filter, filter_all, hit_all));
    _process_row_for_other_conjunct(chunk, start_column, column_count, filter_all, hit_all, filter);

    hash_table.remove_duplicate_index(&filter);
    (*chunk)->filter(filter);

    return Status::OK();
}

Status HashJoiner::_process_semi_join_with_other_conjunct(ChunkPtr* chunk, JoinHashTable& hash_table) {
    bool filter_all = false;
    bool hit_all = false;
    Filter filter;

    RETURN_IF_ERROR(_calc_filter_for_other_conjunct(chunk, filter, filter_all, hit_all));

    hash_table.remove_duplicate_index(&filter);

    (*chunk)->filter(filter);

    return Status::OK();
}

Status HashJoiner::_process_right_anti_join_with_other_conjunct(ChunkPtr* chunk, JoinHashTable& hash_table) {
    bool filter_all = false;
    bool hit_all = false;
    Filter filter;

    RETURN_IF_ERROR(_calc_filter_for_other_conjunct(chunk, filter, filter_all, hit_all));
    hash_table.remove_duplicate_index(&filter);

    (*chunk)->set_num_rows(0);

    return Status::OK();
}

Status HashJoiner::_process_other_conjunct(ChunkPtr* chunk, JoinHashTable& hash_table) {
    SCOPED_TIMER(probe_metrics().other_join_conjunct_evaluate_timer);
    switch (_join_type) {
    case TJoinOp::LEFT_OUTER_JOIN:
    case TJoinOp::FULL_OUTER_JOIN:
        return _process_outer_join_with_other_conjunct(chunk, _output_probe_column_count, _output_build_column_count,
                                                       hash_table);
    case TJoinOp::RIGHT_OUTER_JOIN:
    case TJoinOp::LEFT_SEMI_JOIN:
    case TJoinOp::LEFT_ANTI_JOIN:
    case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN:
    case TJoinOp::RIGHT_SEMI_JOIN:
        return _process_semi_join_with_other_conjunct(chunk, hash_table);
    case TJoinOp::RIGHT_ANTI_JOIN:
        return _process_right_anti_join_with_other_conjunct(chunk, hash_table);
    default:
        // the other join conjunct for inner join will be convert to other predicate
        // so can't reach here
        RETURN_IF_ERROR(ExecNode::eval_conjuncts(_other_join_conjunct_ctxs, (*chunk).get()));
    }
    return Status::OK();
}

Status HashJoiner::_process_where_conjunct(ChunkPtr* chunk) {
    SCOPED_TIMER(probe_metrics().where_conjunct_evaluate_timer);
    return ExecNode::eval_conjuncts(_conjunct_ctxs, (*chunk).get());
}

Status HashJoiner::_create_runtime_in_filters(RuntimeState* state) {
    SCOPED_TIMER(build_metrics().build_runtime_filter_timer);
    size_t ht_row_count = get_ht_row_count();

    if (ht_row_count > config::max_pushdown_conditions_per_column) {
        return Status::OK();
    }

    std::vector<JoinHashTable*> hash_tables;
    _hash_join_builder->visitHt([&hash_tables](JoinHashTable* ht) { hash_tables.push_back(ht); });

    if (ht_row_count > 0) {
        size_t size = _build_expr_ctxs.size();
        for (size_t i = 0; i < size; i++) {
            Expr* probe_expr = _probe_expr_ctxs[i]->root();
            DCHECK(probe_expr->is_slotref());
            // create and fill runtime in filter.
            VectorizedInConstPredicateBuilder builder(state, _pool, probe_expr);
            builder.set_eq_null(_is_null_safes[i]);
            builder.use_as_join_runtime_filter();
            Status st = builder.create();
            if (!st.ok()) {
                _runtime_in_filters.push_back(nullptr);
                continue;
            }

            for (auto* ht : hash_tables) {
                ColumnPtr column = ht->get_key_columns()[i];
                if (probe_expr->type().is_string_type()) {
                    _string_key_columns.emplace_back(column);
                }
                builder.add_values(column, kHashJoinKeyColumnOffset);
            }

            _runtime_in_filters.push_back(builder.get_in_const_predicate());
        }
    }

    COUNTER_UPDATE(build_metrics().runtime_filter_num, static_cast<int64_t>(_runtime_in_filters.size()));
    return Status::OK();
}

Status HashJoiner::_create_runtime_bloom_filters(RuntimeState* state, int64_t limit) {
    SCOPED_TIMER(build_metrics().build_runtime_filter_timer);
    size_t ht_row_count = get_ht_row_count();
    std::vector<JoinHashTable*> hash_tables;
    _hash_join_builder->visitHt([&hash_tables](JoinHashTable* ht) { hash_tables.emplace_back(ht); });

    for (auto* rf_desc : _build_runtime_filters) {
        rf_desc->set_is_pipeline(true);
        // skip if it does not have consumer.
        if (!rf_desc->has_consumer()) {
            _runtime_bloom_filter_build_params.emplace_back();
            continue;
        }
        if (!rf_desc->has_remote_targets() && ht_row_count > limit) {
            _runtime_bloom_filter_build_params.emplace_back();
            continue;
        }

        int expr_order = rf_desc->build_expr_order();
        bool eq_null = _is_null_safes[expr_order];
        bool is_empty = false;
        Columns columns;

        for (auto* ht : hash_tables) {
            ColumnPtr column = ht->get_key_columns()[expr_order];
            is_empty |= column == nullptr || column->empty();
            columns.push_back(column);
        }

        TypeDescriptor type_descriptor = _build_expr_ctxs[expr_order]->root()->type();

        MutableRuntimeFilterPtr filter = nullptr;
        auto multi_partitioned = rf_desc->layout().pipeline_level_multi_partitioned();
        multi_partitioned |= rf_desc->num_colocate_partition() > 0;
        if (multi_partitioned) {
            LogicalType build_type = rf_desc->build_expr_type();
            filter = std::shared_ptr<RuntimeFilter>(
                    RuntimeFilterHelper::create_runtime_bloom_filter(nullptr, build_type, rf_desc->join_mode()));
            if (filter == nullptr) {
                _runtime_bloom_filter_build_params.emplace_back();
                continue;
            }
            filter->get_membership_filter()->init(ht_row_count);
            RETURN_IF_ERROR(RuntimeFilterHelper::fill_runtime_filter(columns, build_type, filter.get(),
                                                                     kHashJoinKeyColumnOffset, eq_null));
        }

        _runtime_bloom_filter_build_params.emplace_back(pipeline::RuntimeMembershipFilterBuildParam(
                multi_partitioned, eq_null, is_empty, std::move(columns), std::move(filter), type_descriptor));
    }
    return Status::OK();
}

} // namespace starrocks

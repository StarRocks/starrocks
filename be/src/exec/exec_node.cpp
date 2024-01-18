// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/exec_node.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/exec_node.h"

#include <thrift/protocol/TDebugProtocol.h>
#include <unistd.h>

#include <sstream>

#include "column/column_helper.h"
#include "common/compiler_util.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/empty_set_node.h"
#include "exec/exchange_node.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/runtime_filter_types.h"
#include "exec/select_node.h"
#include "exec/vectorized/aggregate/aggregate_blocking_node.h"
#include "exec/vectorized/aggregate/aggregate_streaming_node.h"
#include "exec/vectorized/aggregate/distinct_blocking_node.h"
#include "exec/vectorized/aggregate/distinct_streaming_node.h"
#include "exec/vectorized/analytic_node.h"
#include "exec/vectorized/assert_num_rows_node.h"
#include "exec/vectorized/connector_scan_node.h"
#include "exec/vectorized/cross_join_node.h"
#include "exec/vectorized/dict_decode_node.h"
#include "exec/vectorized/except_node.h"
#include "exec/vectorized/file_scan_node.h"
#include "exec/vectorized/hash_join_node.h"
#include "exec/vectorized/intersect_node.h"
#include "exec/vectorized/olap_meta_scan_node.h"
#include "exec/vectorized/olap_scan_node.h"
#include "exec/vectorized/project_node.h"
#include "exec/vectorized/repeat_node.h"
#include "exec/vectorized/schema_scan_node.h"
#include "exec/vectorized/table_function_node.h"
#include "exec/vectorized/topn_node.h"
#include "exec/vectorized/union_node.h"
#include "exprs/expr_context.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_filter_cache.h"
#include "runtime/runtime_state.h"
#include "simd/simd.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"

namespace starrocks {

const std::string ExecNode::ROW_THROUGHPUT_COUNTER = "RowsReturnedRate";

ExecNode::ExecNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : _id(tnode.node_id),
          _type(tnode.node_type),
          _pool(pool),
          _tuple_ids(tnode.row_tuples),
          _row_descriptor(descs, tnode.row_tuples, tnode.nullable_tuples),
          _resource_profile(tnode.resource_profile),
          _debug_phase(TExecNodePhase::INVALID),
          _debug_action(TDebugAction::WAIT),
          _limit(tnode.limit),
          _num_rows_returned(0),
          _rows_returned_counter(nullptr),
          _rows_returned_rate(nullptr),
          _memory_used_counter(nullptr),
          _use_vectorized(tnode.use_vectorized),
          _runtime_state(nullptr),
          _is_closed(false) {
    init_runtime_profile(print_plan_node_type(tnode.node_type));
}

ExecNode::~ExecNode() {
    if (runtime_state() != nullptr) {
        ExecNode::close(_runtime_state);
    }
}

void ExecNode::push_down_predicate(RuntimeState* state, std::list<ExprContext*>* expr_ctxs) {
    if (_type != TPlanNodeType::AGGREGATION_NODE) {
        for (auto& i : _children) {
            i->push_down_predicate(state, expr_ctxs);
            if (expr_ctxs->size() == 0) {
                return;
            }
        }
    }

    auto iter = expr_ctxs->begin();
    while (iter != expr_ctxs->end()) {
        if ((*iter)->root()->is_bound(_tuple_ids)) {
            (*iter)->prepare(state);
            (*iter)->open(state);
            _conjunct_ctxs.push_back(*iter);
            iter = expr_ctxs->erase(iter);
        } else {
            ++iter;
        }
    }
}

void ExecNode::push_down_tuple_slot_mappings(RuntimeState* state,
                                             const std::vector<TupleSlotMapping>& parent_mappings) {
    _tuple_slot_mappings = parent_mappings;
    for (auto& child : _children) {
        child->push_down_tuple_slot_mappings(state, _tuple_slot_mappings);
    }
}

void ExecNode::push_down_join_runtime_filter(RuntimeState* state, vectorized::RuntimeFilterProbeCollector* collector) {
    if (collector->empty()) return;
    if (_type != TPlanNodeType::AGGREGATION_NODE && _type != TPlanNodeType::ANALYTIC_EVAL_NODE) {
        push_down_join_runtime_filter_to_children(state, collector);
    }
    _runtime_filter_collector.push_down(collector, _tuple_ids, _local_rf_waiting_set);
}

void ExecNode::push_down_join_runtime_filter_to_children(RuntimeState* state,
                                                         vectorized::RuntimeFilterProbeCollector* collector) {
    for (auto& i : _children) {
        i->push_down_join_runtime_filter(state, collector);
        if (collector->size() == 0) {
            return;
        }
    }
}

void ExecNode::register_runtime_filter_descriptor(RuntimeState* state,
                                                  vectorized::RuntimeFilterProbeDescriptor* rf_desc) {
    rf_desc->set_probe_plan_node_id(_id);
    _runtime_filter_collector.add_descriptor(rf_desc);
    ExecEnv::GetInstance()->add_rf_event({state->query_id(), rf_desc->filter_id(), BackendOptions::get_localhost(),
                                          strings::Substitute("REGISTER_GRF(probe_node_id=$0", _id)});
    state->runtime_filter_port()->add_listener(rf_desc);
}

Status ExecNode::init_join_runtime_filters(const TPlanNode& tnode, RuntimeState* state) {
    _runtime_filter_collector.set_plan_node_id(_id);
    if (state != nullptr && tnode.__isset.probe_runtime_filters) {
        for (const auto& desc : tnode.probe_runtime_filters) {
            vectorized::RuntimeFilterProbeDescriptor* rf_desc =
                    _pool->add(new vectorized::RuntimeFilterProbeDescriptor());
            RETURN_IF_ERROR(rf_desc->init(_pool, desc, _id));
            register_runtime_filter_descriptor(state, rf_desc);
        }
    }
    if (state != nullptr && state->query_options().__isset.runtime_filter_wait_timeout_ms) {
        _runtime_filter_collector.set_wait_timeout_ms(state->query_options().runtime_filter_wait_timeout_ms);
    }
    if (state != nullptr && state->query_options().__isset.runtime_filter_scan_wait_time_ms) {
        _runtime_filter_collector.set_scan_wait_timeout_ms(state->query_options().runtime_filter_scan_wait_time_ms);
    }
    if (tnode.__isset.filter_null_value_columns) {
        _filter_null_value_columns = tnode.filter_null_value_columns;
    }
    return Status::OK();
}

void ExecNode::init_runtime_filter_for_operator(OperatorFactory* op, pipeline::PipelineBuilderContext* context,
                                                const RcRfProbeCollectorPtr& rc_rf_probe_collector) {
    op->init_runtime_filter(context->fragment_context()->runtime_filter_hub(), this->get_tuple_ids(),
                            this->local_rf_waiting_set(), this->row_desc(), rc_rf_probe_collector,
                            _filter_null_value_columns, _tuple_slot_mappings);
}

Status ExecNode::init(const TPlanNode& tnode, RuntimeState* state) {
    VLOG(2) << "ExecNode init:\n" << apache::thrift::ThriftDebugString(tnode);
    _runtime_state = state;
    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, tnode.conjuncts, &_conjunct_ctxs));
    RETURN_IF_ERROR(init_join_runtime_filters(tnode, state));
    if (tnode.__isset.local_rf_waiting_set) {
        _local_rf_waiting_set = tnode.local_rf_waiting_set;
    }
    return Status::OK();
}

Status ExecNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::PREPARE));
    DCHECK(_runtime_profile.get() != nullptr);
    _rows_returned_counter = ADD_COUNTER(_runtime_profile, "RowsReturned", TUnit::UNIT);
    _rows_returned_rate = runtime_profile()->add_derived_counter(
            ROW_THROUGHPUT_COUNTER, TUnit::UNIT_PER_SECOND,
            [capture0 = _rows_returned_counter, capture1 = runtime_profile()->total_time_counter()] {
                return RuntimeProfile::units_per_second(capture0, capture1);
            },
            "");
    _mem_tracker.reset(new MemTracker(_runtime_profile.get(), -1, _runtime_profile->name(), nullptr));
    RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, state));
    RETURN_IF_ERROR(_runtime_filter_collector.prepare(state, row_desc(), _runtime_profile.get()));

    // TODO(zc):
    // AddExprCtxsToFree(_conjunct_ctxs);

    for (auto& i : _children) {
        RETURN_IF_ERROR(i->prepare(state));
    }

    return Status::OK();
}

Status ExecNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, state));
    RETURN_IF_ERROR(_runtime_filter_collector.open(state));
    push_down_join_runtime_filter(state, &_runtime_filter_collector);
    _runtime_filter_collector.wait(is_scan_node());
    return Status::OK();
}

Status ExecNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    return Status::NotSupported("Don't support vector query engine");
}

pipeline::OpFactories ExecNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    pipeline::OpFactories operators;
    return operators;
}

// specific_get_next to get chunks, It's implemented in subclass.
// if pre chunk is nullptr current chunk size >= chunk_size / 2, direct return the current chunk
// if pre chunk is not nullptr and pre chunk size + cur chunk size <= 4096, merge the two chunk
// if pre chunk is not nullptr and pre chunk size + cur chunk size > 4096, return pre chunk
Status ExecNode::get_next_big_chunk(RuntimeState* state, ChunkPtr* chunk, bool* eos, ChunkPtr& pre_output_chunk,
                                    const std::function<Status(RuntimeState*, ChunkPtr*, bool*)>& specific_get_next) {
    size_t chunk_size = state->chunk_size();

    while (true) {
        bool cur_eos = false;
        ChunkPtr cur_chunk = nullptr;

        RETURN_IF_ERROR(specific_get_next(state, &cur_chunk, &cur_eos));
        TRY_CATCH_ALLOC_SCOPE_START()
        if (cur_eos) {
            if (pre_output_chunk != nullptr) {
                *eos = false;
                *chunk = std::move(pre_output_chunk);
                return Status::OK();
            } else {
                *eos = true;
                return Status::OK();
            }
        } else {
            size_t cur_size = cur_chunk->num_rows();
            if (cur_size <= 0) {
                continue;
            } else if (pre_output_chunk == nullptr) {
                if (cur_size >= chunk_size / 2) {
                    // the probe chunk size of read from right child >= chunk_size, direct return
                    *eos = false;
                    *chunk = std::move(cur_chunk);
                    return Status::OK();
                } else {
                    pre_output_chunk = std::move(cur_chunk);
                    continue;
                }
            } else {
                if (cur_size + pre_output_chunk->num_rows() > chunk_size) {
                    // the two chunk size > chunk_size, return the first reserved chunk
                    *eos = false;
                    *chunk = std::move(pre_output_chunk);
                    pre_output_chunk = std::move(cur_chunk);
                    return Status::OK();
                } else {
                    // TODO: copy the small chunk to big chunk
                    vectorized::Columns& dest_columns = pre_output_chunk->columns();
                    vectorized::Columns& src_columns = cur_chunk->columns();
                    size_t num_rows = cur_size;
                    // copy the new read chunk to the reserved
                    for (size_t i = 0; i < dest_columns.size(); i++) {
                        dest_columns[i]->append(*src_columns[i], 0, num_rows);
                    }
                    continue;
                }
            }
        }
        TRY_CATCH_ALLOC_SCOPE_END()
    }
}

Status ExecNode::reset(RuntimeState* state) {
    _num_rows_returned = 0;
    for (auto& i : _children) {
        RETURN_IF_ERROR(i->reset(state));
    }
    return Status::OK();
}

Status ExecNode::collect_query_statistics(QueryStatistics* statistics) {
    DCHECK(statistics != nullptr);
    for (auto child_node : _children) {
        child_node->collect_query_statistics(statistics);
    }
    return Status::OK();
}

Status ExecNode::close(RuntimeState* state) {
    if (_is_closed) {
        return Status::OK();
    }
    _is_closed = true;
    exec_debug_action(TExecNodePhase::CLOSE);

    if (_rows_returned_counter != nullptr) {
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    }

    Status result;
    for (auto& i : _children) {
        auto st = i->close(state);
        if (result.ok() && !st.ok()) {
            result = st;
        }
    }

    Expr::close(_conjunct_ctxs, state);
    _runtime_filter_collector.close(state);

    return result;
}

Status ExecNode::create_tree(RuntimeState* state, ObjectPool* pool, const TPlan& plan, const DescriptorTbl& descs,
                             ExecNode** root) {
    if (plan.nodes.size() == 0) {
        *root = nullptr;
        return Status::OK();
    }

    int node_idx = 0;
    RETURN_IF_ERROR(create_tree_helper(state, pool, plan.nodes, descs, nullptr, &node_idx, root));

    if (node_idx + 1 != plan.nodes.size()) {
        return Status::InternalError("Plan tree only partially reconstructed. Not all thrift nodes were used.");
    }

    return Status::OK();
}

Status ExecNode::create_tree_helper(RuntimeState* state, ObjectPool* pool, const std::vector<TPlanNode>& tnodes,
                                    const DescriptorTbl& descs, ExecNode* parent, int* node_idx, ExecNode** root) {
    // propagate error case
    if (*node_idx >= tnodes.size()) {
        return Status::InternalError("Failed to reconstruct plan tree from thrift.");
    }
    const TPlanNode& tnode = tnodes[*node_idx];

    int num_children = tnodes[*node_idx].num_children;
    ExecNode* node = nullptr;
    RETURN_IF_ERROR(create_vectorized_node(state, pool, tnodes[*node_idx], descs, &node));

    DCHECK((parent != nullptr) || (root != nullptr));
    if (UNLIKELY(parent == nullptr && root == nullptr)) {
        return Status::InternalError("parent and root shouldn't both be null");
    }
    if (parent != nullptr) {
        parent->_children.push_back(node);
    } else {
        *root = node;
    }

    for (int i = 0; i < num_children; i++) {
        ++*node_idx;
        RETURN_IF_ERROR(create_tree_helper(state, pool, tnodes, descs, node, node_idx, nullptr));

        // we are expecting a child, but have used all nodes
        // this means we have been given a bad tree and must fail
        if (*node_idx >= tnodes.size()) {
            // TODO: print thrift msg
            return Status::InternalError("Failed to reconstruct plan tree from thrift.");
        }
    }

    RETURN_IF_ERROR(node->init(tnode, state));

    // build up tree of profiles; add children >0 first, so that when we print
    // the profile, child 0 is printed last (makes the output more readable)
    for (int i = 1; i < node->_children.size(); ++i) {
        node->runtime_profile()->add_child(node->_children[i]->runtime_profile(), true, nullptr);
    }

    if (!node->_children.empty()) {
        node->runtime_profile()->add_child(node->_children[0]->runtime_profile(), true, nullptr);
    }

    return Status::OK();
}

Status ExecNode::create_vectorized_node(starrocks::RuntimeState* state, starrocks::ObjectPool* pool,
                                        const starrocks::TPlanNode& tnode, const starrocks::DescriptorTbl& descs,
                                        starrocks::ExecNode** node) {
    switch (tnode.node_type) {
    case TPlanNodeType::OLAP_SCAN_NODE:
        *node = pool->add(new vectorized::OlapScanNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::META_SCAN_NODE:
        *node = pool->add(new vectorized::OlapMetaScanNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::AGGREGATION_NODE:
        if (tnode.agg_node.__isset.use_streaming_preaggregation && tnode.agg_node.use_streaming_preaggregation) {
            if (tnode.agg_node.aggregate_functions.size() == 0) {
                *node = pool->add(new vectorized::DistinctStreamingNode(pool, tnode, descs));
            } else {
                *node = pool->add(new vectorized::AggregateStreamingNode(pool, tnode, descs));
            }
        } else {
            if (tnode.agg_node.aggregate_functions.size() == 0) {
                *node = pool->add(new vectorized::DistinctBlockingNode(pool, tnode, descs));
            } else {
                *node = pool->add(new vectorized::AggregateBlockingNode(pool, tnode, descs));
            }
        }
        return Status::OK();
    case TPlanNodeType::EMPTY_SET_NODE:
        *node = pool->add(new EmptySetNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::EXCHANGE_NODE:
        *node = pool->add(new ExchangeNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::HASH_JOIN_NODE:
        *node = pool->add(new vectorized::HashJoinNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::ANALYTIC_EVAL_NODE:
        *node = pool->add(new vectorized::AnalyticNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::SORT_NODE:
        *node = pool->add(new vectorized::TopNNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::CROSS_JOIN_NODE:
    case TPlanNodeType::NESTLOOP_JOIN_NODE:
        *node = pool->add(new vectorized::CrossJoinNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::UNION_NODE:
        *node = pool->add(new vectorized::UnionNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::INTERSECT_NODE:
        *node = pool->add(new vectorized::IntersectNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::EXCEPT_NODE:
        *node = pool->add(new vectorized::ExceptNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::SELECT_NODE:
        *node = pool->add(new SelectNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::FILE_SCAN_NODE: {
        if (tnode.file_scan_node.__isset.enable_pipeline_load && tnode.file_scan_node.enable_pipeline_load) {
            TPlanNode new_node = tnode;
            TConnectorScanNode connector_scan_node;
            connector_scan_node.connector_name = connector::Connector::FILE;
            new_node.connector_scan_node = connector_scan_node;
            *node = pool->add(new vectorized::ConnectorScanNode(pool, new_node, descs));
        } else {
            *node = pool->add(new vectorized::FileScanNode(pool, tnode, descs));
        }
    }
        return Status::OK();
    case TPlanNodeType::REPEAT_NODE:
        *node = pool->add(new vectorized::RepeatNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::ASSERT_NUM_ROWS_NODE:
        *node = pool->add(new vectorized::AssertNumRowsNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::PROJECT_NODE:
        *node = pool->add(new vectorized::ProjectNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::TABLE_FUNCTION_NODE:
        *node = pool->add(new vectorized::TableFunctionNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::HDFS_SCAN_NODE: {
        TPlanNode new_node = tnode;
        TConnectorScanNode connector_scan_node;
        connector_scan_node.connector_name = connector::Connector::HIVE;
        new_node.connector_scan_node = connector_scan_node;
        *node = pool->add(new vectorized::ConnectorScanNode(pool, new_node, descs));
        return Status::OK();
    }
    case TPlanNodeType::MYSQL_SCAN_NODE: {
        TPlanNode new_node = tnode;
        TConnectorScanNode connector_scan_node;
        connector_scan_node.connector_name = connector::Connector::MYSQL;
        new_node.connector_scan_node = connector_scan_node;
        *node = pool->add(new vectorized::ConnectorScanNode(pool, new_node, descs));
        return Status::OK();
    }
    case TPlanNodeType::ES_HTTP_SCAN_NODE: {
        TPlanNode new_node = tnode;
        TConnectorScanNode connector_scan_node;
        connector_scan_node.connector_name = connector::Connector::ES;
        new_node.connector_scan_node = connector_scan_node;
        *node = pool->add(new vectorized::ConnectorScanNode(pool, new_node, descs));
        return Status::OK();
    }
    case TPlanNodeType::SCHEMA_SCAN_NODE:
        *node = pool->add(new vectorized::SchemaScanNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::DECODE_NODE:
        *node = pool->add(new vectorized::DictDecodeNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::JDBC_SCAN_NODE: {
        TPlanNode new_node = tnode;
        TConnectorScanNode connector_scan_node;
        connector_scan_node.connector_name = connector::Connector::JDBC;
        new_node.connector_scan_node = connector_scan_node;
        *node = pool->add(new vectorized::ConnectorScanNode(pool, new_node, descs));
        return Status::OK();
    }
    case TPlanNodeType::LAKE_SCAN_NODE: {
        TPlanNode new_node = tnode;
        TConnectorScanNode connector_scan_node;
        connector_scan_node.connector_name = connector::Connector::LAKE;
        new_node.connector_scan_node = connector_scan_node;
        *node = pool->add(new vectorized::ConnectorScanNode(pool, new_node, descs));
        return Status::OK();
    }
    default:
        return Status::InternalError(strings::Substitute("Vectorized engine not support node: $0", tnode.node_type));
    }
}

std::string ExecNode::debug_string() const {
    std::stringstream out;
    this->debug_string(0, &out);
    return out.str();
}

void ExecNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << " conjuncts=" << Expr::debug_string(_conjuncts);
    *out << " id=" << _id;
    *out << " type=" << print_plan_node_type(_type);
    *out << " tuple_ids=[";
    for (auto id : _tuple_ids) {
        *out << id << ", ";
    }
    *out << "]";

    for (auto i : _children) {
        *out << "\n";
        i->debug_string(indentation_level + 1, out);
    }
}

Status eager_prune_eval_conjuncts(const std::vector<ExprContext*>& ctxs, vectorized::Chunk* chunk) {
    vectorized::Column::Filter filter(chunk->num_rows(), 1);
    vectorized::Column::Filter* raw_filter = &filter;

    // prune chunk when pruned size is large enough
    // these constants are just came up without any specific reason.
    // and plus there is no strong evidence that this strategy has better performance.
    // pruned chunk can be saved from following conjuncts evaluation, that's benefit.
    // but calling `filter` there is memory copy, that's cost. So this is tradeoff.
    // so we don't expect pruned chunk size is too small. and if prune_ratio > 0.5
    // there is at most one prune process. according to my observation to running tpch 100g
    // in many cases, chunk is sparse enough.
    const float prune_ratio = 0.8;
    const int prune_min_size = 1024;

    int prune_threshold = std::max(int(chunk->num_rows() * prune_ratio), prune_min_size);
    int zero_count = 0;

    for (auto* ctx : ctxs) {
        ASSIGN_OR_RETURN(ColumnPtr column, ctx->evaluate(chunk));
        size_t true_count = vectorized::ColumnHelper::count_true_with_notnull(column);

        if (true_count == column->size()) {
            // all hit, skip
            continue;
        } else if (0 == true_count) {
            // all not hit, return
            chunk->set_num_rows(0);
            return Status::OK();
        } else {
            vectorized::ColumnHelper::merge_two_filters(column, raw_filter, nullptr);
            zero_count = SIMD::count_zero(*raw_filter);
            if (zero_count > prune_threshold) {
                int rows = chunk->filter(*raw_filter, true);
                if (rows == 0) {
                    // When all rows in chunk is filtered, direct return
                    // No need to execute the following predicate
                    return Status::OK();
                }
                filter.assign(rows, 1);
                zero_count = 0;
            }
        }
    }
    if (zero_count == 0) {
        return Status::OK();
    }
    chunk->filter(*raw_filter, true);
    return Status::OK();
}

Status ExecNode::eval_conjuncts(const std::vector<ExprContext*>& ctxs, vectorized::Chunk* chunk,
                                vectorized::FilterPtr* filter_ptr, bool apply_filter) {
    // No need to do expression if none rows
    DCHECK(chunk != nullptr);
    if (chunk->num_rows() == 0) {
        return Status::OK();
    }

    // if we don't need filter, then we can prune chunk during eval conjuncts.
    // when doing prune, we expect all columns are in conjuncts, otherwise
    // there will be extra memcpy of columns/slots which are not children of any conjunct.
    // ideally, we can collects slots in conjuncts, and check the overlap with chunk->columns
    // if overlap ratio is high enough, it's good to do prune.
    // but here for simplicity, we just check columns numbers absolute value.
    // TO BE NOTED, that there is no storng evidence that this has better performance.
    // It's just by intuition.
    TRY_CATCH_ALLOC_SCOPE_START()
    const int eager_prune_max_column_number = 5;
    if (filter_ptr == nullptr && chunk->num_columns() <= eager_prune_max_column_number) {
        return eager_prune_eval_conjuncts(ctxs, chunk);
    }

    if (!apply_filter) {
        DCHECK(filter_ptr) << "Must provide a filter if not apply it directly";
    }
    vectorized::FilterPtr filter(new vectorized::Column::Filter(chunk->num_rows(), 1));
    if (filter_ptr != nullptr) {
        *filter_ptr = filter;
    }
    vectorized::Column::Filter* raw_filter = filter.get();

    for (auto* ctx : ctxs) {
        ASSIGN_OR_RETURN(ColumnPtr column, ctx->evaluate(chunk));
        size_t true_count = vectorized::ColumnHelper::count_true_with_notnull(column);

        if (true_count == column->size()) {
            // all hit, skip
            continue;
        } else if (0 == true_count) {
            // all not hit, return
            if (apply_filter) {
                chunk->set_num_rows(0);
            } else {
                filter->assign(filter->size(), 0);
            }
            return Status::OK();
        } else {
            bool all_zero = false;
            vectorized::ColumnHelper::merge_two_filters(column, raw_filter, &all_zero);
            if (all_zero) {
                if (apply_filter) {
                    chunk->set_num_rows(0);
                } else {
                    filter->assign(filter->size(), 0);
                }
                return Status::OK();
            }
        }
    }

    if (apply_filter) {
        chunk->filter(*raw_filter);
    }
    TRY_CATCH_ALLOC_SCOPE_END()
    return Status::OK();
}

StatusOr<size_t> ExecNode::eval_conjuncts_into_filter(const std::vector<ExprContext*>& ctxs, vectorized::Chunk* chunk,
                                                      vectorized::Filter* filter) {
    // No need to do expression if none rows
    DCHECK(chunk != nullptr);
    if (chunk->num_rows() == 0) {
        return 0;
    }
    for (auto* ctx : ctxs) {
        ASSIGN_OR_RETURN(ColumnPtr column, ctx->evaluate(chunk, filter->data()));
        size_t true_count = vectorized::ColumnHelper::count_true_with_notnull(column);

        if (true_count == column->size()) {
            // all hit, skip
            continue;
        } else if (0 == true_count) {
            return 0;
        } else {
            bool all_zero = false;
            vectorized::ColumnHelper::merge_two_filters(column, filter, &all_zero);
            if (all_zero) {
                return 0;
            }
        }
    }

    size_t true_count = SIMD::count_nonzero(*filter);
    return true_count;
}

void ExecNode::eval_join_runtime_filters(vectorized::Chunk* chunk) {
    if (chunk == nullptr) return;
    _runtime_filter_collector.evaluate(chunk);
    eval_filter_null_values(chunk);
}

void ExecNode::eval_join_runtime_filters(vectorized::ChunkPtr* chunk) {
    if (chunk == nullptr) return;
    eval_join_runtime_filters(chunk->get());
}

void ExecNode::eval_filter_null_values(vectorized::Chunk* chunk, const std::vector<SlotId>& filter_null_value_columns) {
    if (filter_null_value_columns.size() == 0) return;
    size_t before_size = chunk->num_rows();
    if (before_size == 0) return;

    // lazy allocation.
    vectorized::Buffer<uint8_t> selection(0);

    for (SlotId slot_id : filter_null_value_columns) {
        const ColumnPtr& c = chunk->get_column_by_slot_id(slot_id);
        if (!c->is_nullable()) continue;
        if (c->only_null()) {
            chunk->reset();
            return;
        }
        const vectorized::NullableColumn* nullable_column =
                vectorized::ColumnHelper::as_raw_column<vectorized::NullableColumn>(c);
        if (!nullable_column->has_null()) continue;
        if (selection.size() == 0) {
            selection.assign(before_size, 1);
        }
        // how many data() should we call? I really don't know.
        // let compiler tells me.
        // let compiler does vectorization.
        // let compiler does everything.
        // let's pray for compiler,
        // till the end of the world.
        const uint8_t* nulls = nullable_column->null_column()->raw_data();
        uint8_t* sel = selection.data();
        for (size_t i = 0; i < before_size; i++) {
            sel[i] &= !nulls[i];
        }
    }
    if (selection.size() == 0) return;

    size_t after_size = SIMD::count_nonzero(selection);
    // Those rows will be filtered out anyway, better to be filtered out here.
    if (after_size != before_size) {
        VLOG_FILE << "filter null values. before_size = " << before_size << ", after_size = " << after_size;
        chunk->filter(selection, true);
    }
}

void ExecNode::eval_filter_null_values(vectorized::Chunk* chunk) {
    eval_filter_null_values(chunk, _filter_null_value_columns);
}

void ExecNode::collect_nodes(TPlanNodeType::type node_type, std::vector<ExecNode*>* nodes) {
    if (_type == node_type) {
        nodes->push_back(this);
    }
    for (auto& i : _children) {
        i->collect_nodes(node_type, nodes);
    }
}

void ExecNode::collect_scan_nodes(vector<ExecNode*>* nodes) {
    collect_nodes(TPlanNodeType::OLAP_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::FILE_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::ES_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::ES_HTTP_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::HDFS_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::META_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::JDBC_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::MYSQL_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::LAKE_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::SCHEMA_SCAN_NODE, nodes);
}

void ExecNode::init_runtime_profile(const std::string& name) {
    std::stringstream ss;
    ss << name << " (id=" << _id << ")";
    _runtime_profile.reset(new RuntimeProfile(ss.str()));
    _runtime_profile->set_metadata(_id);
}

Status ExecNode::exec_debug_action(TExecNodePhase::type phase) {
    DCHECK(phase != TExecNodePhase::INVALID);

    if (_debug_phase != phase) {
        return Status::OK();
    }

    if (_debug_action == TDebugAction::FAIL) {
        return Status::InternalError("Debug Action: FAIL");
    }

    if (_debug_action == TDebugAction::WAIT) {
        while (true) {
            sleep(1);
        }
    }

    return Status::OK();
}

} // namespace starrocks

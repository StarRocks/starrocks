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

#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "common/object_pool.h"
#include "common/runtime_profile.h"
#include "common/status.h"
#include "common/util/debug_util.h"
#include "exec/exec_factory.h"
#include "exec/pipeline/chunk_accumulate_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exprs/chunk_predicate_evaluator.h"
#include "exprs/dictionary_get_expr.h"
#include "exprs/expr_context.h"
#include "exprs/expr_executor.h"
#include "exprs/expr_factory.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_filter_cache.h"
#include "runtime/runtime_state.h"

namespace starrocks {

const std::string ExecNode::ROW_THROUGHPUT_COUNTER = "RowsReturnedRate";

ExecNode::ExecNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : _id(tnode.node_id),
          _type(tnode.node_type),
          _pool(pool),
          _tuple_ids(tnode.row_tuples),
          _row_descriptor(descs, tnode.row_tuples),
          _resource_profile(tnode.resource_profile),
          _debug_phase(TExecNodePhase::INVALID),
          _debug_action(TDebugAction::WAIT),
          _limit(tnode.limit),
          _num_rows_returned(0),
          _rows_returned_counter(nullptr),
          _rows_returned_rate(nullptr),
          _memory_used_counter(nullptr),
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
            WARN_IF_ERROR((*iter)->prepare(state), "prepare expression failed");
            WARN_IF_ERROR((*iter)->open(state), "open expression failed");
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

void ExecNode::push_down_join_runtime_filter(RuntimeState* state, RuntimeFilterProbeCollector* collector) {
    if (collector->empty()) return;
    if (_type != TPlanNodeType::AGGREGATION_NODE && _type != TPlanNodeType::ANALYTIC_EVAL_NODE) {
        push_down_join_runtime_filter_to_children(state, collector);
    }
    _runtime_filter_collector.push_down(state, id(), collector, _tuple_ids, _local_rf_waiting_set);
}

void ExecNode::push_down_join_runtime_filter_to_children(RuntimeState* state, RuntimeFilterProbeCollector* collector) {
    for (auto& i : _children) {
        i->push_down_join_runtime_filter(state, collector);
        if (collector->size() == 0) {
            return;
        }
    }
}

void ExecNode::register_runtime_filter_descriptor(RuntimeState* state, RuntimeFilterProbeDescriptor* rf_desc) {
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
            RuntimeFilterProbeDescriptor* rf_desc = _pool->add(new RuntimeFilterProbeDescriptor());
            RETURN_IF_ERROR(rf_desc->init(_pool, desc, _id, state));
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
    RETURN_IF_ERROR(ExprFactory::create_expr_trees(_pool, tnode.conjuncts, &_conjunct_ctxs, state));
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
    _mem_tracker.reset(new MemTracker(_runtime_profile.get(), std::make_tuple(true, false, false), "", -1,
                                      _runtime_profile->name(), nullptr));
    RETURN_IF_ERROR(ExprExecutor::prepare(_conjunct_ctxs, state));
    RETURN_IF_ERROR(_runtime_filter_collector.prepare(state, _runtime_profile.get()));

    // TODO(zc):
    // AddExprCtxsToFree(_conjunct_ctxs);

    for (auto& i : _children) {
        RETURN_IF_ERROR(i->prepare(state));
    }

    return Status::OK();
}

Status ExecNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    RETURN_IF_ERROR(ExprExecutor::open(_conjunct_ctxs, state));
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
                    auto& src_columns = cur_chunk->columns();
                    size_t num_rows = cur_size;
                    // copy the new read chunk to the reserved
                    auto& dest_columns = pre_output_chunk->columns();
                    for (size_t i = 0; i < dest_columns.size(); i++) {
                        dest_columns[i]->as_mutable_raw_ptr()->append(*src_columns[i], 0, num_rows);
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
        (void)child_node->collect_query_statistics(statistics);
    }
    return Status::OK();
}

void ExecNode::close(RuntimeState* state) {
    if (_is_closed) {
        return;
    }
    _is_closed = true;
    (void)exec_debug_action(TExecNodePhase::CLOSE);

    if (_rows_returned_counter != nullptr) {
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    }

    for (auto& i : _children) {
        i->close(state);
    }

    ExprExecutor::close(_conjunct_ctxs, state);
    _runtime_filter_collector.close(state);
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
    // check tuple ids is in descs before create node
    RETURN_IF_ERROR(checkTupleIdsInDescs(descs, tnodes[*node_idx]));
    RETURN_IF_ERROR(ExecFactory::create_vectorized_node(state, pool, tnodes[*node_idx], descs, &node));

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

std::string ExecNode::debug_string() const {
    std::stringstream out;
    this->debug_string(0, &out);
    return out.str();
}

Status ExecNode::checkTupleIdsInDescs(const DescriptorTbl& descs, const TPlanNode& planNode) {
    for (auto id : planNode.row_tuples) {
        if (descs.get_tuple_descriptor(id) == nullptr) {
            std::stringstream ss;
            ss << "Plan node id: " << planNode.node_id << ", Tuple ids: ";
            for (auto id : planNode.row_tuples) {
                ss << id << ", ";
            }
            LOG(ERROR) << ss.str();
            ss.str("");
            ss << "DescriptorTbl: " << descs.debug_string();
            LOG(ERROR) << ss.str();
            ss.str("");
            ss << "TPlanNode: " << apache::thrift::ThriftDebugString(planNode);
            LOG(ERROR) << ss.str();
            return Status::InternalError("Tuple ids are not in descs");
        }
    }

    return Status::OK();
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

void ExecNode::eval_join_runtime_filters(Chunk* chunk) {
    if (chunk == nullptr) return;
    _runtime_filter_collector.evaluate(chunk);
    eval_filter_null_values(chunk);
}

void ExecNode::eval_join_runtime_filters(ChunkPtr* chunk) {
    if (chunk == nullptr) return;
    eval_join_runtime_filters(chunk->get());
}

void ExecNode::eval_filter_null_values(Chunk* chunk) {
    ChunkPredicateEvaluator::eval_filter_null_values(chunk, _filter_null_value_columns);
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
    collect_nodes(TPlanNodeType::LAKE_META_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::JDBC_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::MYSQL_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::BENCHMARK_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::LAKE_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::SCHEMA_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::STREAM_SCAN_NODE, nodes);
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

void ExecNode::may_add_chunk_accumulate_operator(OpFactories& ops, pipeline::PipelineBuilderContext* context, int id) {
    // TODO(later): Need to rewrite ChunkAccumulateOperator to support StreamPipelines,
    // for now just disable it in stream pipelines:
    // - make sure UPDATE_BEFORE/UPDATE_AFTER are in the same chunk.
    if (!context->is_stream_pipeline()) {
        ops.emplace_back(std::make_shared<pipeline::ChunkAccumulateOperatorFactory>(context->next_operator_id(), id));
    }
}

} // namespace starrocks

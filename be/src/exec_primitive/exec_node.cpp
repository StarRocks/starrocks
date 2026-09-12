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

#include "exec_primitive/exec_node.h"

#include <thrift/protocol/TDebugProtocol.h>
#include <unistd.h>

#include <sstream>

#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "common/object_pool.h"
#include "common/runtime_profile.h"
#include "common/status.h"
#include "common/system/backend_options.h"
#include "common/util/debug_util.h"
#include "exec_primitive/runtime_filter/runtime_filter_registry.h"
#include "exprs/chunk_predicate_evaluator.h"
#include "exprs/expr_context.h"
#include "exprs/expr_executor.h"
#include "exprs/expr_factory.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_filter_cache.h"
#include "runtime/runtime_state.h"
#include "runtime/service_contexts.h"

namespace starrocks {

const std::string ExecNode::ROW_THROUGHPUT_COUNTER = "RowsReturnedRate";

ExecNode::ExecNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : _id(tnode.node_id),
          _type(tnode.node_type),
          _pool(pool),
          _tuple_ids(tnode.row_tuples),
          _record_descriptor(descs, tnode.row_tuples),
          _limit(tnode.limit) {
    init_runtime_profile(print_plan_node_type(tnode.node_type));
}

ExecNode::~ExecNode() {
    if (runtime_state() != nullptr) {
        ExecNode::close(_runtime_state);
    }
}

void ExecNode::push_down_tuple_slot_mappings(RuntimeState* state,
                                             const std::vector<TupleSlotMapping>& parent_mappings) {
    _tuple_slot_mappings = parent_mappings;
    for (auto& child : _children) {
        child->push_down_tuple_slot_mappings(state, _tuple_slot_mappings);
    }
}

void ExecNode::register_runtime_filter_descriptor(RuntimeState* state, RuntimeFilterProbeDescriptor* rf_desc) {
    rf_desc->set_probe_plan_node_id(_id);
    _runtime_filter_collector.add_descriptor(rf_desc);
    auto* query_execution_services = state->query_execution_services();
    query_execution_services->runtime->runtime_filter_cache->add_rf_event(
            {state->query_id(), rf_desc->filter_id(), BackendOptions::get_localhost(),
             strings::Substitute("REGISTER_GRF(probe_node_id=$0", _id)});
    state->runtime_filter_registry()->register_descriptor(rf_desc);
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
    if (state != nullptr && state->query_execution_services() != nullptr &&
        state->query_execution_services()->runtime != nullptr) {
        _runtime_filter_collector.set_runtime_filter_cache(
                state->query_execution_services()->runtime->runtime_filter_cache);
    }
    if (tnode.__isset.filter_null_value_columns) {
        _filter_null_value_columns = tnode.filter_null_value_columns;
    }
    return Status::OK();
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
    _mem_tracker = std::make_shared<MemTracker>(_runtime_profile.get(), std::make_tuple(true, false, false), "", -1,
                                                _runtime_profile->name(), nullptr);
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
    _runtime_filter_collector.wait(is_scan_node());
    return Status::OK();
}

Status ExecNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    return Status::NotSupported("Don't support vector query engine");
}

StatusOr<pipeline::OpFactories> ExecNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    pipeline::OpFactories operators;
    return operators;
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
    collect_nodes(TPlanNodeType::ADBC_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::MYSQL_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::BENCHMARK_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::LAKE_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::LAKE_CACHE_STATS_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::SCHEMA_SCAN_NODE, nodes);
}

void ExecNode::init_runtime_profile(const std::string& name) {
    std::stringstream ss;
    ss << name << " (id=" << _id << ")";
    _runtime_profile = std::make_shared<RuntimeProfile>(ss.str());
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

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
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/empty_set_node.h"
#include "exec/exchange_node.h"
#include "exec/select_node.h"
#include "exec/vectorized/adapter_node.h"
#include "exec/vectorized/aggregate/aggregate_blocking_node.h"
#include "exec/vectorized/aggregate/aggregate_streaming_node.h"
#include "exec/vectorized/aggregate/distinct_blocking_node.h"
#include "exec/vectorized/aggregate/distinct_streaming_node.h"
#include "exec/vectorized/analytic_node.h"
#include "exec/vectorized/assert_num_rows_node.h"
#include "exec/vectorized/cross_join_node.h"
#include "exec/vectorized/es_http_scan_node.h"
#include "exec/vectorized/except_node.h"
#include "exec/vectorized/file_scan_node.h"
#include "exec/vectorized/hash_join_node.h"
#include "exec/vectorized/hdfs_scan_node.h"
#include "exec/vectorized/intersect_node.h"
#include "exec/vectorized/mysql_scan_node.h"
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
#include "runtime/initial_reservations.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_filter_worker.h"
#include "runtime/runtime_state.h"
#include "simd/simd.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"

// Our new vectorized query executor is more powerful and stable than old query executor,
// The executor query executor related codes could be deleted safely.
// TODO: Remove old query executor related codes before 2021-09-30

namespace starrocks {

const std::string ExecNode::ROW_THROUGHPUT_COUNTER = "RowsReturnedRate";

int ExecNode::get_node_id_from_profile(RuntimeProfile* p) {
    return p->metadata();
}

ExecNode::RowBatchQueue::RowBatchQueue(int max_batches) : TimedBlockingQueue<RowBatch*>(max_batches) {}

ExecNode::RowBatchQueue::~RowBatchQueue() {
    DCHECK(cleanup_queue_.empty());
}

void ExecNode::RowBatchQueue::AddBatch(RowBatch* batch) {
    if (!blocking_put(batch)) {
        std::lock_guard<std::mutex> lock(lock_);
        cleanup_queue_.push_back(batch);
    }
}

bool ExecNode::RowBatchQueue::AddBatchWithTimeout(RowBatch* batch, int64_t timeout_micros) {
    // return blocking_put_with_timeout(batch, timeout_micros);
    return blocking_put(batch);
}

RowBatch* ExecNode::RowBatchQueue::GetBatch() {
    RowBatch* result = nullptr;
    if (blocking_get(&result)) return result;
    return nullptr;
}

int ExecNode::RowBatchQueue::Cleanup() {
    int num_io_buffers = 0;

    // RowBatch* batch = NULL;
    // while ((batch = GetBatch()) != NULL) {
    //   num_io_buffers += batch->num_io_buffers();
    //   delete batch;
    // }

    std::lock_guard<std::mutex> l(lock_);
    for (auto& it : cleanup_queue_) {
        // num_io_buffers += (*it)->num_io_buffers();
        delete it;
    }
    cleanup_queue_.clear();
    return num_io_buffers;
}

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
          _is_closed(false) {
    init_runtime_profile(print_plan_node_type(tnode.node_type));
}

ExecNode::~ExecNode() = default;

void ExecNode::push_down_predicate(RuntimeState* state, std::list<ExprContext*>* expr_ctxs, bool is_vectorized) {
    if (_type != TPlanNodeType::AGGREGATION_NODE) {
        for (auto& i : _children) {
            i->push_down_predicate(state, expr_ctxs, is_vectorized);
            if (expr_ctxs->size() == 0) {
                return;
            }
        }
    }

    std::list<ExprContext*>::iterator iter = expr_ctxs->begin();
    while (iter != expr_ctxs->end()) {
        if ((*iter)->root()->is_bound(_tuple_ids)) {
            // LOG(INFO) << "push down success expr is " << (*iter)->debug_string()
            //          << " and node is " << debug_string();
            (*iter)->prepare(state, row_desc(), _expr_mem_tracker.get());
            (*iter)->open(state);
            _conjunct_ctxs.push_back(*iter);
            iter = expr_ctxs->erase(iter);
        } else {
            ++iter;
        }
    }
}

void ExecNode::push_down_join_runtime_filter(RuntimeState* state, vectorized::RuntimeFilterProbeCollector* collector) {
    if (_type != TPlanNodeType::AGGREGATION_NODE) {
        push_down_join_runtime_filter_to_children(state, collector);
    }
    _runtime_filter_collector.push_down(collector, _tuple_ids);
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
    _runtime_filter_collector.add_descriptor(rf_desc);
    state->runtime_filter_port()->add_listener(rf_desc);
}

Status ExecNode::init_join_runtime_filters(const TPlanNode& tnode, RuntimeState* state) {
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
    return Status::OK();
}

Status ExecNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, tnode.conjuncts, &_conjunct_ctxs));
    RETURN_IF_ERROR(init_join_runtime_filters(tnode, state));
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
    _mem_tracker.reset(
            new MemTracker(_runtime_profile.get(), -1, _runtime_profile->name(), state->instance_mem_tracker()));
    _expr_mem_tracker.reset(new MemTracker(-1, "Exprs", _mem_tracker.get()));
    _expr_mem_pool.reset(new MemPool(_expr_mem_tracker.get()));
    RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, state, row_desc(), expr_mem_tracker()));
    RETURN_IF_ERROR(_runtime_filter_collector.prepare(state, row_desc(), expr_mem_tracker(), _runtime_profile.get()));

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
    _runtime_filter_collector.wait();
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
// if pre chunk is nullptr current chunk size >= batch_size / 2, direct return the current chunk
// if pre chunk is not nullptr and pre chunk size + cur chunk size <= 4096, merge the two chunk
// if pre chunk is not nullptr and pre chunk size + cur chunk size > 4096, return pre chunk
Status ExecNode::get_next_big_chunk(RuntimeState* state, ChunkPtr* chunk, bool* eos, ChunkPtr& pre_output_chunk,
                                    const std::function<Status(RuntimeState*, ChunkPtr*, bool*)>& specific_get_next) {
    size_t batch_size = state->batch_size();

    while (true) {
        bool cur_eos = false;
        ChunkPtr cur_chunk = nullptr;

        RETURN_IF_ERROR(specific_get_next(state, &cur_chunk, &cur_eos));
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
                if (cur_size >= batch_size / 2) {
                    // the probe chunk size of read from right child >= batch_size, direct return
                    *eos = false;
                    *chunk = std::move(cur_chunk);
                    return Status::OK();
                } else {
                    pre_output_chunk = std::move(cur_chunk);
                    continue;
                }
            } else {
                if (cur_size + pre_output_chunk->num_rows() > batch_size) {
                    // the two chunk size > batch_size, return the first reserved chunk
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
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::CLOSE));

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

    if (expr_mem_pool() != nullptr) {
        _expr_mem_pool->free_all();
    }

    if (_buffer_pool_client.is_registered()) {
        VLOG_FILE << _id << " returning reservation " << _resource_profile.min_reservation;
        state->initial_reservations()->Return(&_buffer_pool_client, _resource_profile.min_reservation);
        state->exec_env()->buffer_pool()->DeregisterClient(&_buffer_pool_client);
    }

    if (_expr_mem_tracker != nullptr) {
        _expr_mem_tracker->close();
    }

    if (_mem_tracker != nullptr) {
        _mem_tracker->close();
    }

    return result;
}

void ExecNode::add_runtime_exec_option(const std::string& str) {
    std::lock_guard<std::mutex> l(_exec_options_lock);

    if (_runtime_exec_options.empty()) {
        _runtime_exec_options = str;
    } else {
        _runtime_exec_options.append(", ");
        _runtime_exec_options.append(str);
    }

    runtime_profile()->add_info_string("ExecOption", _runtime_exec_options);
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
        // TODO: print thrift msg for diagnostic purposes.
        return Status::InternalError("Plan tree only partially reconstructed. Not all thrift nodes were used.");
    }

    return Status::OK();
}

Status ExecNode::create_tree_helper(RuntimeState* state, ObjectPool* pool, const std::vector<TPlanNode>& tnodes,
                                    const DescriptorTbl& descs, ExecNode* parent, int* node_idx, ExecNode** root) {
    // propagate error case
    if (*node_idx >= tnodes.size()) {
        // TODO: print thrift msg
        return Status::InternalError("Failed to reconstruct plan tree from thrift.");
    }
    const TPlanNode& tnode = tnodes[*node_idx];

    int num_children = tnodes[*node_idx].num_children;
    ExecNode* node = nullptr;
    RETURN_IF_ERROR(create_vectorized_node(state, pool, tnodes[*node_idx], descs, &node));

    // assert(parent != NULL || (node_idx == 0 && root_expr != NULL));
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
    case TPlanNodeType::FILE_SCAN_NODE:
        *node = pool->add(new vectorized::FileScanNode(pool, tnode, descs));
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
    case TPlanNodeType::HDFS_SCAN_NODE:
#ifdef STARROCKS_WITH_HDFS
        *node = pool->add(new vectorized::HdfsScanNode(pool, tnode, descs));
        return Status::OK();
#else
        return Status::InternalError("Don't support HDFS table, you should rebuild StarRocks with WITH_HDFS option ON");
#endif
    case TPlanNodeType::MYSQL_SCAN_NODE:
        *node = pool->add(new vectorized::MysqlScanNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::ES_HTTP_SCAN_NODE:
        *node = pool->add(new vectorized::EsHttpScanNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::SCHEMA_SCAN_NODE:
        *node = pool->add(new vectorized::SchemaScanNode(pool, tnode, descs));
        return Status::OK();
    default:
        return Status::InternalError(strings::Substitute("Vectorized engine not support node: $0", tnode.node_type));
    }
}

void ExecNode::set_debug_options(int node_id, TExecNodePhase::type phase, TDebugAction::type action, ExecNode* root) {
    if (root->_id == node_id) {
        root->_debug_phase = phase;
        root->_debug_action = action;
        return;
    }

    for (auto& i : root->_children) {
        set_debug_options(node_id, phase, action, i);
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

bool ExecNode::eval_conjuncts(ExprContext* const* ctxs, int num_ctxs, TupleRow* row) {
    for (int i = 0; i < num_ctxs; ++i) {
        BooleanVal v = ctxs[i]->get_boolean_val(row);
        if (v.is_null || !v.val) {
            return false;
        }
    }
    return true;
}

static void eager_prune_eval_conjuncts(const std::vector<ExprContext*>& ctxs, vectorized::Chunk* chunk) {
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
        ColumnPtr column = ctx->evaluate(chunk);
        size_t true_count = vectorized::ColumnHelper::count_true_with_notnull(column);

        if (true_count == column->size()) {
            // all hit, skip
            continue;
        } else if (0 == true_count) {
            // all not hit, return
            chunk->set_num_rows(0);
            return;
        } else {
            vectorized::ColumnHelper::merge_two_filters(column, raw_filter, nullptr);
            zero_count = SIMD::count_zero(*raw_filter);
            if (zero_count > prune_threshold) {
                int rows = chunk->filter(*raw_filter);
                if (rows == 0) {
                    // When all rows in chunk is filtered, direct return
                    // No need to execute the following predicate
                    return;
                }
                filter.assign(rows, 1);
                zero_count = 0;
            }
        }
    }
    if (zero_count == 0) {
        return;
    }
    chunk->filter(*raw_filter);
}

void ExecNode::eval_conjuncts(const std::vector<ExprContext*>& ctxs, vectorized::Chunk* chunk,
                              vectorized::FilterPtr* filter_ptr) {
    // No need to do expression if none rows
    if (chunk->num_rows() == 0) {
        return;
    }

    // if we don't need filter, then we can prune chunk during eval conjuncts.
    // when doing prune, we expect all columns are in conjuncts, otherwise
    // there will be extra memcpy of columns/slots which are not children of any conjunct.
    // ideally, we can collects slots in conjuncts, and check the overlap with chunk->columns
    // if overlap ratio is high enough, it's good to do prune.
    // but here for simplicity, we just check columns numbers absolute value.
    // TO BE NOTED, that there is no storng evidence that this has better performance.
    // It's just by intuition.
    const int eager_prune_max_column_number = 5;
    if (filter_ptr == nullptr && chunk->num_columns() <= eager_prune_max_column_number) {
        return eager_prune_eval_conjuncts(ctxs, chunk);
    }

    vectorized::FilterPtr filter(new vectorized::Column::Filter(chunk->num_rows(), 1));
    if (filter_ptr != nullptr) {
        *filter_ptr = filter;
    }
    vectorized::Column::Filter* raw_filter = filter.get();

    for (auto* ctx : ctxs) {
        ColumnPtr column = ctx->evaluate(chunk);
        size_t true_count = vectorized::ColumnHelper::count_true_with_notnull(column);

        if (true_count == column->size()) {
            // all hit, skip
            continue;
        } else if (0 == true_count) {
            // all not hit, return
            chunk->set_num_rows(0);
            return;
        } else {
            bool all_zero = false;
            vectorized::ColumnHelper::merge_two_filters(column, raw_filter, &all_zero);
            if (all_zero) {
                chunk->set_num_rows(0);
                return;
            }
        }
    }

    int zero_count = SIMD::count_zero(*raw_filter);
    if (zero_count == 0) {
        return;
    }
    chunk->filter(*raw_filter);
}

void ExecNode::eval_join_runtime_filters(vectorized::Chunk* chunk) {
    if (chunk == nullptr) return;
    _runtime_filter_collector.evaluate(chunk);
}

void ExecNode::eval_join_runtime_filters(vectorized::ChunkPtr* chunk) {
    if (chunk == nullptr) return;
    eval_join_runtime_filters(chunk->get());
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
}

bool ExecNode::_check_has_vectorized_scan_child() {
    if (_use_vectorized) {
        return true;
    }

    for (auto& i : _children) {
        if (i->_check_has_vectorized_scan_child()) {
            return true;
        }
    }

    return false;
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

Status ExecNode::claim_buffer_reservation(RuntimeState* state) {
    DCHECK(!_buffer_pool_client.is_registered());
    BufferPool* buffer_pool = ExecEnv::GetInstance()->buffer_pool();
    // Check the minimum buffer size in case the minimum buffer size used by the planner
    // doesn't match this backend's.
    std::stringstream ss;
    if (_resource_profile.__isset.spillable_buffer_size &&
        _resource_profile.spillable_buffer_size < buffer_pool->min_buffer_len()) {
        ss << "Spillable buffer size for node " << _id << " of " << _resource_profile.spillable_buffer_size
           << "bytes is less than the minimum buffer pool buffer size of " << buffer_pool->min_buffer_len() << "bytes";
        return Status::InternalError(ss.str());
    }

    ss << print_plan_node_type(_type) << " id=" << _id << " ptr=" << this;
    RETURN_IF_ERROR(buffer_pool->RegisterClient(ss.str(), state->instance_buffer_reservation(), mem_tracker(),
                                                buffer_pool->GetSystemBytesLimit(), runtime_profile(),
                                                &_buffer_pool_client));

    state->initial_reservations()->Claim(&_buffer_pool_client, _resource_profile.min_reservation);
    /*
    if (debug_action_ == TDebugAction::SET_DENY_RESERVATION_PROBABILITY &&
        (debug_phase_ == TExecNodePhase::PREPARE || debug_phase_ == TExecNodePhase::OPEN)) {
       // We may not have been able to enable the debug action at the start of Prepare() or
       // Open() because the client is not registered then. Do it now to be sure that it is
       // effective.
               RETURN_IF_ERROR(EnableDenyReservationDebugAction());
    }
*/
    return Status::OK();
}

Status ExecNode::release_unused_reservation() {
    return _buffer_pool_client.DecreaseReservationTo(_resource_profile.min_reservation);
}
/*
Status ExecNode::enable_deny_reservation_debug_action() {
  DCHECK_EQ(debug_action_, TDebugAction::SET_DENY_RESERVATION_PROBABILITY);
  DCHECK(_buffer_pool_client.is_registered());
  // Parse [0.0, 1.0] probability.
  StringParser::ParseResult parse_result;
  double probability = StringParser::StringToFloat<double>(
      debug_action_param_.c_str(), debug_action_param_.size(), &parse_result);
  if (parse_result != StringParser::PARSE_SUCCESS || probability < 0.0
      || probability > 1.0) {
    return Status::InternalError(strings::Substitute(
        "Invalid SET_DENY_RESERVATION_PROBABILITY param: '$0'", debug_action_param_));
  }
  _buffer_pool_client.SetDebugDenyIncreaseReservation(probability);
  return Status::OK()();
}
*/

Status ExecNode::QueryMaintenance(RuntimeState* state, const std::string& msg) {
    // TODO chenhao , when introduce latest AnalyticEvalNode open it
    // ScalarExprEvaluator::FreeLocalAllocations(evals_to_free_);
    return state->check_query_state(msg);
}

} // namespace starrocks

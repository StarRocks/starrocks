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

#include "exec/short_circuit.h"

#include "column/column_helper.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "connector/connector.h"
#include "exec/scan_node.h"
#include "exec/short_circuit_hybrid.h"
#include "runtime/exec_env.h"
#include "runtime/memory_scratch_sink.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/result_sink.h"
#include "service/brpc.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "util/thrift_util.h"

namespace starrocks {
class MysqlResultMemorySink : public DataSink {
public:
    MysqlResultMemorySink(const vector<TExpr>& t_exprs, bool is_binary_format,
                          vector<std::unique_ptr<TFetchDataResult>>& results)
            : _t_exprs(t_exprs), _is_binary_format(is_binary_format), _results(results) {}

    ~MysqlResultMemorySink() override { delete _row_buffer; };

    Status prepare(RuntimeState* state) override {
        _row_buffer = new (std::nothrow) MysqlRowBuffer(_is_binary_format);

        RETURN_IF_ERROR(Expr::create_expr_trees(state->obj_pool(), _t_exprs, &_output_expr_ctxs, state));
        RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
        return DataSink::prepare(state);
    }

    Status open(RuntimeState* state) override { return Expr::open(_output_expr_ctxs, state); }

    RuntimeProfile* profile() override { return nullptr; }

    Status send_chunk(RuntimeState* state, Chunk* chunk) override {
        // maybe refactor to reuse MysqlResultWriter
        int num_rows = chunk->num_rows();
        if (num_rows == 0) {
            return Status::OK();
        }
        auto result = std::make_unique<TFetchDataResult>();
        auto& result_rows = result->result_batch.rows;
        result_rows.resize(num_rows);

        Columns result_columns;
        // Step 1: compute expr
        int num_columns = _output_expr_ctxs.size();
        result_columns.reserve(num_columns);

        for (int i = 0; i < num_columns; ++i) {
            ASSIGN_OR_RETURN(ColumnPtr column, _output_expr_ctxs[i]->evaluate(chunk))
            column = _output_expr_ctxs[i]->root()->type().type == TYPE_TIME
                             ? ColumnHelper::convert_time_column_from_double_to_str(column)
                             : column;
            result_columns.emplace_back(std::move(column));
        }

        // Step 2: convert chunk to mysql row format row by row
        {
            _row_buffer->reserve(128);
            for (int i = 0; i < num_rows; ++i) {
                DCHECK_EQ(0, _row_buffer->length());
                if (_is_binary_format) {
                    _row_buffer->start_binary_row(num_columns);
                }
                for (auto& result_column : result_columns) {
                    result_column->put_mysql_row_buffer(_row_buffer, i);
                }
                size_t len = _row_buffer->length();
                _row_buffer->move_content(&result_rows[i]);
                _row_buffer->reserve(len * 1.1);
            }
        }
        _results.emplace_back(std::move(result));
        return Status::OK();
    }

private:
    const std::vector<TExpr>& _t_exprs;
    const bool _is_binary_format;
    MysqlRowBuffer* _row_buffer;
    std::vector<std::unique_ptr<TFetchDataResult>>& _results;
    std::vector<ExprContext*> _output_expr_ctxs;
};

ShortCircuitExecutor::~ShortCircuitExecutor() {
    close();
}

void ShortCircuitExecutor::close() {
    if (_closed) {
        return;
    }
    _closed = true;

    if (_runtime_state != nullptr) {
        if (_source != nullptr) {
            (void)_source->close(_runtime_state.get());
        }
        if (_sink != nullptr) {
            (void)_sink->close(_runtime_state.get(), Status::OK());
        }
    }
    return;
}

ShortCircuitExecutor::ShortCircuitExecutor(ExecEnv* exec_env)
        : _query_id(generate_uuid()), _fragment_instance_id(generate_uuid()), _exec_env(exec_env) {
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    _runtime_state =
            std::make_shared<RuntimeState>(_query_id, _fragment_instance_id, query_options, query_globals, _exec_env);
    _runtime_state->init_instance_mem_tracker();
    _runtime_profile = _runtime_state->runtime_profile();
}

Status ShortCircuitExecutor::prepare(TExecShortCircuitParams& common_request) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    auto* timer = ADD_TIMER(_runtime_profile, "PrepareTime");
    SCOPED_TIMER(timer);

    _common_request = &common_request;
    const TDescriptorTable& t_desc_tbl = _common_request->desc_tbl;
    const std::vector<TExpr>& output_exprs = _common_request->output_exprs;
    const TScanRange& scan_range = _common_request->scan_range;
    _enable_profile = _common_request->enable_profile;

    // build descs
    DescriptorTbl* desc_tbl = nullptr;
    RETURN_IF_ERROR(DescriptorTbl::create(runtime_state(), runtime_state()->obj_pool(), t_desc_tbl, &desc_tbl, 1024));
    runtime_state()->set_desc_tbl(desc_tbl);

    // build source
    int node_index = 0;
    RETURN_IF_ERROR(build_source_exec_helper(runtime_state()->obj_pool(), common_request.plan.nodes, &node_index,
                                             *desc_tbl, scan_range, &_source));

    // build sink
    if (_common_request->__isset.data_sink &&
        !(_common_request->data_sink.type == TDataSinkType::RESULT_SINK &&
          _common_request->data_sink.result_sink.type == TResultSinkType::type::MYSQL_PROTOCAL)) {
        // sink is not mysql result sink
        TPlanFragmentExecParams t_params;
        RETURN_IF_ERROR(DataSink::create_data_sink(runtime_state(), _common_request->data_sink, output_exprs, t_params,
                                                   0, _source->row_desc(), &_sink));
    } else {
        bool is_binary_format = _common_request->is_binary_row;
        _sink = std::make_unique<MysqlResultMemorySink>(output_exprs, is_binary_format, _results);
    }

    // prepare
    RETURN_IF_ERROR(_source->prepare(runtime_state()));
    RETURN_IF_ERROR(_sink->prepare(runtime_state()));

    return Status::OK();
}

Status ShortCircuitExecutor::execute() {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    auto* timer = ADD_TIMER(_runtime_profile, "ExecuteTime");
    SCOPED_TIMER(timer);

    RETURN_IF_ERROR(_source->open(runtime_state()));
    RETURN_IF_ERROR(_sink->open(runtime_state()));

    ChunkPtr chunk;
    bool eos = false;
    while (true) {
        if (eos) {
            break;
        }
        RETURN_IF_ERROR(_source->get_next(runtime_state(), &chunk, &eos));
        eos = true;
        if (!_results.empty()) {
            return Status::NotSupported("Not support multi result set yet");
        }
        RETURN_IF_ERROR(_sink->send_chunk(runtime_state(), chunk.get()));
    }
    _source->close(runtime_state());
    _finish = true;
    close();

    return Status::OK();
}

Status ShortCircuitExecutor::build_source_exec_helper(starrocks::ObjectPool* pool, std::vector<TPlanNode>& tnodes,
                                                      int* node_index, DescriptorTbl& descs,
                                                      const TScanRange& scan_range, starrocks::ExecNode** node) {
    TPlanNode& t_node = tnodes.at(*node_index);
    RETURN_IF_ERROR(build_source_exec_node(pool, t_node, descs, scan_range, node));

    _runtime_profile->add_child((*node)->runtime_profile(), true, nullptr);

    std::vector<ExecNode*> children;
    for (int i = 0; i < t_node.num_children; ++i) {
        ++(*node_index);
        starrocks::ExecNode* child;
        RETURN_IF_ERROR(build_source_exec_helper(pool, tnodes, node_index, descs, scan_range, &child));
        children.emplace_back(child);
    }
    (*node)->set_children(std::move(children));
    return Status::OK();
}

Status ShortCircuitExecutor::build_source_exec_node(starrocks::ObjectPool* pool, TPlanNode& t_node,
                                                    DescriptorTbl& descs, const TScanRange& scan_range,
                                                    starrocks::ExecNode** node) {
    switch (t_node.node_type) {
    case TPlanNodeType::OLAP_SCAN_NODE: {
        *node = pool->add(
                new ShortCircuitHybridScanNode(pool, t_node, descs, scan_range, _runtime_profile, *_common_request));
        break;
    }
    case TPlanNodeType::PROJECT_NODE:
    case TPlanNodeType::UNION_NODE: // values
        RETURN_IF_ERROR(ExecNode::create_vectorized_node(runtime_state(), pool, t_node, descs, node));
        break;
    default:
        return Status::InternalError(strings::Substitute("Short circuit not support node: $0", t_node.node_type));
    }

    RETURN_IF_ERROR((*node)->init(t_node, runtime_state()));
    return Status::OK();
}

RuntimeState* ShortCircuitExecutor::runtime_state() {
    return _runtime_state.get();
}

Status ShortCircuitExecutor::fetch_data(brpc::Controller* cntl, PExecShortCircuitResult& response) {
    {
        SCOPED_TIMER(_runtime_profile->total_time_counter());
        auto* timer = ADD_TIMER(_runtime_profile, "CloseTime");
        SCOPED_TIMER(timer);

        //TODO need row count
        // response.set_affected_rows(runtime_state()->num_rows_load_sink_success());

        if (!_results.empty()) {
            DCHECK(_results.size() <= 1);
            uint8_t* buf = nullptr;
            uint32_t len = 0;
            ThriftSerializer ser(false, 4096);
            RETURN_IF_ERROR(ser.serialize(&_results[0]->result_batch, &len, &buf));
            cntl->response_attachment().append(buf, len);
        }
    }

    if (_enable_profile) {
        if (_sink != nullptr && _sink->profile() != nullptr) {
            _runtime_profile->add_child(_sink->profile(), true, nullptr);
        }
        TRuntimeProfileTree profileTree;
        _runtime_profile->to_thrift(&profileTree);
        uint8_t* buf = nullptr;
        uint32_t len = 0;
        ThriftSerializer ser(false, 4096);
        RETURN_IF_ERROR(ser.serialize(&profileTree, &len, &buf));
        response.set_profile((char*)buf, len);
    }
    return Status::OK();
}
} // namespace starrocks

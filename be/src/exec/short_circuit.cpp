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
#include "runtime/exec_env.h"
#include "runtime/memory_scratch_sink.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/result_sink.h"
#include "service/brpc.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "util/thrift_util.h"

namespace starrocks {
// scan use current thread instead of io thread pool asynchronously
class ShortCircuitScanNode : public starrocks::ScanNode {
public:
    ShortCircuitScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs,
                         const TScanRange& scan_range, RuntimeProfile* runtime_profile,
                         const std::vector<TKeyLiteralExpr>& key_literal_exprs, 
                         const std::vector<TabletSharedPtr>& tablets,
                         const std::vector<std::string> versions)
            : ScanNode(pool, tnode, descs),
              _scan_range(scan_range),
              _runtime_profile(runtime_profile),
              _key_literal_exprs(key_literal_exprs),
              _tablets(tablets),
              _tuple_id(tnode.olap_scan_node.tuple_id),
              _versions(versions) {}

    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override { return Status::OK(); }

    Status open(RuntimeState* state) {
        SCOPED_TIMER(_runtime_profile->total_time_counter());

        _num_rows = _key_literal_exprs.size();
        //init tuple
        _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
        DCHECK(_tuple_desc != nullptr);

        // skips runtime filters in ScanNode::open
        RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, state));
        return Status::OK();
    }

    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
        SCOPED_TIMER(_runtime_profile->total_time_counter());
        std::vector<bool> found(_num_rows, false);
        Buffer<uint8_t> selections;

        RETURN_IF_ERROR(_process_key_chunk());
        RETURN_IF_ERROR(_process_value_chunk(found));
        size_t result_size = 0;
        for (int i = 0; i < found.size(); ++i) {
            if (found[i]) {
                result_size ++ ;
                selections.emplace_back(1);
            } else {
                selections.emplace_back(0);
            }
        }

        auto tablet_schema = _tablets[0]->tablet_schema()->schema();
        auto column_ids = tablet_schema->field_column_ids();
        auto tablet_schema_without_rowstore = std::make_unique<Schema>(tablet_schema, column_ids);
        auto result_chunk = ChunkHelper::new_chunk(*_tuple_desc, result_size);

        //idx is column id, value is slot id
        if (result_size > 0) {
            std::map<std::string, SlotId> column_name_to_slot_id;

            _key_chunk->filter(selections);
            for (auto slot_desc : _tuple_desc->slots()) {
                auto field = tablet_schema_without_rowstore->get_field_by_name(slot_desc->col_name());
                if (field->is_key()) {
                    result_chunk->get_column_by_slot_id(slot_desc->id())->append(*(_key_chunk->get_column_by_name(field->name().data()).get()));
                }
            }

            for (auto slot_desc : _tuple_desc->slots()) {
                auto field = tablet_schema_without_rowstore->get_field_by_name(slot_desc->col_name());
                if (!field->is_key()) {
                    result_chunk->get_column_by_slot_id(slot_desc->id())->append(*(_value_chunk->get_column_by_name(field->name().data()).get()));
                }
            }
            RETURN_IF_ERROR(ExecNode::eval_conjuncts(_conjunct_ctxs, result_chunk.get()));
        }
        *eos = true;
        *chunk = std::move(result_chunk);
        return Status::OK();
    }

    Status _process_key_chunk() {
        DCHECK(_tablets.size() > 0);
        _tablet_schema = _tablets[0]->tablet_schema();
        auto& key_column_cids = _tablet_schema->sort_key_idxes();
        auto key_schema = ChunkHelper::convert_schema(_tablet_schema, key_column_cids);

        _key_chunk = ChunkHelper::new_chunk(key_schema, _num_rows);
        _key_chunk->reset();

        for (int i = 0; i < _num_rows; ++i) {
            // TODO (jkj) if expr is k1=1 and k2 in (3, 4), we need bind tablet with expr, 
            // tablet 1  <---> k1 =1, k2 =3
            // tablet 2  <---> k1 =1, k2 =4
            // this prune need happen in fe
            auto keys_literal_expr = _key_literal_exprs[i].literal_exprs;
            size_t num_pk_filters = keys_literal_expr.size();
            // must all columns
            if (UNLIKELY(num_pk_filters != _tablet_schema->num_key_columns())) {
                return Status::Corruption("short circuit only support all key predicate");
            }
            for (int j = 0; j < num_pk_filters; ++j) {
                // init expr context
                std::vector<ExprContext*> expr_ctxs;
                std::vector<TExpr> key_literal_expr{keys_literal_expr[j]};
                // prepare
                Expr::create_expr_trees(runtime_state()->obj_pool(), key_literal_expr, &expr_ctxs, runtime_state());
                Expr::prepare(expr_ctxs, runtime_state());
                Expr::open(expr_ctxs, runtime_state());
                auto& iteral_expr_ctx = expr_ctxs[0];
                ASSIGN_OR_RETURN(ColumnPtr value, iteral_expr_ctx->root()->evaluate_const(iteral_expr_ctx));
                // add const column to chunk
                auto const_column = ColumnHelper::get_data_column(value.get());
                _key_chunk->get_column_by_index(j)->append(*const_column);
            }
        }

        return Status::OK();
    }

    Status _process_value_chunk(std::vector<bool>& found) {
        std::vector<string> value_field_names;
        auto value_schema = std::make_unique<Schema>(_tablet_schema->schema(), _tablet_schema->schema()->value_field_column_ids());

        for (auto slot_desc : _tuple_desc->slots()) {
            auto field = value_schema->get_field_by_name(slot_desc->col_name());
            if (field != nullptr && !field->is_key()) {
                value_field_names.emplace_back(std::move(field->name()));
            }
        }

        _value_chunk = ChunkHelper::new_chunk(*(value_schema), _num_rows);

        for (int i = 0; i < _tablets.size(); ++i) {
            LocalTableReaderParams params;
            params.version = std::stoi(_versions[i]);
            params.tablet_id = _tablets[i]->get_tablet_info().tablet_id;
            _table_reader = std::make_shared<TableReader>();
            RETURN_IF_ERROR(_table_reader->init(params));

            auto current_chunk = ChunkHelper::new_chunk(*(value_schema), _num_rows);
            std::vector<bool> curent_found;
            Status status = _table_reader->multi_get(*(_key_chunk.get()), value_field_names, curent_found, *(current_chunk.get()));
            if (!status.ok()) {
                // todo retry
                LOG(WARNING) << "fail to execute multi get: " << status.detailed_message();
            }

            // merge all tablet result
            for (int i = 0; i < curent_found.size(); ++i) {
                if (curent_found[i]) {
                    found[i] = true;
                }
            }
            _value_chunk->append(*(current_chunk.get()));
        }

        return Status::OK();
    }

private:
    int64_t _read_limit = -1; // no limit
    std::vector<ExprContext*> _conjunct_ctxs;
    const TScanRange& _scan_range;
    TableReaderPtr _table_reader;
    RuntimeProfile* _runtime_profile;
    ChunkUniquePtr _key_chunk;
    ChunkUniquePtr _value_chunk;
    const std::vector<TKeyLiteralExpr> _key_literal_exprs;
    TupleDescriptor* _tuple_desc;
    std::vector<TabletSharedPtr> _tablets;
    TabletSchemaCSPtr _tablet_schema;
    TupleId _tuple_id;
    std::vector<string> _versions;
    int64_t _num_rows;
};

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

    _t_desc_tbl = &common_request.desc_tbl;
    const std::vector<TExpr>& output_exprs = common_request.output_exprs;
    const TScanRange& scan_range = common_request.scan_range;
    _enable_profile = common_request.enable_profile;
    _key_literal_exprs = &common_request.key_literal_exprs;
    _versions.swap(common_request.versions);

    // get tablet
    for (auto tablet_id : common_request.tablet_ids) {
        auto tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
        if (tablet == nullptr) {
            return Status::NotFound(fmt::format("tablet {} not exist", tablet_id));
        }
        _tablets.emplace_back(std::move(tablet));
    }

    // build descs
    DescriptorTbl* desc_tbl = nullptr;
    RETURN_IF_ERROR(DescriptorTbl::create(runtime_state(), runtime_state()->obj_pool(), *_t_desc_tbl, &desc_tbl, 1024));
    runtime_state()->set_desc_tbl(desc_tbl);

    // build source
    int node_index = 0;
    RETURN_IF_ERROR(build_source_exec_helper(runtime_state()->obj_pool(), common_request.plan.nodes, &node_index,
                                             *desc_tbl, scan_range, &_source));

    // build sink
    bool is_binary_format = common_request.is_binary_row;
    _sink = std::make_unique<MysqlResultMemorySink>(output_exprs, is_binary_format, _results);

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
    bool eos;
    while (true) {
        RETURN_IF_ERROR(_source->get_next(runtime_state(), &chunk, &eos));
        RETURN_IF_ERROR(_sink->send_chunk(runtime_state(), chunk.get()));
        // TODO(many records iterator)
        if (eos) {
            break;
        }
    }
    _source->close(runtime_state());
    RETURN_IF_ERROR(_sink->close(runtime_state(), Status::OK()));
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
        *node = pool->add(new ShortCircuitScanNode(pool, t_node, descs, scan_range, _runtime_profile, *_key_literal_exprs, _tablets, _versions));
        break;
    }
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

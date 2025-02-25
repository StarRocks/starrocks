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

#include "paimon_table_sink_operator.h"

#include "exec/jni_writer.h"
#include "exec/paimon_native_writer.h"
#include "paimon/commit_message.h"

namespace starrocks::pipeline {

Status PaimonTableSinkOperator::prepare(RuntimeState* state) {
    RETURN_AND_SET_IF_ERROR(Operator::prepare(state));
    return init_profile();
}

void PaimonTableSinkOperator::close(RuntimeState* state) {
    Operator::close(state);
}

Status PaimonTableSinkOperator::init_profile() {
    if (_use_native_writer) {
        _unique_metrics->add_info_string("PaimonWriterType", "NATIVE");
    } else {
        _unique_metrics->add_info_string("PaimonWriterType", "JNI");
    }
    _init_timer = ADD_TIMER(_unique_metrics, "initTime");
    _write_timer = ADD_TIMER(_unique_metrics, "writeTime");
    _convert_timer = ADD_TIMER(_unique_metrics, "convertTime");
    _commit_timer = ADD_TIMER(_unique_metrics, "commitTime");
    return Status::OK();
}

bool PaimonTableSinkOperator::need_input() const {
    return true;
}

bool PaimonTableSinkOperator::is_finished() const {
    return _closed;
}

Status PaimonTableSinkOperator::set_finishing(RuntimeState* state) {
    LOG(INFO) << "set finishing PaimonTableSinkOperator...";
    if (!_closed) {
        if (!_error) {
            // It is not certainly sure we should return error here.
            auto st = do_commit(state);
            if (!st.ok()) {
                LOG(ERROR) << "Failed to do commit, reason: " << st.detailed_message();
            }
        }
        _closed = true;
    }
    return Status::OK();
}

bool PaimonTableSinkOperator::pending_finish() const {
    return !is_finished();
}

Status PaimonTableSinkOperator::set_cancelled(RuntimeState* state) {
    return Status::OK();
}

StatusOr<ChunkPtr> PaimonTableSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from paimon table sink operator");
}

Status PaimonTableSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    if (_writer == nullptr) {
        _writer = create_paimon_writer();
        RETURN_AND_SET_IF_ERROR(_writer->do_init(state));
    }
    _writer->set_output_expr(_output_expr);
    {
        SCOPED_TIMER(_write_timer);
        RETURN_AND_SET_IF_ERROR(_writer->write(state, chunk));
    }
    ++_num_chunk;
    if (!_use_native_writer && _num_chunk >= config::paimon_sink_commit_chunk_num) {
        // only used in jni writer
        RETURN_AND_SET_IF_ERROR(do_commit(state));
        _num_chunk = 0;
    }
    state->update_num_rows_load_sink(chunk->num_rows());
    return Status::OK();
}

Status PaimonTableSinkOperator::do_commit(RuntimeState* state) {
    SCOPED_TIMER(_commit_timer);
    if (_writer != nullptr) {
        RETURN_AND_SET_IF_ERROR(_writer->commit(state));
        add_paimon_commit_info(_writer->get_commit_message(), state);
        _writer->close(state);
        _writer = nullptr;
    }
    return Status::OK();
}

void PaimonTableSinkOperator::add_paimon_commit_info(const std::string& paimon_commit_info, RuntimeState* state) {
    TPaimonCommitMessage paimon_commit_message;
    paimon_commit_message.__set_version(paimon::CommitMessage::CurrentVersion());
    paimon_commit_message.__set_commit_message(std::move(paimon_commit_info));

    TSinkCommitInfo commit_info;
    commit_info.__set_paimon_commit_message(paimon_commit_message);

    // update runtime state
    state->add_sink_commit_info(commit_info);
}

std::unique_ptr<PaimonWriter> PaimonTableSinkOperator::create_paimon_writer() {
    if (_use_native_writer) {
        return std::make_unique<PaimonNativeWriter>(_paimon_table, _output_expr, _data_column_names, _data_column_types,
                                                    _convert_timer);
    }
    return std::move(create_paimon_jni_writer());
}

std::unique_ptr<PaimonWriter> PaimonTableSinkOperator::create_paimon_jni_writer() {
    std::map<std::string, std::string> jni_writer_params;
    jni_writer_params["native_table"] = _paimon_table->get_paimon_native_table();
    std::string writer_factory_class = "com/starrocks/paimon/reader/PaimonWriterFactory";
    return std::make_unique<starrocks::JniWriter>(writer_factory_class, jni_writer_params, _output_expr,
                                                  _data_column_types);
}

PaimonTableSinkOperatorFactory::PaimonTableSinkOperatorFactory(
        int32_t id, FragmentContext* fragment_ctx, PaimonTableDescriptor* paimon_table,
        const TPaimonTableSink& t_paimon_table_sink, vector<TExpr> t_output_expr,
        std::vector<ExprContext*> partition_expr_ctxs, std::vector<ExprContext*> output_expr_ctxs,
        std::vector<std::string> data_column_names, std::vector<std::string> data_column_types, bool use_native_writer)
        : OperatorFactory(id, "paimon_table_sink", Operator::s_pseudo_plan_node_id_for_final_sink),
          _t_output_expr(std::move(t_output_expr)),
          _output_expr_ctxs(std::move(output_expr_ctxs)),
          _partition_expr_ctxs(std::move(partition_expr_ctxs)),
          _paimon_table(std::move(paimon_table)),
          _data_column_names(std::move(data_column_names)),
          _data_column_types(std::move(data_column_types)),
          _use_native_writer(use_native_writer) {
    DCHECK(t_paimon_table_sink.__isset.target_table_id);
}

OperatorPtr PaimonTableSinkOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<PaimonTableSinkOperator>(this, _id, _plan_node_id, _paimon_table, driver_sequence,
                                                     _output_expr_ctxs, _data_column_names, _data_column_types,
                                                     _use_native_writer);
}

Status PaimonTableSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));

    RETURN_IF_ERROR(Expr::create_expr_trees(state->obj_pool(), _t_output_expr, &_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));

    RETURN_IF_ERROR(Expr::prepare(_partition_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_partition_expr_ctxs, state));

    return Status::OK();
}

void PaimonTableSinkOperatorFactory::close(RuntimeState* state) {
    Expr::close(_partition_expr_ctxs, state);
    Expr::close(_output_expr_ctxs, state);
    OperatorFactory::close(state);
}
} // namespace starrocks::pipeline
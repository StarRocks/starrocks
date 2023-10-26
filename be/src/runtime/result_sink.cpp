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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/result_sink.cpp

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

#include "runtime/result_sink.h"

#include <memory>

#include "common/config.h"
#include "exprs/expr.h"
#include "runtime/buffer_control_block.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/file_result_writer.h"
#include "runtime/mem_tracker.h"
#include "runtime/mysql_result_writer.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/statistic_result_writer.h"
#include "runtime/variable_result_writer.h"
#include "util/uid_util.h"

namespace starrocks {

ResultSink::ResultSink(const RowDescriptor& row_desc, const std::vector<TExpr>& t_output_expr, const TResultSink& sink,
                       int buffer_size)
        : _t_output_expr(t_output_expr), _buf_size(buffer_size) {
    if (!sink.__isset.type || sink.type == TResultSinkType::MYSQL_PROTOCAL) {
        _sink_type = TResultSinkType::MYSQL_PROTOCAL;
    } else {
        _sink_type = sink.type;
    }

    if (_sink_type == TResultSinkType::HTTP_PROTOCAL) {
        _format_type = sink.format;
    }

    if (_sink_type == TResultSinkType::FILE) {
        CHECK(sink.__isset.file_options);
        _file_opts = std::make_shared<ResultFileOptions>(sink.file_options);
    }

    _is_binary_format = sink.is_binary_row;
}

Status ResultSink::prepare_exprs(RuntimeState* state) {
    // From the thrift expressions create the real exprs.
    RETURN_IF_ERROR(Expr::create_expr_trees(state->obj_pool(), _t_output_expr, &_output_expr_ctxs, state));
    // Prepare the exprs to run.
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    return Status::OK();
}

Status ResultSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    std::stringstream title;
    if (_sink_type == TResultSinkType::STATISTIC) {
        title << "StatisticSender (dst_fragment_instance_id=" << print_id(state->fragment_instance_id()) << ")";
    } else {
        title << "DataBufferSender (dst_fragment_instance_id=" << print_id(state->fragment_instance_id()) << ")";
    }
    // create profile
    _profile = state->obj_pool()->add(new RuntimeProfile(title.str()));
    // prepare output_expr
    RETURN_IF_ERROR(prepare_exprs(state));

    // create sender
    RETURN_IF_ERROR(state->exec_env()->result_mgr()->create_sender(state->fragment_instance_id(), _buf_size, &_sender));

    // create writer based on sink type
    switch (_sink_type) {
    case TResultSinkType::MYSQL_PROTOCAL:
        _writer.reset(new (std::nothrow)
                              MysqlResultWriter(_sender.get(), _output_expr_ctxs, _is_binary_format, _profile));
        break;
    case TResultSinkType::FILE:
        CHECK(_file_opts.get() != nullptr);
        _writer.reset(new (std::nothrow) FileResultWriter(_file_opts.get(), _output_expr_ctxs, _profile));
        break;
    case TResultSinkType::STATISTIC:
        _writer.reset(new (std::nothrow) StatisticResultWriter(_sender.get(), _output_expr_ctxs, _profile));
        break;
    case TResultSinkType::VARIABLE:
        _writer.reset(new (std::nothrow) VariableResultWriter(_sender.get(), _output_expr_ctxs, _profile));
        break;
    default:
        return Status::InternalError("Unknown result sink type");
    }

    RETURN_IF_ERROR(_writer->init(state));
    return Status::OK();
}

Status ResultSink::open(RuntimeState* state) {
    RETURN_IF_ERROR(_writer->open(state));
    return Expr::open(_output_expr_ctxs, state);
}

Status ResultSink::send_chunk(RuntimeState* state, Chunk* chunk) {
    // The ResultWriter memory that sends the results is no longer recorded to the query memory.
    // There are two reason:
    // 1. the query result has come out, and then the memory limit is triggered, cancel, it is not necessary
    // 2. if this memory is counted, The memory of the receiving thread needs to be recorded,
    // and the life cycle of MemTracker needs to be considered
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(nullptr);
    return _writer->append_chunk(chunk);
}

Status ResultSink::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }

    Status final_status = exec_status;
    // close the writer
    if (_writer) {
        Status st = _writer->close();
        if (!st.ok() && exec_status.ok()) {
            // close file writer failed, should return this error to client
            final_status = st;
        }
    }

    // close sender, this is normal path end
    if (_sender) {
        if (_writer != nullptr) {
            _sender->update_num_written_rows(_writer->get_written_rows());
        }
        (void)_sender->close(final_status);
    }
    auto st = state->exec_env()->result_mgr()->cancel_at_time(
            time(nullptr) + config::result_buffer_cancelled_interval_time, state->fragment_instance_id());
    st.permit_unchecked_error();
    Expr::close(_output_expr_ctxs, state);

    _closed = true;
    return Status::OK();
}

void ResultSink::set_query_statistics(std::shared_ptr<QueryStatistics> statistics) {
    _sender->set_query_statistics(statistics);
}

} // namespace starrocks
/* vim: set ts=4 sw=4 sts=4 tw=100 : */

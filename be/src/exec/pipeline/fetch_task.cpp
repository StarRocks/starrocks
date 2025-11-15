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

#include "exec/pipeline/fetch_task.h"

#include <butil/iobuf.h>

#include <memory>

#include "exec/pipeline/fetch_processor.h"
#include "exec/pipeline/lookup_request.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "serde/column_array_serde.h"
#include "util/brpc_stub_cache.h"
#include "util/defer_op.h"
#include "util/disposable_closure.h"

namespace starrocks::pipeline {

std::string BatchUnit::debug_string() const {
    return fmt::format(
            "BatchUnit {}, input_chunks: {}, total_request_num: {}, finished_request_num: {}, "
            "next_output_idx: {}, build_output_done: {}",
            (void*)this, input_chunks.size(), total_request_num, finished_request_num.load(), next_output_idx,
            build_output_done);
}

Status FetchTask::submit(RuntimeState* state) {
    if (is_local()) {
        return _submit_local_task(state);
    } else {
        return _submit_remote_task(state);
    }
}

Status FetchTask::_submit_local_task(RuntimeState* state) {
    _ctx->callback = [ctx = _ctx](const Status& status) {
        DeferOp defer([&]() { ctx->unit->finished_request_num++; });
        if (!status.ok()) {
            LOG(WARNING) << "local fetch request failed, error: " << status.to_string();
            ctx->processor->_set_io_task_status(status);
            return;
        }
    };
    LookUpRequestContextPtr request = std::make_shared<LocalLookUpRequestContext>(_ctx);
    return _ctx->processor->_local_dispatcher->add_request(std::move(request));
}

Status FetchTask::_submit_remote_task(RuntimeState* state) {
    const auto source_id = _ctx->source_node_id;
    const auto& request_chunk = _ctx->request_chunk;

    auto* closure = new DisposableClosure<PLookUpResponse, FetchTaskContextPtr>(_ctx);
    closure->addSuccessHandler([this, closure](const FetchTaskContextPtr& ctx, const PLookUpResponse& resp) noexcept {
        DLOG(INFO) << "[GLM] receive a response, finished request num: " << ctx->unit->finished_request_num
                   << ", total request num: " << ctx->unit->total_request_num << ", " << (void*)ctx->processor
                   << ", latency: " << (MonotonicNanos() - ctx->send_ts) * 1.0 / 1000000 << "ms";
        DeferOp defer([&]() {
            if (++ctx->unit->finished_request_num == ctx->unit->total_request_num) {
                VLOG_ROW << "[GLM] all request finished, notify fetch processor, total_request_num: "
                         << ctx->unit->total_request_num << ", " << (void*)ctx->processor;
            }
            _is_done = true;
        });
        COUNTER_UPDATE(ctx->processor->_rpc_count, 1);
        COUNTER_UPDATE(ctx->processor->_network_timer, MonotonicNanos() - ctx->send_ts);

        if (resp.status().status_code() != TStatusCode::OK) {
            auto msg = fmt::format("fetch request failed, error: {}", resp.status().DebugString());
            LOG(WARNING) << msg;
            ctx->processor->_set_io_task_status(Status::InternalError(msg));
            return;
        }
        DLOG(INFO) << "[GLM] receive a response, response size: " << closure->cntl.response_attachment().size();
        if (closure->cntl.response_attachment().size() > 0) {
            SCOPED_TIMER(ctx->processor->_deserialize_timer);
            butil::IOBuf& io_buf = closure->cntl.response_attachment();
            raw::RawString buffer;

            for (size_t i = 0; i < resp.columns_size(); i++) {
                const auto& pcolumn = resp.columns(i);
                if (UNLIKELY(io_buf.size() < pcolumn.data_size())) {
                    auto msg = fmt::format("io_buf size {} is less than column data size {}", io_buf.size(),
                                           pcolumn.data_size());
                    LOG(WARNING) << msg;
                    ctx->processor->_set_io_task_status(Status::InternalError(msg));
                    return;
                }
                buffer.resize(pcolumn.data_size());
                size_t size = io_buf.cutn(buffer.data(), pcolumn.data_size());
                if (UNLIKELY(size != pcolumn.data_size())) {
                    auto msg = fmt::format("iobuf read {} != expected {}", size, pcolumn.data_size());
                    LOG(WARNING) << msg;
                    ctx->processor->_set_io_task_status(Status::InternalError(msg));
                    return;
                }
                int32_t slot_id = pcolumn.slot_id();
                const SlotDescriptor* slot_desc = ctx->processor->_slot_id_to_desc.at(slot_id);
                auto column = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
                const uint8_t* buff = reinterpret_cast<const uint8_t*>(buffer.data());
                auto ret = serde::ColumnArraySerde::deserialize(buff, column.get());
                if (!ret.ok()) {
                    auto msg = fmt::format("deserialize column error, slot_id: {}", slot_id);
                    LOG(WARNING) << msg;
                    ctx->processor->_set_io_task_status(Status::InternalError(msg));
                    return;
                }
                DCHECK(!ctx->response_columns.contains(slot_id));
                DLOG(INFO) << "[GLM] add response column, slot_id: " << slot_id << ", column: " << column->get_name();
                ctx->response_columns.insert({slot_id, std::move(column)});
            }
        }
    });

    closure->addFailedHandler([this](const FetchTaskContextPtr& ctx, std::string_view rpc_error_msg) noexcept {
        DeferOp defer([&]() {
            if (++ctx->unit->finished_request_num == ctx->unit->total_request_num) {
                DLOG(INFO) << "all request finished, notify fetch processor, " << (void*)ctx->processor;
            }
            _is_done = true;
        });
        LOG(WARNING) << "fetch request failed, error: " << rpc_error_msg;
        ctx->processor->_set_io_task_status(Status::InternalError(rpc_error_msg));
    });

    closure->cntl.Reset();
    closure->cntl.set_timeout_ms(state->query_options().query_timeout * 1000);

    PLookUpRequest request;
    PUniqueId p_query_id;
    p_query_id.set_hi(state->query_id().hi);
    p_query_id.set_lo(state->query_id().lo);
    *request.mutable_query_id() = std::move(p_query_id);
    request.set_lookup_node_id(_ctx->processor->_target_node_id);
    request.set_request_tuple_id(_ctx->request_tuple_id);
    {
        SCOPED_TIMER(_ctx->processor->_serialize_timer);
        size_t max_serialize_size = 0;
        for (const auto& column : request_chunk->columns()) {
            max_serialize_size += serde::ColumnArraySerde::max_serialized_size(*column);
        }

        _ctx->processor->_serialize_buffer.clear();
        _ctx->processor->_serialize_buffer.resize(max_serialize_size);

        uint8_t* buff = reinterpret_cast<uint8_t*>(_ctx->processor->_serialize_buffer.data());
        uint8_t* begin = buff;
        for (const auto& [slot_id, idx] : request_chunk->get_slot_id_to_index_map()) {
            if (slot_id == FetchProcessor::kPositionColumnSlotId) {
                // we don't need to send position column to remote node
                continue;
            }
            auto p_column = request.add_request_columns();
            p_column->set_slot_id(slot_id);
            const auto& column = request_chunk->get_column_by_index(idx);
            uint8_t* start = buff;
            ASSIGN_OR_RETURN(buff, serde::ColumnArraySerde::serialize(*column, buff));
            p_column->set_data_size(buff - start);
        }
        size_t actual_serialize_size = buff - begin;
        closure->cntl.request_attachment().append(_ctx->processor->_serialize_buffer.data(), actual_serialize_size);
    }
    DLOG(INFO) << "[GLM] send fetch request, source_id: " << source_id << ", " << (void*)_ctx->processor
               << ", unit: " << _ctx->unit->debug_string();
    _ctx->send_ts = MonotonicNanos();
    const auto* node_info = _ctx->processor->_nodes_info->find_node(source_id);
    auto stub = state->exec_env()->brpc_stub_cache()->get_stub(node_info->host, node_info->brpc_port);
    stub->lookup(&closure->cntl, &request, &closure->result, closure);

    return Status::OK();
}

} // namespace starrocks::pipeline
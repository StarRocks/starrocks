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

#include <algorithm>
#include <memory>

#include "base/brpc/disposable_closure.h"
#include "base/brpc/ref_count_closure.h"
#include "base/status_fmt.hpp"
#include "base/time/time.h"
#include "base/utility/defer_op.h"
#include "common/brpc/brpc_stub_cache.h"
#include "exec/pipeline/fetch_processor.h"
#include "exec/pipeline/lookup_request.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "serde/column_array_serde.h"

namespace starrocks::pipeline {

namespace {

bool may_serialize_large_lookup_response(const TupleDescriptor* tuple_desc, const RowPositionDescriptor* row_pos_desc) {
    const auto& lookup = row_pos_desc->get_lookup_ref_slot_ids();
    const auto& fetch = row_pos_desc->get_fetch_ref_slot_ids();

    for (const auto* slot : tuple_desc->slots()) {
        const auto slot_id = slot->id();
        if (std::find(lookup.begin(), lookup.end(), slot_id) != lookup.end()) {
            continue;
        }
        if (std::find(fetch.begin(), fetch.end(), slot_id) != fetch.end()) {
            continue;
        }
        const auto& type = slot->type();
        if ((type.is_string_type() && type.len > 512 * 1024) || type.is_huge_type() || type.is_complex_type()) {
            return true;
        }
    }
    return false;
}

bool should_use_lookup_http_rpc(RuntimeState* state, const PLookUpRequest& request, size_t request_attachment_size,
                                TupleId request_tuple_id,
                                const phmap::flat_hash_map<TupleId, RowPositionDescriptor*>& row_pos_descs) {
    const int64_t rpc_http_min_size = state->get_rpc_http_min_size();
    if (rpc_http_min_size < 0) {
        return false;
    }

    const size_t http_iobuf_size = request.ByteSizeLong() + request_attachment_size + sizeof(size_t) * 2;
    if (http_iobuf_size > static_cast<size_t>(rpc_http_min_size)) {
        return true;
    }

    const auto row_pos_it = row_pos_descs.find(request_tuple_id);
    const auto* tuple_desc = state->desc_tbl().get_tuple_descriptor(request_tuple_id);
    return row_pos_it != row_pos_descs.end() && tuple_desc != nullptr &&
           may_serialize_large_lookup_response(tuple_desc, row_pos_it->second);
}

} // namespace

std::string BatchUnit::debug_string() const {
    return fmt::format(
            "BatchUnit {}, input_chunks: {}, total_request_num: {}, finished_request_num: {}, "
            "next_output_idx: {}, build_output_done: {}",
            (void*)this, input_chunks.size(), total_request_num, finished_request_num.load(), next_output_idx,
            build_output_done);
}

Status FetchTask::submit(RuntimeState* state) {
    return _submit_remote_task(state);
}

Status FetchTask::_submit_remote_task(RuntimeState* state) {
    const auto source_id = _ctx->source_node_id;
    const auto& request_chunk = _ctx->request_chunk;

    auto closure = std::make_unique<DisposableClosure<PLookUpResponse, FetchTaskContextPtr>>(_ctx);
    // The RPC callback can outlive queue ownership when the source finishes early.
    auto self = shared_from_this();
    auto processor = _ctx->processor.lock();
    DCHECK(processor != nullptr);
    const auto* node_info = processor->_nodes_info->find_node(source_id);
    DCHECK(node_info != nullptr);
    RETURN_IF(node_info == nullptr,
              Status::InternalError(fmt::format("Failed to find node info for source_id: {}", source_id)));
    closure->addSuccessHandler([self, done = closure.get(), host = node_info->host, port = node_info->brpc_port](
                                       const FetchTaskContextPtr& ctx, const PLookUpResponse& resp) noexcept {
        auto processor = ctx->processor.lock();
        auto unit = ctx->unit.lock();
        if (processor == nullptr || unit == nullptr) {
            self->_is_done = true;
            return;
        }
        DLOG(INFO) << "[GLM] receive a response, finished request num: " << unit->finished_request_num
                   << ", total request num: " << unit->total_request_num
                   << ", latency: " << (MonotonicNanos() - ctx->send_ts) * 1.0 / 1000000 << "ms";
        DeferOp defer([&]() {
            if (++unit->finished_request_num == unit->total_request_num) {
                VLOG_ROW << "[GLM] all request finished, notify fetch processor, total_request_num: "
                         << unit->total_request_num;
            }
            self->_is_done = true;
        });
        COUNTER_UPDATE(processor->_rpc_count, 1);
        COUNTER_UPDATE(processor->_network_timer, MonotonicNanos() - ctx->send_ts);

        const PLookUpResponse* actual_resp = &resp;
        PLookUpResponse http_resp;
        // HTTP fallback uses a framed response body, so brpc does not parse it
        // into the protobuf response object. Parse the frame manually first.
        if (done->cntl.response() == nullptr) {
            Status st = parse_lookup_response_from_http_iobuf(&done->cntl.response_attachment(), &http_resp);
            if (!st.ok()) {
                processor->_set_io_task_status(st);
                LOG(WARNING) << "parse lookup http response failed, error: " << st;
                return;
            }
            actual_resp = &http_resp;
        }

        if (actual_resp->status().status_code() != TStatusCode::OK) {
            auto msg = fmt::format("fetch request failed, error: {}, host: {}, port: {}",
                                   actual_resp->status().DebugString(), host, port);
            LOG(WARNING) << msg;
            processor->_set_io_task_status(Status::InternalError(msg));
            return;
        }
        DLOG(INFO) << "[GLM] receive a response, response size: " << done->cntl.response_attachment().size();
        if (done->cntl.response_attachment().size() > 0) {
            SCOPED_TIMER(processor->_deserialize_timer);
            butil::IOBuf& io_buf = done->cntl.response_attachment();
            raw::RawString buffer;

            for (size_t i = 0; i < actual_resp->columns_size(); i++) {
                const auto& pcolumn = actual_resp->columns(i);
                if (UNLIKELY(io_buf.size() < pcolumn.data_size())) {
                    auto msg = fmt::format("io_buf size {} is less than column data size {}", io_buf.size(),
                                           pcolumn.data_size());
                    LOG(WARNING) << msg;
                    processor->_set_io_task_status(Status::InternalError(msg));
                    return;
                }
                buffer.resize(pcolumn.data_size());
                size_t size = io_buf.cutn(buffer.data(), pcolumn.data_size());
                if (UNLIKELY(size != pcolumn.data_size())) {
                    auto msg = fmt::format("iobuf read {} != expected {}", size, pcolumn.data_size());
                    LOG(WARNING) << msg;
                    processor->_set_io_task_status(Status::InternalError(msg));
                    return;
                }
                int32_t slot_id = pcolumn.slot_id();
                const SlotDescriptor* slot_desc = processor->_slot_id_to_desc.at(slot_id);
                auto column = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
                const uint8_t* buff = reinterpret_cast<const uint8_t*>(buffer.data());
                auto ret = serde::ColumnArraySerde::deserialize(buff, buff + buffer.size(), column.get());
                if (!ret.ok()) {
                    auto msg = fmt::format("deserialize column error, slot_id: {}", slot_id);
                    LOG(WARNING) << msg;
                    processor->_set_io_task_status(Status::InternalError(msg));
                    return;
                }
                DCHECK(!ctx->response_columns.contains(slot_id));
                DLOG(INFO) << "[GLM] add response column, slot_id: " << slot_id << ", column: " << column->get_name();
                ctx->response_columns.insert({slot_id, std::move(column)});
            }
            if (UNLIKELY(!io_buf.empty())) {
                auto msg = fmt::format("lookup response attachment has {} trailing bytes", io_buf.size());
                LOG(WARNING) << msg;
                processor->_set_io_task_status(Status::InternalError(msg));
                return;
            }
        }
    });

    closure->addFailureHandler([self](const FetchTaskContextPtr& ctx, std::string_view rpc_error_msg) noexcept {
        auto processor = ctx->processor.lock();
        auto unit = ctx->unit.lock();
        if (processor == nullptr || unit == nullptr) {
            self->_is_done = true;
            return;
        }
        DeferOp defer([&]() {
            if (++unit->finished_request_num == unit->total_request_num) {
                DLOG(INFO) << "all request finished, notify fetch processor, " << (void*)processor.get();
            }
            self->_is_done = true;
        });
        processor->_set_io_task_status(Status::InternalError(rpc_error_msg));
        LOG(WARNING) << "fetch request failed, error: " << rpc_error_msg;
    });

    closure->cntl.Reset();
    closure->cntl.set_timeout_ms(state->query_options().query_timeout * 1000);

    PLookUpRequest request;
    PUniqueId p_query_id;
    p_query_id.set_hi(state->query_id().hi);
    p_query_id.set_lo(state->query_id().lo);
    *request.mutable_query_id() = std::move(p_query_id);
    request.set_lookup_node_id(processor->_target_node_id);
    request.set_request_tuple_id(_ctx->request_tuple_id);
    size_t actual_serialize_size = 0;
    {
        SCOPED_TIMER(processor->_serialize_timer);
        size_t max_serialize_size = 0;
        for (const auto& column : request_chunk->columns()) {
            max_serialize_size += serde::ColumnArraySerde::max_serialized_size(*column);
        }

        processor->_serialize_buffer.clear();
        processor->_serialize_buffer.resize(max_serialize_size);

        uint8_t* buff = reinterpret_cast<uint8_t*>(processor->_serialize_buffer.data());
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
        actual_serialize_size = buff - begin;
    }
    auto unit = _ctx->unit.lock();
    auto unit_debug_string = unit != nullptr ? unit->debug_string() : std::string("BatchUnit <expired>");
    DLOG(INFO) << "[GLM] send fetch request, source_id: " << source_id << ", " << (void*)processor.get()
               << ", unit: " << unit_debug_string;

    const bool use_http = should_use_lookup_http_rpc(state, request, actual_serialize_size, _ctx->request_tuple_id,
                                                     processor->_row_pos_descs);

    _ctx->send_ts = MonotonicNanos();
    if (use_http) {
        TNetworkAddress brpc_addr;
        brpc_addr.hostname = node_info->host;
        brpc_addr.port = node_info->brpc_port;

        closure->cntl.http_request().set_content_type("application/proto");
        butil::IOBuf request_iobuf;
        butil::IOBufAsZeroCopyOutputStream wrapper(&request_iobuf);
        if (UNLIKELY(!request.SerializeToZeroCopyStream(&wrapper))) {
            return Status::InternalError("failed to serialize lookup request protobuf");
        }
        size_t request_size = request_iobuf.size();
        closure->cntl.request_attachment().append(&request_size, sizeof(request_size));
        closure->cntl.request_attachment().append(request_iobuf);
        size_t attachment_size = actual_serialize_size;
        closure->cntl.request_attachment().append(&attachment_size, sizeof(attachment_size));
        closure->cntl.request_attachment().append(processor->_serialize_buffer.data(), actual_serialize_size);

        auto res = HttpBrpcStubCache::getInstance()->get_http_stub(brpc_addr);
        if (!res.ok()) {
            return res.status();
        }

        auto done = closure.release();
        // Keep the framed HTTP body in response_attachment instead of letting brpc parse it as PLookUpResponse.
        res.value()->lookup_via_http(&done->cntl, nullptr, nullptr, done);
    } else {
        auto* query_execution_services = state->query_execution_services();
        auto stub = query_execution_services->rpc->brpc_stub_cache->get_stub(node_info->host, node_info->brpc_port);
        if (stub == nullptr) {
            auto msg = fmt::format("Connect {}:{} failed.", node_info->host, node_info->brpc_port);
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }

        closure->cntl.request_attachment().append(processor->_serialize_buffer.data(), actual_serialize_size);
        auto done = closure.release();
        stub->lookup(&done->cntl, &request, &done->result, done);
    }

    return Status::OK();
}

void LookUpCloseTask::submit(RuntimeState* state) {
    auto* query_execution_services = state->query_execution_services();
    auto stub = query_execution_services->rpc->brpc_stub_cache->get_stub(_host, _port);
    if (stub == nullptr) {
        auto msg = fmt::format("Connect {}:{} failed.", _host, _port);
        LOG(WARNING) << msg;
        return;
    }
    PLookUpCloseRequest request;
    request.set_lookup_node_id(_target_node_id);
    PUniqueId p_query_id;
    p_query_id.set_hi(state->query_id().hi);
    p_query_id.set_lo(state->query_id().lo);
    *request.mutable_query_id() = std::move(p_query_id);

    auto* closure = new DisposableClosure<PLookUpCloseResponse, int>(0);
    closure->addFailureHandler([](int ctx, std::string_view rpc_error_msg) noexcept {
        LOG(WARNING) << "lookup close rpc failed:" << rpc_error_msg;
    });
    closure->addSuccessHandler([](int ctx, const PLookUpCloseResponse& resp) noexcept {
        if (resp.status().status_code() != TStatusCode::OK) {
            LOG(WARNING) << "lookup close failed, error: " << resp.status().DebugString();
        }
    });
    closure->cntl.set_timeout_ms(state->query_options().query_timeout * 1000);
    stub->lookup_close(&closure->cntl, &request, &closure->result, closure);
}

} // namespace starrocks::pipeline

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

#include "runtime/batch_write/batch_write_mgr.h"

#include "brpc/controller.h"
#include "butil/endpoint.h"
#include "gen_cpp/internal_service.pb.h"
#include "http/http_common.h"
#include "runtime/batch_write/batch_write_util.h"
#include "runtime/exec_env.h"
#include "runtime/stream_load/time_bounded_stream_load_pipe.h"
#include "testutil/sync_point.h"

namespace starrocks {

Status BatchWriteMgr::register_stream_load_pipe(StreamLoadContext* pipe_ctx) {
    BatchWriteId batch_write_id = {
            .db = pipe_ctx->db, .table = pipe_ctx->table, .load_params = pipe_ctx->load_parameters};
    ASSIGN_OR_RETURN(IsomorphicBatchWriteSharedPtr batch_write, _get_batch_write(batch_write_id, true));
    return batch_write->register_stream_load_pipe(pipe_ctx);
}

void BatchWriteMgr::unregister_stream_load_pipe(StreamLoadContext* pipe_ctx) {
    BatchWriteId batch_write_id = {
            .db = pipe_ctx->db, .table = pipe_ctx->table, .load_params = pipe_ctx->load_parameters};
    auto batch_write = _get_batch_write(batch_write_id, false);
    if (!batch_write.ok()) {
        return;
    }
    batch_write.value()->unregister_stream_load_pipe(pipe_ctx);
}

StatusOr<IsomorphicBatchWriteSharedPtr> BatchWriteMgr::get_batch_write(const BatchWriteId& batch_write_id) {
    return _get_batch_write(batch_write_id, false);
}

Status BatchWriteMgr::append_data(StreamLoadContext* data_ctx) {
    TEST_SYNC_POINT_CALLBACK("BatchWriteMgr::append_data::cb", &data_ctx);
    TEST_SUCC_POINT("BatchWriteMgr::append_data::success");
    TEST_ERROR_POINT("BatchWriteMgr::append_data::fail");
    BatchWriteId batch_write_id = {
            .db = data_ctx->db, .table = data_ctx->table, .load_params = data_ctx->load_parameters};
    ASSIGN_OR_RETURN(IsomorphicBatchWriteSharedPtr batch_write, _get_batch_write(batch_write_id, true));
    return batch_write->append_data(data_ctx);
}

StatusOr<IsomorphicBatchWriteSharedPtr> BatchWriteMgr::_get_batch_write(const starrocks::BatchWriteId& batch_write_id,
                                                                        bool create_if_missing) {
    {
        std::shared_lock<bthreads::BThreadSharedMutex> lock(_rw_mutex);
        auto it = _batch_write_map.find(batch_write_id);
        if (it != _batch_write_map.end()) {
            return it->second;
        }
    }
    if (!create_if_missing) {
        return Status::NotFound("");
    }

    std::unique_lock<bthreads::BThreadSharedMutex> lock(_rw_mutex);
    if (_stopped) {
        return Status::ServiceUnavailable("Batch write is stopped");
    }
    auto it = _batch_write_map.find(batch_write_id);
    if (it != _batch_write_map.end()) {
        return it->second;
    }

    auto batch_write = std::make_shared<IsomorphicBatchWrite>(batch_write_id, _executor.get());
    Status st = batch_write->init();
    if (!st.ok()) {
        LOG(ERROR) << "Fail to init batch write, " << batch_write_id << ", status: " << st;
        return Status::InternalError("Fail to init batch write, error: " + st.to_string());
    }
    _batch_write_map.emplace(batch_write_id, batch_write);
    LOG(INFO) << "Create batch write, " << batch_write_id;
    return batch_write;
}

void BatchWriteMgr::stop() {
    std::vector<IsomorphicBatchWriteSharedPtr> stop_writes;
    {
        std::unique_lock<bthreads::BThreadSharedMutex> lock(_rw_mutex);
        if (_stopped) {
            return;
        }
        _stopped = true;
        for (auto& [_, batch_write] : _batch_write_map) {
            stop_writes.emplace_back(batch_write);
        }
        _batch_write_map.clear();
    }
    for (auto& batch_write : stop_writes) {
        batch_write->stop();
    }
}

StatusOr<StreamLoadContext*> BatchWriteMgr::create_and_register_pipe(
        ExecEnv* exec_env, BatchWriteMgr* batch_write_mgr, const std::string& db, const std::string& table,
        const std::map<std::string, std::string>& load_parameters, const std::string& label, long txn_id,
        const TUniqueId& load_id, int32_t batch_write_interval_ms) {
    std::string pipe_name = fmt::format("txn_{}_label_{}_id_{}", txn_id, label, print_id(load_id));
    auto pipe = std::make_shared<TimeBoundedStreamLoadPipe>(pipe_name, batch_write_interval_ms,
                                                            config::merge_commit_stream_load_pipe_block_wait_us,
                                                            config::merge_commit_stream_load_pipe_max_buffered_bytes);
    RETURN_IF_ERROR(exec_env->load_stream_mgr()->put(load_id, pipe));
    StreamLoadContext* ctx = new StreamLoadContext(exec_env, load_id);
    ctx->ref();
    ctx->id = load_id;
    ctx->db = db;
    ctx->table = table;
    ctx->label = label;
    ctx->txn_id = txn_id;
    ctx->enable_batch_write = true;
    ctx->load_parameters = load_parameters;
    ctx->body_sink = pipe;

    DeferOp op([&] {
        // batch write manager will hold a reference if register success.
        // need to release the reference to delete the context if register failed
        StreamLoadContext::release(ctx);
    });
    RETURN_IF_ERROR(batch_write_mgr->register_stream_load_pipe(ctx));
    return ctx;
}

static std::string s_empty;

#define GET_PARAMETER_OR_EMPTY(parameters, key) \
    (parameters.find(key) != parameters.end() ? parameters.at(key) : s_empty)

#define ASSIGN_AND_RETURN(lhs, rhs) \
    lhs = rhs;                      \
    return

void BatchWriteMgr::receive_stream_load_rpc(ExecEnv* exec_env, brpc::Controller* cntl,
                                            const PStreamLoadRequest* request, PStreamLoadResponse* response) {
    auto* ctx = new StreamLoadContext(exec_env);
    ctx->ref();
    DeferOp defer([&]() {
        response->set_json_result(ctx->to_json());
        StreamLoadContext::release(ctx);
    });
    ctx->db = request->db();
    ctx->table = request->table();
    std::map<std::string, std::string> parameters;
    for (const PStringPair& pair : request->parameters()) {
        parameters.emplace(pair.key(), pair.val());
    }

    {
        auto value = GET_PARAMETER_OR_EMPTY(parameters, HTTP_ENABLE_MERGE_COMMIT);
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        ctx->enable_batch_write = StringParser::string_to_bool(value.c_str(), value.length(), &parse_result);
        if (UNLIKELY(parse_result != StringParser::PARSE_SUCCESS)) {
            ASSIGN_AND_RETURN(ctx->status, Status::InvalidArgument(fmt::format(
                                                   "Invalid parameter {}. The value must be bool type, but is {}",
                                                   HTTP_ENABLE_MERGE_COMMIT, value)));
        }
        if (!ctx->enable_batch_write) {
            ASSIGN_AND_RETURN(ctx->status,
                              Status::InvalidArgument(fmt::format(
                                      "RPC interface only support batch write currently. Must set {} to true",
                                      HTTP_ENABLE_MERGE_COMMIT, value)));
        }
    }
    ctx->label = GET_PARAMETER_OR_EMPTY(parameters, HTTP_LABEL_KEY);
    if (ctx->label.empty()) {
        ctx->label = generate_uuid_string();
    }
    std::string timeout = GET_PARAMETER_OR_EMPTY(parameters, HTTP_TIMEOUT);
    if (!timeout.empty()) {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        auto timeout_second =
                StringParser::string_to_unsigned_int<int32_t>(timeout.c_str(), timeout.length(), &parse_result);
        if (UNLIKELY(parse_result != StringParser::PARSE_SUCCESS)) {
            ASSIGN_AND_RETURN(ctx->status, Status::InvalidArgument(fmt::format("Invalid timeout format: {}", timeout)));
        }
        ctx->timeout_second = timeout_second;
    }
    auto user_ip = butil::ip2str(cntl->remote_side().ip);
    ctx->auth.user = request->user();
    ctx->auth.passwd = request->passwd();
    ctx->auth.user_ip.assign(user_ip.c_str());
    ctx->load_parameters = get_load_parameters_from_brpc(parameters);

    butil::IOBuf& io_buf = cntl->request_attachment();
    size_t max_bytes = config::streaming_load_max_batch_size_mb * 1024 * 1024;
    if (io_buf.empty()) {
        ASSIGN_AND_RETURN(ctx->status, Status::InternalError(fmt::format("The data can not be empty")));
    } else if (io_buf.size() > max_bytes) {
        ASSIGN_AND_RETURN(ctx->status, Status::InternalError(fmt::format(
                                               "The data size {} exceed the max size {}. You can change the max size"
                                               " by modify config::streaming_load_max_batch_size_mb on BE",
                                               io_buf.size(), max_bytes)));
    }
    auto st_or = ByteBuffer::allocate_with_tracker(io_buf.size());
    if (st_or.ok()) {
        ctx->buffer = std::move(st_or.value());
    } else {
        ASSIGN_AND_RETURN(ctx->status,
                          Status::InternalError(fmt::format("Can't allocate buffer, data size: {}, error: {}",
                                                            io_buf.size(), st_or.status().to_string())));
    }
    auto copy_size = io_buf.copy_to(ctx->buffer->ptr, io_buf.size());
    if (copy_size != io_buf.size()) {
        ASSIGN_AND_RETURN(ctx->status,
                          Status::InternalError(fmt::format("Failed to copy buffer, data size: {}, copied size: {}",
                                                            io_buf.size(), copy_size)));
    }
    ctx->buffer->pos += io_buf.size();
    ctx->buffer->flip();
    ctx->receive_bytes = io_buf.size();
    ctx->mc_read_data_cost_nanos = MonotonicNanos() - ctx->start_nanos;
    ctx->status = exec_env->batch_write_mgr()->append_data(ctx);
}

} // namespace starrocks
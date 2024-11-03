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
#include "http/http_request.h"
#include "http/utils.h"
#include "runtime/batch_write/batch_write_util.h"
#include "runtime/exec_env.h"
#include "runtime/stream_load/time_bounded_stream_load_pipe.h"

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

Status BatchWriteMgr::append_data(const BatchWriteId& batch_write_id, StreamLoadContext* data_ctx) {
    ASSIGN_OR_RETURN(IsomorphicBatchWriteSharedPtr batch_write, _get_batch_write(batch_write_id, true));
    return batch_write->append_data(data_ctx);
}

StatusOr<IsomorphicBatchWriteSharedPtr> BatchWriteMgr::_get_batch_write(const starrocks::BatchWriteId& batch_write_id,
                                                                        bool create_if_missing) {
    {
        std::shared_lock<std::shared_mutex> lock(_mutex);
        auto it = _batch_write_map.find(batch_write_id);
        if (it != _batch_write_map.end()) {
            return it->second;
        }
    }
    if (!create_if_missing) {
        return Status::NotFound("");
    }

    std::unique_lock<std::shared_mutex> lock(_mutex);
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
        return Status::InternalError("Fail to init batch write, " + st.to_string());
    }
    _batch_write_map.emplace(batch_write_id, batch_write);
    LOG(INFO) << "Create batch write, " << batch_write_id;
    return batch_write;
}

void BatchWriteMgr::stop() {
    std::vector<IsomorphicBatchWriteSharedPtr> stop_writes;
    {
        std::unique_lock<std::shared_mutex> lock(_mutex);
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

StatusOr<StreamLoadContext*> BatchWriteMgr::create_and_register_context(
        ExecEnv* exec_env, const std::string& db, const std::string& table, const std::string& label, long txn_id,
        const TUniqueId& load_id, int32_t batch_write_interval_ms,
        const std::map<std::string, std::string>& load_parameters) {
    auto pipe = std::make_shared<TimeBoundedStreamLoadPipe>(batch_write_interval_ms);
    RETURN_IF_ERROR(exec_env->load_stream_mgr()->put(load_id, pipe));
    StreamLoadContext* ctx = new StreamLoadContext(exec_env, load_id);
    ctx->ref();
    ctx->db = db;
    ctx->table = table;
    ctx->label = label;
    ctx->txn_id = txn_id;
    ctx->body_sink = pipe;
    ctx->enable_batch_write = true;
    ctx->load_parameters = load_parameters;
    DeferOp op([&] {
        // batch write manager will hold a reference if register success.
        // need to release the reference to delete the context if register failed
        StreamLoadContext::release(ctx);
    });
    return exec_env->batch_write_mgr()->register_stream_load_pipe(ctx);
}

static std::string s_empty;

#define GET_PARAMETER_OR_EMPTY(parameters, key) \
    (parameters.find(key) != parameters.end() ? parameters.at(key) : s_empty)

bool parse_basic_auth(const std::map<std::string, std::string>& parameters, const std::string& remote_host,
                      AuthInfo* auth) {
    std::string full_user;
    auto& auth_str = GET_PARAMETER_OR_EMPTY(parameters, HttpHeaders::AUTHORIZATION);
    if (!parse_basic_auth(auth_str, &full_user, &auth->passwd)) {
        return false;
    }
    auto pos = full_user.find('@');
    if (pos != std::string::npos) {
        auth->user.assign(full_user.data(), pos);
        auth->cluster.assign(full_user.data() + pos + 1);
    } else {
        auth->user = full_user;
    }
    auth->user_ip.assign(remote_host);
    return true;
}

void BatchWriteMgr::receive_rpc_request(ExecEnv* exec_env, brpc::Controller* cntl,
                                        const PBatchWriteLoadRequest* request, PBatchWriteLoadResponse* response) {
    auto* ctx = new StreamLoadContext(exec_env);
    ctx->ref();
    DeferOp defer([&]() {
        response->set_json_result(ctx->to_json());
        ctx->buffer = nullptr;
        StreamLoadContext::release(ctx);
    });
    ctx->db = request->db();
    ctx->table = request->table();
    std::map<std::string, std::string> parameters;
    for (const PKeyValue& kv : request->parameters()) {
        parameters.emplace(kv.key(), kv.value());
    }

    {
        auto value = GET_PARAMETER_OR_EMPTY(parameters, HTTP_ENABLE_BATCH_WRITE);
        if (value.empty()) {
            ctx->status = Status::InvalidArgument("RPC request must set parameter " + HTTP_ENABLE_BATCH_WRITE);
            return;
        }
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        ctx->enable_batch_write = StringParser::string_to_bool(value.c_str(), value.length(), &parse_result);
        if (UNLIKELY(parse_result != StringParser::PARSE_SUCCESS)) {
            ctx->status = Status::InvalidArgument(fmt::format(
                    "Invalid parameter {}. The value must be be bool type, but is {}", HTTP_ENABLE_BATCH_WRITE, value));
            return;
        }
    }

    ctx->label = GET_PARAMETER_OR_EMPTY(parameters, HTTP_LABEL_KEY);
    if (ctx->label.empty()) {
        ctx->label = generate_uuid_string();
    }

    std::string remote_host;
    butil::ip2hostname(cntl->remote_side().ip, &remote_host);
    bool auth_ret = parse_basic_auth(parameters, remote_host, &ctx->auth);
    if (!auth_ret) {
        ctx->status = Status::InternalError("no valid basic authorization");
        return;
    }

    {
        auto value = GET_PARAMETER_OR_EMPTY(parameters, HTTP_TIMEOUT);
        if (!value.empty()) {
            StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
            auto timeout_second =
                    StringParser::string_to_unsigned_int<int32_t>(value.c_str(), value.length(), &parse_result);
            if (UNLIKELY(parse_result != StringParser::PARSE_SUCCESS)) {
                ctx->status = Status::InvalidArgument("Invalid timeout format: " + value);
                return;
            }
            ctx->timeout_second = timeout_second;
        }
    }

    auto ret = get_batch_write_load_parameters(parameters);
    if (!ret.ok()) {
        ctx->status = ret.status();
        return;
    }

    butil::IOBuf& io_buf = cntl->request_attachment();
    size_t max_bytes = config::streaming_load_max_batch_size_mb * 1024 * 1024;
    if (io_buf.size() > max_bytes) {
        ctx->status =
                Status::InternalError(fmt::format("The data size {} exceed the max size {}. You can change the max size"
                                                  " by modify config::streaming_load_max_batch_size_mb on BE",
                                                  io_buf.size(), max_bytes));
        return;
    }

    auto st_or = ByteBuffer::allocate_with_tracker(io_buf.size());
    if (st_or.ok()) {
        ctx->buffer = st_or.value();
    } else {
        ctx->status = Status::InternalError(fmt::format("Can't allocate buffer, data size: {}, error: {}",
                                                        io_buf.size(), st_or.status().to_string()));
        return;
    }
    auto copy_size = io_buf.copy_to(ctx->buffer->ptr, io_buf.size());
    if (copy_size != io_buf.size()) {
        ctx->status = Status::InternalError(
                fmt::format("Failed to copy buffer, data size: {}, copied size: {}", io_buf.size(), copy_size));
        return;
    }
    ctx->buffer->pos += io_buf.size();
    ctx->buffer->flip();

    BatchWriteId batch_write_id{.db = ctx->db, .table = ctx->table, .load_params = std::move(ret.value())};
    ctx->status = exec_env->batch_write_mgr()->append_data(batch_write_id, ctx);
}

} // namespace starrocks
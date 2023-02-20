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

#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>

#include "fmt/format.h"
#include "stream_load_context.h"
#include "util/starrocks_metrics.h"
#include "util/thread.h"
#include "util/uid_util.h" // for std::hash for UniqueId

namespace starrocks {

class HttpRequest;

// used to register all streams in process so that other module can get this stream
class StreamContextMgr {
public:
    StreamContextMgr() = default;

    ~StreamContextMgr() {
        std::lock_guard<std::mutex> l(_lock);
        for (auto& iter : _stream_map) {
            if (iter.second->unref()) {
                delete iter.second;
            }
        }
    }

    Status put(const std::string& id, StreamLoadContext* stream) {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _stream_map.find(id);
        if (it != std::end(_stream_map)) {
            return Status::InternalError("id already exist");
        }
        _stream_map.emplace(id, stream);
        stream->ref();
        VLOG(3) << "put stream load pipe: " << id;
        return Status::OK();
    }

    StreamLoadContext* get(const std::string& id) {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _stream_map.find(id);
        if (it == std::end(_stream_map)) {
            return nullptr;
        }
        auto stream = it->second;
        stream->ref();
        return stream;
    }

    void remove(const std::string& id) {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _stream_map.find(id);
        if (it != std::end(_stream_map)) {
            if (it->second->unref()) {
                delete it->second;
            }
            _stream_map.erase(it);
            VLOG(3) << "remove stream load pipe: " << id;
        }
    }

    std::vector<std::string> get_ids() {
        std::vector<std::string> ids;
        {
            std::lock_guard<std::mutex> l(_lock);
            for (auto& iter : _stream_map) {
                ids.emplace_back(iter.first);
            }
        }

        return ids;
    }

    Status create_channel_context(ExecEnv* exec_env, const string& label, int channel_id, const string& db_name,
                                  const string& table_name, TFileFormatType::type format, StreamLoadContext*& ctx,
                                  const TUniqueId& load_id, long txn_id) {
        auto pipe = std::make_shared<StreamLoadPipe>();
        RETURN_IF_ERROR(exec_env->load_stream_mgr()->put(load_id, pipe));
        ctx = new StreamLoadContext(exec_env, load_id);
        if (ctx == nullptr) {
            return Status::InternalError("allocate stream load context fail");
        }
        ctx->ref();

        ctx->load_type = TLoadType::MANUAL_LOAD;
        ctx->load_src_type = TLoadSourceType::RAW;

        ctx->db = db_name;
        ctx->table = table_name;
        ctx->label = label;
        ctx->channel_id = channel_id;

        ctx->start_nanos = UnixSeconds();
        ctx->last_active_ts = ctx->start_nanos;
        ctx->need_rollback = false;
        ctx->format = format;

        ctx->body_sink = pipe;
        ctx->txn_id = txn_id;

        return Status::OK();
    }

    Status put_channel_context(const string& label, int channel_id, StreamLoadContext* ctx) {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _channel_stream_map.find(label);
        if (it == std::end(_channel_stream_map)) {
            std::unordered_map<int, StreamLoadContext*> empty_map;
            it = _channel_stream_map.emplace(label, std::move(empty_map)).first;
        }
        auto& label_channel_map = it->second;
        auto it2 = label_channel_map.find(channel_id);
        if (it2 != std::end(label_channel_map)) {
            return Status::InternalError("channel id " + std::to_string(channel_id) + " for label " + label +
                                         " already exist");
        }
        ctx->ref();
        label_channel_map.emplace(channel_id, ctx);
        LOG(INFO) << "stream load: " << label << ", channel_id: " << std::to_string(channel_id) << " start pipe";
        return Status::OK();
    }

    StreamLoadContext* get_channel_context(const string& label, int channel_id) {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _channel_stream_map.find(label);
        if (it == std::end(_channel_stream_map)) {
            return nullptr;
        }
        auto& label_channel_map = it->second;
        auto it2 = label_channel_map.find(channel_id);
        if (it2 == std::end(label_channel_map)) {
            return nullptr;
        }
        auto stream = it2->second;
        stream->ref();
        return stream;
    }

    void remove_channel_context(StreamLoadContext* ctx) {
        const string& label = ctx->label;
        int channel_id = ctx->channel_id;
        std::lock_guard<std::mutex> l(_lock);
        auto it = _channel_stream_map.find(label);
        if (it != std::end(_channel_stream_map)) {
            auto& label_channel_map = it->second;
            auto it2 = label_channel_map.find(channel_id);
            if (it2 != std::end(label_channel_map)) {
                if (it2->second->unref()) {
                    delete it2->second;
                }
                label_channel_map.erase(it2);
                VLOG(3) << "remove channel stream load context: " << label
                        << ", channel_id: " << std::to_string(channel_id);
            }
            if (label_channel_map.size() == 0) {
                _channel_stream_map.erase(it);
            }
        }
    };

    Status finish_body_sink(const string& label, int channel_id) {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _channel_stream_map.find(label);
        if (it != std::end(_channel_stream_map)) {
            auto& label_channel_map = it->second;
            auto it2 = label_channel_map.find(channel_id);
            if (it2 != std::end(label_channel_map)) {
                if (it2->second->body_sink != nullptr) {
                    StreamLoadContext* ctx = it2->second;
                    // 1. finish stream pipe & wait it done
                    if (ctx->buffer != nullptr && ctx->buffer->pos > 0) {
                        ctx->buffer->flip();
                        ctx->body_sink->append(std::move(ctx->buffer));
                        ctx->buffer = nullptr;
                    }
                    ctx->body_sink->finish();
                } else {
                    std::string error_msg = fmt::format("stream load {} channel_id {}'s pipe doesn't exist", label,
                                                        std::to_string(channel_id));
                    LOG(WARNING) << error_msg;
                    return Status::InternalError(error_msg);
                }
                LOG(INFO) << "stream load: " << label << ", channel_id: " << std::to_string(channel_id)
                          << " finish pipe";
            }
        }
        return Status::OK();
    }

private:
    std::mutex _lock;
    std::unordered_map<std::string, StreamLoadContext*> _stream_map;
    std::unordered_map<std::string, std::unordered_map<int, StreamLoadContext*>> _channel_stream_map;
};

class TransactionMgr {
public:
    explicit TransactionMgr(ExecEnv* exec_env);
    ~TransactionMgr();

    Status begin_transaction(const HttpRequest* req, std::string* resp);
    Status commit_transaction(const HttpRequest* req, std::string* resp);
    Status rollback_transaction(const HttpRequest* req, std::string* resp);
    Status list_transactions(const HttpRequest* req, std::string* resp);

    Status _begin_transaction(const HttpRequest* http_req, StreamLoadContext* ctx);
    Status _commit_transaction(StreamLoadContext* ctx, bool prepare);
    Status _rollback_transaction(StreamLoadContext* ctx);

    std::string _build_reply(const std::string& txn_op, StreamLoadContext* ctx);
    std::string _build_reply(const std::string& label, const std::string& txn_op, const Status& st);

private:
    void _clean_stream_context();

    ExecEnv* _exec_env;
    std::thread _transaction_clean_thread;
    std::atomic<bool> _is_stopped = false;
};

} // namespace starrocks

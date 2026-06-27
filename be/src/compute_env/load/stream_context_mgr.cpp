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

#include "compute_env/load/stream_context_mgr.h"

#include <iterator>
#include <memory>
#include <utility>

#include "base/time/time.h"
#include "common/logging.h"
#include "compute_env/load/load_stream_mgr.h"
#include "compute_env/load/stream_load_context.h"
#include "compute_env/load/stream_load_pipe.h"
#include "fmt/format.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/message_body_sink.h"

namespace starrocks {

namespace {

void release_context_ref(StreamLoadContext* ctx) {
    if (ctx != nullptr && ctx->unref()) {
        delete ctx;
    }
}

} // namespace

StreamContextMgr::StreamContextMgr(LoadStreamMgr* load_stream_mgr) : _load_stream_mgr(load_stream_mgr) {}

StreamContextMgr::~StreamContextMgr() {
    std::lock_guard<std::mutex> l(_lock);
    for (auto& iter : _stream_map) {
        release_context_ref(iter.second);
    }
    _stream_map.clear();

    // ExecEnv teardown may be the last owner of channel stream-load contexts.
    // Releasing them here lets StreamLoadContext run rollback callbacks while
    // DataWorkflowsEnv still owns StreamLoadExecutor.
    for (auto& label_entry : _channel_stream_map) {
        for (auto& table_entry : label_entry.second) {
            for (auto& channel_entry : table_entry.second) {
                release_context_ref(channel_entry.second);
            }
        }
    }
    _channel_stream_map.clear();
}

Status StreamContextMgr::put(const std::string& id, StreamLoadContext* stream) {
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

StreamLoadContext* StreamContextMgr::get(const std::string& id) {
    std::lock_guard<std::mutex> l(_lock);
    auto it = _stream_map.find(id);
    if (it == std::end(_stream_map)) {
        return nullptr;
    }
    auto stream = it->second;
    stream->ref();
    return stream;
}

void StreamContextMgr::remove(const std::string& id) {
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

std::vector<std::string> StreamContextMgr::get_ids() {
    std::vector<std::string> ids;
    {
        std::lock_guard<std::mutex> l(_lock);
        for (auto& iter : _stream_map) {
            ids.emplace_back(iter.first);
        }
    }

    return ids;
}

Status StreamContextMgr::create_channel_context(ExecEnv* exec_env, const std::string& label, int channel_id,
                                                const std::string& db_name, const std::string& table_name,
                                                int32_t format, StreamLoadContext*& ctx, const TUniqueId& load_id,
                                                long txn_id) {
    if (_load_stream_mgr == nullptr) {
        return Status::InternalError("load stream manager is not initialized");
    }
    auto pipe = std::make_shared<StreamLoadPipe>(true);
    RETURN_IF_ERROR(_load_stream_mgr->put(load_id, pipe));
    ctx = new StreamLoadContext(exec_env, load_id, _load_stream_mgr);
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
    ctx->format = static_cast<TFileFormatType::type>(format);

    ctx->body_sink = pipe;
    ctx->txn_id = txn_id;

    return Status::OK();
}

Status StreamContextMgr::put_channel_context(const std::string& label, const std::string& table_name, int channel_id,
                                             StreamLoadContext* ctx) {
    std::lock_guard<std::mutex> l(_lock);
    auto it = _channel_stream_map.find(label);
    if (it == std::end(_channel_stream_map)) {
        std::unordered_map<std::string, std::unordered_map<int, StreamLoadContext*>> empty_map;
        it = _channel_stream_map.emplace(label, std::move(empty_map)).first;
    }
    auto& label_map = it->second;
    auto it_table = label_map.find(table_name);
    if (it_table == std::end(label_map)) {
        std::unordered_map<int, StreamLoadContext*> empty_channel_map;
        it_table = label_map.emplace(table_name, std::move(empty_channel_map)).first;
    }
    auto& channel_map = it_table->second;
    auto it2 = channel_map.find(channel_id);
    if (it2 != std::end(channel_map)) {
        return Status::InternalError("channel id " + std::to_string(channel_id) + " for label " + label +
                                     " already exist");
    }
    ctx->ref();
    channel_map.emplace(channel_id, ctx);
    LOG(INFO) << "stream load: " << label << ", channel_id: " << std::to_string(channel_id) << " start pipe";
    return Status::OK();
}

StreamLoadContext* StreamContextMgr::get_channel_context(const std::string& label, const std::string& table_name,
                                                         int channel_id) {
    std::lock_guard<std::mutex> l(_lock);
    auto it = _channel_stream_map.find(label);
    if (it == std::end(_channel_stream_map)) {
        return nullptr;
    }
    auto& label_map = it->second;
    auto it_table = label_map.find(table_name);
    if (it_table == std::end(label_map)) {
        return nullptr;
    }
    auto& channel_map = it_table->second;
    auto it2 = channel_map.find(channel_id);
    if (it2 == std::end(channel_map)) {
        return nullptr;
    }
    auto stream = it2->second;
    stream->ref();
    return stream;
}

void StreamContextMgr::remove_channel_context(StreamLoadContext* ctx) {
    const std::string& label = ctx->label;
    const std::string& table_name = ctx->table;
    int channel_id = ctx->channel_id;
    std::lock_guard<std::mutex> l(_lock);
    auto it = _channel_stream_map.find(label);
    if (it != std::end(_channel_stream_map)) {
        auto& label_map = it->second;
        auto it_table = label_map.find(table_name);
        if (it_table != std::end(label_map)) {
            auto& channel_map = it_table->second;
            auto it2 = channel_map.find(channel_id);
            if (it2 != std::end(channel_map)) {
                if (it2->second->unref()) {
                    delete it2->second;
                }
                channel_map.erase(it2);
                VLOG(3) << "remove channel stream load context: " << label << ", table: " << table_name
                        << ", channel_id: " << std::to_string(channel_id);
            }
            if (channel_map.size() == 0) {
                label_map.erase(it_table);
            }
        }
        if (label_map.size() == 0) {
            _channel_stream_map.erase(it);
        }
    }
}

Status StreamContextMgr::finish_body_sink(const std::string& label, const std::string& table_name, int channel_id) {
    std::lock_guard<std::mutex> l(_lock);
    auto it = _channel_stream_map.find(label);
    if (it != std::end(_channel_stream_map)) {
        auto& label_map = it->second;
        auto it_table = label_map.find(table_name);
        if (it_table != std::end(label_map)) {
            auto& channel_map = it_table->second;
            auto it2 = channel_map.find(channel_id);
            if (it2 != std::end(channel_map)) {
                if (it2->second->body_sink != nullptr) {
                    StreamLoadContext* ctx = it2->second;
                    // 1. finish stream pipe & wait it done
                    if (ctx->buffer != nullptr && ctx->buffer->pos > 0) {
                        ctx->buffer->flip_to_read();
                        RETURN_IF_ERROR(ctx->body_sink->append(std::move(ctx->buffer)));
                        ctx->buffer = nullptr;
                    }
                    RETURN_IF_ERROR(ctx->body_sink->finish());
                } else {
                    std::string error_msg = fmt::format("stream load {} table {} channel_id {}'s pipe doesn't exist",
                                                        label, table_name, std::to_string(channel_id));
                    LOG(WARNING) << error_msg;
                    return Status::InternalError(error_msg);
                }
                LOG(INFO) << "stream load: " << label << ", table: " << table_name
                          << ", channel_id: " << std::to_string(channel_id) << " finish pipe";
            }
        }
    }
    return Status::OK();
}

} // namespace starrocks

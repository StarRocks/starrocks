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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/load_channel.cpp

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

#include "runtime/load_channel.h"

#include <memory>

#include "common/closure_guard.h"
#include "fmt/format.h"
#include "runtime/lake_tablets_channel.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/local_tablets_channel.h"
#include "runtime/mem_tracker.h"
#include "util/compression/block_compression.h"
#include "util/faststring.h"
#include "util/lru_cache.h"

#define RETURN_RESPONSE_IF_ERROR(stmt, response)                                      \
    do {                                                                              \
        auto&& status__ = (stmt);                                                     \
        if (UNLIKELY(!status__.ok())) {                                               \
            response->mutable_status()->set_status_code(TStatusCode::INTERNAL_ERROR); \
            response->mutable_status()->add_error_msgs(status__.to_string());         \
            return;                                                                   \
        }                                                                             \
    } while (false)

namespace starrocks {

LoadChannel::LoadChannel(LoadChannelMgr* mgr, LakeTabletManager* lake_tablet_mgr, const UniqueId& load_id,
                         const std::string& txn_trace_parent, int64_t timeout_s,
                         std::unique_ptr<MemTracker> mem_tracker)
        : _load_mgr(mgr),
          _lake_tablet_mgr(lake_tablet_mgr),
          _load_id(load_id),
          _timeout_s(timeout_s),
          _has_chunk_meta(false),
          _mem_tracker(std::move(mem_tracker)),
          _last_updated_time(time(nullptr)) {
    _span = Tracer::Instance().start_trace_or_add_span("load_channel", txn_trace_parent);
    _span->SetAttribute("load_id", load_id.to_string());
}

LoadChannel::~LoadChannel() {
    _span->SetAttribute("num_chunk", _num_chunk);
    _span->End();
}

void LoadChannel::open(brpc::Controller* cntl, const PTabletWriterOpenRequest& request,
                       PTabletWriterOpenResult* response, google::protobuf::Closure* done) {
    _span->AddEvent("open_index", {{"index_id", request.index_id()}});
    auto scoped = trace::Scope(_span);
    ClosureGuard done_guard(done);
    auto t0 = std::chrono::steady_clock::now();

    _last_updated_time.store(time(nullptr), std::memory_order_relaxed);
    int64_t index_id = request.index_id();
    bool is_lake_tablet = request.has_is_lake_tablet() && request.is_lake_tablet();

    Status st = Status::OK();
    {
        // We will `bthread::execution_queue_join()` in the destructor of AsyncDeltaWriter,
        // it will block the bthread, so we put its destructor outside the lock.
        std::shared_ptr<TabletsChannel> channel;
        std::lock_guard l(_lock);
        if (_schema == nullptr) {
            _schema.reset(new OlapTableSchemaParam());
            RETURN_RESPONSE_IF_ERROR(_schema->init(request.schema()), response);
        }
        if (_row_desc == nullptr) {
            _row_desc = std::make_unique<RowDescriptor>(_schema->tuple_desc(), false);
        }
        auto it = _tablets_channels.find(index_id);
        if (it == _tablets_channels.end()) {
            TabletsChannelKey key(request.id(), index_id);
            if (is_lake_tablet) {
                channel = new_lake_tablets_channel(this, _lake_tablet_mgr, key, _mem_tracker.get());
            } else {
                channel = new_local_tablets_channel(this, key, _mem_tracker.get());
            }
            if (st = channel->open(request, _schema, request.is_incremental()); st.ok()) {
                _tablets_channels.insert({index_id, std::move(channel)});
            }
        } else if (request.is_incremental()) {
            auto local_tablets_channel = dynamic_cast<LocalTabletsChannel*>(it->second.get());
            if (local_tablets_channel) {
                size_t i = 0;
                while (local_tablets_channel->num_ref_senders() != 0) {
                    bthread_usleep(10000); // 10ms
                    auto t1 = std::chrono::steady_clock::now();
                    if (std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count() / 1000 >
                        request.timeout_ms()) {
                        std::stringstream ss;
                        ss << "LoadChannel txn_id: " << request.txn_id() << " load_id: " << print_id(request.id())
                           << " wait other sender finish write " << request.timeout_ms() << "ms timeout still has "
                           << local_tablets_channel->num_ref_senders() << " sender";
                        LOG(INFO) << ss.str();
                        st = Status::InternalError(ss.str());
                        break;
                    }

                    if (++i % 3000 == 0) {
                        LOG(INFO) << "LoadChannel txn_id: " << request.txn_id()
                                  << " load_id: " << print_id(request.id())
                                  << " wait other sender finish write already "
                                  << std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count() / 1000
                                  << "ms still has " << local_tablets_channel->num_ref_senders() << " sender";
                    }
                }
                if (st.ok()) {
                    st = local_tablets_channel->incremental_open(request, _schema);
                }
            } else {
                st = Status::NotSupported("incremental open not supported by this tablets channel");
            }
        }
    }
    LOG_IF(WARNING, !st.ok()) << "Fail to open index " << index_id << " of load " << _load_id << ": " << st.to_string();
    response->mutable_status()->set_status_code(st.code());
    response->mutable_status()->add_error_msgs(st.get_error_msg());

    if (config::enable_load_colocate_mv) {
        response->set_is_repeated_chunk(true);
    }
}

void LoadChannel::_add_chunk(Chunk* chunk, const PTabletWriterAddChunkRequest& request,
                             PTabletWriterAddBatchResult* response) {
    _num_chunk++;
    _last_updated_time.store(time(nullptr), std::memory_order_relaxed);
    auto channel = get_tablets_channel(request.index_id());
    if (channel == nullptr) {
        response->mutable_status()->set_status_code(TStatusCode::INTERNAL_ERROR);
        response->mutable_status()->add_error_msgs("cannot find the tablets channel associated with the index id");
        return;
    }
    channel->add_chunk(chunk, request, response);
}

void LoadChannel::add_chunk(const PTabletWriterAddChunkRequest& request, PTabletWriterAddBatchResult* response) {
    faststring uncompressed_buffer;
    Chunk chunk;
    if (request.has_chunk()) {
        auto& pchunk = request.chunk();
        RETURN_RESPONSE_IF_ERROR(_build_chunk_meta(pchunk), response);
        RETURN_RESPONSE_IF_ERROR(_deserialize_chunk(pchunk, chunk, &uncompressed_buffer), response);
        _add_chunk(&chunk, request, response);
    } else {
        _add_chunk(nullptr, request, response);
    }
}

void LoadChannel::add_chunks(const PTabletWriterAddChunksRequest& req, PTabletWriterAddBatchResult* response) {
    // only support repeated chunks
    if (!req.is_repeated_chunk() || !config::enable_load_colocate_mv) {
        response->mutable_status()->set_status_code(TStatusCode::INTERNAL_ERROR);
        response->mutable_status()->add_error_msgs("server not support repeated chunk");
        return;
    }
    faststring uncompressed_buffer;
    std::unique_ptr<Chunk> chunk;
    for (int i = 0; i < req.requests_size(); i++) {
        auto& request = req.requests(i);
        VLOG_RPC << "tablet writer add chunk, id=" << print_id(request.id()) << ", index_id=" << request.index_id()
                 << ", sender_id=" << request.sender_id() << " request_index=" << i << " eos=" << request.eos();

        if (i == 0 && request.has_chunk()) {
            chunk = std::make_unique<Chunk>();
            auto& pchunk = request.chunk();
            RETURN_RESPONSE_IF_ERROR(_build_chunk_meta(pchunk), response);
            RETURN_RESPONSE_IF_ERROR(_deserialize_chunk(pchunk, *chunk, &uncompressed_buffer), response);
        }
        _add_chunk(chunk.get(), request, response);

        if (response->status().status_code() != TStatusCode::OK) {
            LOG(WARNING) << "tablet writer add chunk, id=" << print_id(request.id())
                         << ", index_id=" << request.index_id() << ", sender_id=" << request.sender_id()
                         << " request_index=" << i << " eos=" << request.eos()
                         << " err=" << response->status().error_msgs(0);
            return;
        }
    }
}

void LoadChannel::add_segment(brpc::Controller* cntl, const PTabletWriterAddSegmentRequest* request,
                              PTabletWriterAddSegmentResult* response, google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    _num_segment++;
    auto channel = get_tablets_channel(request->index_id());
    if (channel == nullptr) {
        response->mutable_status()->set_status_code(TStatusCode::INTERNAL_ERROR);
        response->mutable_status()->add_error_msgs("cannot find the tablets channel associated with the index id");
        return;
    }
    auto local_tablets_channel = dynamic_cast<LocalTabletsChannel*>(channel.get());
    if (local_tablets_channel == nullptr) {
        response->mutable_status()->set_status_code(TStatusCode::INTERNAL_ERROR);
        response->mutable_status()->add_error_msgs("channel is not local tablets channel.");
        return;
    }

    local_tablets_channel->add_segment(cntl, request, response, done);
    closure_guard.release();
}

void LoadChannel::cancel() {
    std::lock_guard l(_lock);
    for (auto& it : _tablets_channels) {
        it.second->cancel();
    }
}

void LoadChannel::abort() {
    _span->AddEvent("cancel");
    auto scoped = trace::Scope(_span);
    std::lock_guard l(_lock);
    for (auto& it : _tablets_channels) {
        it.second->abort();
    }
}

void LoadChannel::abort(int64_t index_id, const std::vector<int64_t>& tablet_ids, const std::string& reason) {
    auto channel = get_tablets_channel(index_id);
    if (channel != nullptr) {
        auto local_tablets_channel = dynamic_cast<LocalTabletsChannel*>(channel.get());
        if (local_tablets_channel != nullptr) {
            local_tablets_channel->incr_num_ref_senders();
            local_tablets_channel->abort(tablet_ids, reason);
        } else {
            channel->abort();
        }
    }
}

void LoadChannel::remove_tablets_channel(int64_t index_id) {
    std::unique_lock l(_lock);
    _tablets_channels.erase(index_id);
    if (_tablets_channels.empty()) {
        l.unlock();
        _load_mgr->remove_load_channel(_load_id);
        // Do NOT touch |this| since here, it could have been deleted.
    }
}

std::shared_ptr<TabletsChannel> LoadChannel::get_tablets_channel(int64_t index_id) {
    std::lock_guard l(_lock);
    auto it = _tablets_channels.find(index_id);
    if (it != _tablets_channels.end()) {
        auto local_tablets_channel = dynamic_cast<LocalTabletsChannel*>(it->second.get());
        if (local_tablets_channel) {
            local_tablets_channel->incr_num_ref_senders();
        } else {
            // nothing to do
        }
        return it->second;
    } else {
        return nullptr;
    }
}

Status LoadChannel::_build_chunk_meta(const ChunkPB& pb_chunk) {
    if (_has_chunk_meta.load(std::memory_order_acquire)) {
        return Status::OK();
    }
    std::lock_guard l(_chunk_meta_lock);
    if (_has_chunk_meta.load(std::memory_order_acquire)) {
        return Status::OK();
    }
    if (_row_desc == nullptr) {
        return Status::InternalError(fmt::format("load channel not open yet, load id: {}", _load_id.to_string()));
    }
    StatusOr<serde::ProtobufChunkMeta> res = serde::build_protobuf_chunk_meta(*_row_desc, pb_chunk);
    if (!res.ok()) return res.status();
    _chunk_meta = std::move(res).value();
    _has_chunk_meta.store(true, std::memory_order_release);
    return Status::OK();
}

Status LoadChannel::_deserialize_chunk(const ChunkPB& pchunk, Chunk& chunk, faststring* uncompressed_buffer) {
    if (pchunk.compress_type() == CompressionTypePB::NO_COMPRESSION) {
        TRY_CATCH_BAD_ALLOC({
            serde::ProtobufChunkDeserializer des(_chunk_meta);
            StatusOr<Chunk> res = des.deserialize(pchunk.data());
            if (!res.ok()) return res.status();
            chunk = std::move(res).value();
        });
    } else {
        size_t uncompressed_size = 0;
        {
            const BlockCompressionCodec* codec = nullptr;
            RETURN_IF_ERROR(get_block_compression_codec(pchunk.compress_type(), &codec));
            uncompressed_size = pchunk.uncompressed_size();
            TRY_CATCH_BAD_ALLOC(uncompressed_buffer->resize(uncompressed_size));
            Slice output{uncompressed_buffer->data(), uncompressed_size};
            RETURN_IF_ERROR(codec->decompress(pchunk.data(), &output));
        }
        {
            TRY_CATCH_BAD_ALLOC({
                std::string_view buff(reinterpret_cast<const char*>(uncompressed_buffer->data()), uncompressed_size);
                serde::ProtobufChunkDeserializer des(_chunk_meta);
                StatusOr<Chunk> res = Status::OK();
                TRY_CATCH_BAD_ALLOC(res = des.deserialize(buff));
                if (!res.ok()) return res.status();
                chunk = std::move(res).value();
            });
        }
    }
    return Status::OK();
}
} // namespace starrocks

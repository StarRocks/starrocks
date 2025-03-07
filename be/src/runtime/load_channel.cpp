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
#include "util/runtime_profile.h"
#include "util/starrocks_metrics.h"
#include "util/thrift_util.h"

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
                         int64_t txn_id, const std::string& txn_trace_parent, int64_t timeout_s,
                         std::unique_ptr<MemTracker> mem_tracker)
        : _load_mgr(mgr),
          _lake_tablet_mgr(lake_tablet_mgr),
          _load_id(load_id),
          _txn_id(txn_id),
          _timeout_s(timeout_s),
          _has_chunk_meta(false),
          _mem_tracker(std::move(mem_tracker)),
          _last_updated_time(time(nullptr)) {
    _span = Tracer::Instance().start_trace_or_add_span("load_channel", txn_trace_parent);
    _span->SetAttribute("load_id", load_id.to_string());
    _create_time_ns = MonotonicNanos();

    _root_profile = std::make_shared<RuntimeProfile>("LoadChannel");
    _root_profile->add_info_string("LoadId", print_id(load_id));
    _root_profile->add_info_string("TxnId", std::to_string(txn_id));
    _profile = _root_profile->create_child(fmt::format("Channel (host={})", BackendOptions::get_localhost()), true);
    _index_num = ADD_COUNTER(_profile, "IndexNum", TUnit::UNIT);
    ADD_COUNTER(_profile, "LoadMemoryLimit", TUnit::BYTES)->set(_mem_tracker->limit());
    _peak_memory_usage = ADD_PEAK_COUNTER(_profile, "PeakMemoryUsage", TUnit::BYTES);
    _deserialize_chunk_count = ADD_COUNTER(_profile, "DeserializeChunkCount", TUnit::UNIT);
    _deserialize_chunk_timer = ADD_TIMER(_profile, "DeserializeChunkTime");
    _profile_report_count = ADD_COUNTER(_profile, "ProfileReportCount", TUnit::UNIT);
    _profile_report_timer = ADD_TIMER(_profile, "ProfileReportTime");
    _profile_serialized_size = ADD_COUNTER(_profile, "ProfileSerializedSize", TUnit::BYTES);
}

LoadChannel::~LoadChannel() {
    _span->SetAttribute("num_chunk", _num_chunk);
    _span->End();
}

void LoadChannel::set_profile_config(const PLoadChannelProfileConfig& config) {
    if (config.has_enable_profile()) {
        _enable_profile = config.enable_profile();
    }

    if (config.has_big_query_profile_threshold_ns()) {
        _big_query_profile_threshold_ns = config.big_query_profile_threshold_ns();
    }

    if (config.has_runtime_profile_report_interval_ns()) {
        _runtime_profile_report_interval_ns = config.runtime_profile_report_interval_ns();
    }
}

void LoadChannel::open(brpc::Controller* cntl, const PTabletWriterOpenRequest& request,
                       PTabletWriterOpenResult* response, google::protobuf::Closure* done) {
    _span->AddEvent("open_index", {{"index_id", request.index_id()}});
    auto scoped = trace::Scope(_span);
    ClosureGuard done_guard(done);

    _last_updated_time.store(time(nullptr), std::memory_order_relaxed);
    bool is_lake_tablet = request.has_is_lake_tablet() && request.is_lake_tablet();

    Status st = Status::OK();
    TabletsChannelKey key(request.id(), request.sink_id(), request.index_id());
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
            _row_desc = std::make_unique<RowDescriptor>(_schema->tuple_desc());
        }
        auto it = _tablets_channels.find(key);
        if (it == _tablets_channels.end()) {
            if (is_lake_tablet) {
                channel = new_lake_tablets_channel(this, _lake_tablet_mgr, key, _mem_tracker.get(), _profile);
            } else {
                channel = new_local_tablets_channel(this, key, _mem_tracker.get(), _profile);
            }
            if (st = channel->open(request, response, _schema, request.is_incremental()); st.ok()) {
                _tablets_channels.insert({key, std::move(channel)});
            }
            COUNTER_UPDATE(_index_num, 1);
        } else if (request.is_incremental()) {
            st = it->second->incremental_open(request, response, _schema);
        }
    }
    LOG_IF(WARNING, !st.ok()) << "Fail to open index " << key << " of load " << _load_id << ": " << st.to_string();
    response->mutable_status()->set_status_code(st.code());
    response->mutable_status()->add_error_msgs(std::string(st.message()));

    if (config::enable_load_colocate_mv) {
        response->set_is_repeated_chunk(true);
    }
}

void LoadChannel::_add_chunk(Chunk* chunk, const MonotonicStopWatch* watch, const PTabletWriterAddChunkRequest& request,
                             PTabletWriterAddBatchResult* response) {
    _num_chunk++;
    _last_updated_time.store(time(nullptr), std::memory_order_relaxed);
    TabletsChannelKey key(request.id(), request.sink_id(), request.index_id());
    auto channel = get_tablets_channel(key);
    if (channel == nullptr) {
        LOG(WARNING) << "cannot find the tablets channel associated with the key " << key.to_string();
        response->mutable_status()->set_status_code(TStatusCode::INTERNAL_ERROR);
        response->mutable_status()->add_error_msgs(
                fmt::format("cannot find the tablets channel associated with the key {}", key.to_string()));
        return;
    }
    bool close_channel;
    channel->add_chunk(chunk, request, response, &close_channel);
    if (close_channel &&
        (_should_enable_profile() || (watch != nullptr && watch->elapsed_time() > request.timeout_ms() * 1000000))) {
        // If close_channel is true, the channel has been removed from _tablets_channels
        // in TabletsChannel::add_chunk, so there will be no chance to get the channel to
        // update the profile later. So update the profile here
        channel->update_profile();
    }
}

void LoadChannel::add_chunk(const PTabletWriterAddChunkRequest& request, PTabletWriterAddBatchResult* response) {
    faststring uncompressed_buffer;
    Chunk chunk;
    if (request.has_chunk()) {
        auto& pchunk = request.chunk();
        RETURN_RESPONSE_IF_ERROR(_build_chunk_meta(pchunk), response);
        RETURN_RESPONSE_IF_ERROR(_deserialize_chunk(pchunk, chunk, &uncompressed_buffer), response);
        _add_chunk(&chunk, nullptr, request, response);
    } else {
        _add_chunk(nullptr, nullptr, request, response);
    }
    report_profile(response, config::pipeline_print_profile);
}

void LoadChannel::add_chunks(const PTabletWriterAddChunksRequest& req, PTabletWriterAddBatchResult* response) {
    // only support repeated chunks
    if (!req.is_repeated_chunk() || !config::enable_load_colocate_mv) {
        response->mutable_status()->set_status_code(TStatusCode::INTERNAL_ERROR);
        response->mutable_status()->add_error_msgs("server not support repeated chunk");
        return;
    }
    MonotonicStopWatch watch;
    watch.start();
    faststring uncompressed_buffer;
    std::unique_ptr<Chunk> chunk;
    int eos_count = 0;
    int64_t timeout_ms = -1;
    for (int i = 0; i < req.requests_size(); i++) {
        auto& request = req.requests(i);
        VLOG_RPC << "tablet writer add chunk, id=" << print_id(request.id()) << ", index_id=" << request.index_id()
                 << ", sender_id=" << request.sender_id() << " request_index=" << i << " eos=" << request.eos();

        if (request.eos()) {
            eos_count = 1;
        }

        if (timeout_ms == -1) {
            timeout_ms = request.timeout_ms();
        }

        if (i == 0 && request.has_chunk()) {
            chunk = std::make_unique<Chunk>();
            auto& pchunk = request.chunk();
            RETURN_RESPONSE_IF_ERROR(_build_chunk_meta(pchunk), response);
            RETURN_RESPONSE_IF_ERROR(_deserialize_chunk(pchunk, *chunk, &uncompressed_buffer), response);
        }
        _add_chunk(chunk.get(), &watch, request, response);

        if (response->status().status_code() != TStatusCode::OK) {
            LOG(WARNING) << "tablet writer add chunk, id=" << print_id(request.id())
                         << ", index_id=" << request.index_id() << ", sender_id=" << request.sender_id()
                         << " request_index=" << i << " eos=" << request.eos()
                         << " err=" << response->status().error_msgs(0);
            break;
        }
    }

    int64_t total_time_us = watch.elapsed_time() / 1000;
    StarRocksMetrics::instance()->load_channel_add_chunks_total.increment(1);
    StarRocksMetrics::instance()->load_channel_add_chunks_eos_total.increment(eos_count);
    StarRocksMetrics::instance()->load_channel_add_chunks_duration_us.increment(total_time_us);

    report_profile(response, config::pipeline_print_profile);

    // log profile if rpc timeout
    if (total_time_us > timeout_ms * 1000) {
        // update profile
        auto channels = _get_all_channels();
        for (auto& channel : channels) {
            channel->update_profile();
        }

        std::stringstream ss;
        _root_profile->pretty_print(&ss);
        if (timeout_ms > config::load_rpc_slow_log_frequency_threshold_seconds) {
            LOG(WARNING) << "tablet writer add chunk timeout. txn_id=" << _txn_id << ", cost=" << total_time_us / 1000
                         << "ms, timeout=" << timeout_ms << "ms, profile=" << ss.str();
        } else {
            // reduce slow log print frequency if the log job is small batch and high frequency
            LOG_EVERY_N(WARNING, 10) << "tablet writer add chunk timeout. txn_id=" << _txn_id
                                     << ", cost=" << total_time_us / 1000 << "ms, timeout=" << timeout_ms
                                     << "ms, profile=" << ss.str();
        }
    }
}

void LoadChannel::add_segment(brpc::Controller* cntl, const PTabletWriterAddSegmentRequest* request,
                              PTabletWriterAddSegmentResult* response, google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    _num_segment++;
    TabletsChannelKey key(request->id(), request->sink_id(), request->index_id());
    auto channel = get_tablets_channel(key);
    if (channel == nullptr) {
        LOG(WARNING) << "cannot find the tablets channel associated with the key " << key.to_string();
        response->mutable_status()->set_status_code(TStatusCode::INTERNAL_ERROR);
        response->mutable_status()->add_error_msgs(
                fmt::format("cannot find the tablets channel associated with the key {}", key.to_string()));
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

void LoadChannel::abort(const TabletsChannelKey& key, const std::vector<int64_t>& tablet_ids,
                        const std::string& reason) {
    auto channel = get_tablets_channel(key);
    if (channel != nullptr) {
        channel->abort(tablet_ids, reason);
    }
}

void LoadChannel::remove_tablets_channel(const TabletsChannelKey& key) {
    std::unique_lock l(_lock);
    _tablets_channels.erase(key);
    if (_tablets_channels.empty()) {
        l.unlock();
        _closed.store(true);
        _load_mgr->remove_load_channel(_load_id);
        // Do NOT touch |this| since here, it could have been deleted.
    }
}

std::shared_ptr<TabletsChannel> LoadChannel::get_tablets_channel(const TabletsChannelKey& key) {
    std::lock_guard l(_lock);
    auto it = _tablets_channels.find(key);
    if (it != _tablets_channels.end()) {
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
    COUNTER_UPDATE(_deserialize_chunk_count, 1);
    SCOPED_TIMER(_deserialize_chunk_timer);
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

std::vector<std::shared_ptr<TabletsChannel>> LoadChannel::_get_all_channels() {
    std::vector<std::shared_ptr<TabletsChannel>> channels;
    std::lock_guard l(_lock);
    channels.reserve(_tablets_channels.size());
    for (auto& it : _tablets_channels) {
        channels.push_back(it.second);
    }
    return channels;
}

bool LoadChannel::_should_enable_profile() {
    if (_enable_profile) {
        return true;
    }
    if (_big_query_profile_threshold_ns <= 0) {
        return false;
    }
    int64_t query_run_duration = MonotonicNanos() - _create_time_ns;
    return query_run_duration > _big_query_profile_threshold_ns;
}

void LoadChannel::report_profile(PTabletWriterAddBatchResult* result, bool print_profile) {
    if (!_should_enable_profile()) {
        return;
    }

    bool expect = false;
    if (!_is_reporting_profile.compare_exchange_strong(expect, true)) {
        // skip concurrent report
        return;
    }
    DeferOp defer([this]() { _is_reporting_profile.store(false); });

    int64_t now = MonotonicNanos();
    bool should_report;
    if (_closed) {
        // final profile report
        bool old = false;
        should_report = _final_report.compare_exchange_strong(old, true);
    } else {
        // runtime profile report periodically
        should_report = now - _last_report_time_ns >= _runtime_profile_report_interval_ns;
    }
    if (!should_report) {
        return;
    }
    _last_report_time_ns.store(now);
    Status status = _update_and_serialize_profile(result->mutable_load_channel_profile(), print_profile);
    if (!status.ok()) {
        result->clear_load_channel_profile();
    }
}

void LoadChannel::diagnose(const PLoadDiagnoseRequest* request, PLoadDiagnoseResult* result) {
    if (request->has_profile() && request->profile()) {
        Status st = _update_and_serialize_profile(result->mutable_profile_data(), config::pipeline_print_profile);
        result->mutable_profile_status()->set_status_code(st.code());
        result->mutable_profile_status()->add_error_msgs(st.to_string());
        if (!st.ok()) {
            result->clear_profile_data();
        }
        VLOG(2) << "load channel diagnose profile, load_id: " << _load_id << ", txn_id: " << _txn_id
                << ", status: " << st;
    }
}

Status LoadChannel::_update_and_serialize_profile(std::string* result, bool print_profile) {
    COUNTER_UPDATE(_profile_report_count, 1);
    SCOPED_TIMER(_profile_report_timer);

    COUNTER_SET(_peak_memory_usage, _mem_tracker->peak_consumption());
    auto channels = _get_all_channels();
    for (auto& channel : channels) {
        channel->update_profile();
    }
    _profile->inc_version();

    if (print_profile) {
        std::stringstream ss;
        _root_profile->pretty_print(&ss);
        LOG(INFO) << ss.str();
    }

    TRuntimeProfileTree thrift_profile;
    _root_profile->to_thrift(&thrift_profile);
    uint8_t* buf = nullptr;
    uint32_t len = 0;
    ThriftSerializer ser(false, 4096);
    Status st = ser.serialize(&thrift_profile, &len, &buf);
    if (!st.ok()) {
        LOG(ERROR) << "Failed to serialize LoadChannel profile, load_id: " << _load_id << ", txn_id: " << _txn_id
                   << ", status: " << st;
        return Status::InternalError("Failed to serialize profile, error: " + st.to_string());
    }
    COUNTER_UPDATE(_profile_serialized_size, len);
    result->append((char*)buf, len);
    VLOG(2) << "report profile, load_id: " << _load_id << ", txn_id: " << _txn_id << ", size: " << len;
    return Status::OK();
}
} // namespace starrocks

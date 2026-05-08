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

#ifdef USE_STAROS

#include "tablet_warmup_manager.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <chrono>

#include "base/string/string_parser.hpp"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "common/config.h"
#include "common/system/master_info.h"
#include "common/thread/threadpool.h"
#include "gen_cpp/FrontendService.h"
#include "runtime/client_cache.h"
#include "runtime/thrift_rpc_helper.h"
#include "staros_integration/staros_status.h"
#include "staros_integration/staros_worker.h"
#include "staros_integration/staros_worker_runtime.h"
#include "storage/chunk_helper.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/versioned_tablet.h"

namespace {
bvar::Adder<int64_t> g_lake_warmup_tablet_fail_count("lake_warmup_tablet_fail_count");
bvar::Adder<int64_t> g_lake_warmup_tablet_success_count("lake_warmup_tablet_success_count");
bvar::Adder<int64_t> g_lake_warmup_tablet_processing_count("lake_warmup_tablet_processing_count");
bvar::Adder<int64_t> g_lake_warmup_read_remote_bytes;
bvar::Window<bvar::Adder<int64_t>> g_lake_warmup_read_remote_bytes_minute("lake", "warmup_read_remote_bytes_minute",
                                                                          &g_lake_warmup_read_remote_bytes, 60);
bvar::LatencyRecorder g_lake_warmup_tablet_latency("lake_warmup_tablet_latency");
} // namespace

namespace starrocks::lake {

static constexpr int64_t INVALID_PARTITION_ID = -1;

std::string TabletWarmupManager::WarmupStats::to_json_str() {
    rapidjson::Document root;
    root.SetObject();
    auto& allocator = root.GetAllocator();
    // add stats
    if (before_version_ts && pending_ts) {
        root.AddMember("wait_version_ms", rapidjson::Value(before_version_ts - pending_ts), allocator);
    }
    if (get_version_ts && before_version_ts) {
        root.AddMember("get_version_ms", rapidjson::Value(get_version_ts - before_version_ts), allocator);
    }
    if (start_ts && get_version_ts) {
        root.AddMember("wait_warmup_ms", rapidjson::Value(start_ts - get_version_ts), allocator);
    }
    if (finish_ts && start_ts) {
        root.AddMember("warmup_ms", rapidjson::Value(finish_ts - start_ts), allocator);
    }
    root.AddMember("read_remote_bytes", rapidjson::Value(io_bytes_read_remote), allocator);
    root.AddMember("read_remote_ms", rapidjson::Value(io_ms_read_remote), allocator);
    root.AddMember("write_local_ms", rapidjson::Value(io_ms_write_local), allocator);
    if (finish_ts && pending_ts) {
        root.AddMember("total_ms", rapidjson::Value(finish_ts - pending_ts), allocator);
    }

    rapidjson::StringBuffer strbuf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    return {strbuf.GetString()};
}

TabletWarmupManager::WarmupContext::~WarmupContext() {
    if (_future.wait_for(std::chrono::nanoseconds(1)) == std::future_status::timeout) {
        _promise.set_value(Status::Aborted("aborted!"));
    }
    auto st = _future.get();
    if (!st.ok()) {
        VLOG(3) << "warmup failed for tablet: " << _tablet_id << ", error:" << st
                << ", profile: " << _stats.to_json_str();
        g_lake_warmup_tablet_fail_count << 1;
    }
}

TabletWarmupManager::TabletWarmupManager(TabletManager* tablet_mgr)
        : _tablet_mgr(tablet_mgr), _partition_version_cache(_FIXED_FIFO_CACHE_SIZE, _FXIED_FIFO_CACHE_EXPIRE_MS) {
    // Nothing to do
}

TabletWarmupManager::~TabletWarmupManager() {
    if (!_stopped) {
        stop();
    }
}

void TabletWarmupManager::init() {
    if (config::tablet_warmup_max_threads <= 0) {
        LOG(WARNING) << "tablet_warmup_max_threads is invalid: " << config::tablet_warmup_max_threads
                     << ", use 4 instead";
        config::tablet_warmup_max_threads = 4;
    }
    int max_threads = config::tablet_warmup_max_threads;
    auto st = ThreadPoolBuilder("cloud_native_warmup")
                      .set_min_threads(max_threads)
                      .set_max_threads(max_threads)
                      .set_max_queue_size(INT_MAX)
                      .build(&_thread_pool);
    CHECK(st.ok()) << st;
    _stopped = false;
    _schedule_thread = std::thread([this] { this->loop_schedule(); });
}

void TabletWarmupManager::stop() {
    bool expected = false;
    if (_stopped.compare_exchange_strong(expected, true)) {
        _schedule_thread.join();
        _thread_pool->shutdown();
        {
            std::scoped_lock lock(_mutex_pending);
            _tablet_pending.clear();
        }
        {
            std::scoped_lock lock(_mutex_in_progress);
            _tablet_in_progress.clear();
        }
    }
}

Status TabletWarmupManager::update_max_threads(int max_threads) {
    if (max_threads <= 0) {
        return Status::InvalidArgument("tablet_warmup_max_threads must be greater than 0");
    }
    if (_thread_pool != nullptr) {
        return _thread_pool->update_max_threads(max_threads);
    } else {
        return Status::InternalError("Thread pool not exist");
    }
}

void TabletWarmupManager::warmup_tablet(uint64_t tablet_id) {
    staros::WarmupLevel warmup_level = staros_worker_warmup_level();
    if (warmup_level == staros::WarmupLevel::WARMUP_NOT_SET || warmup_level == staros::WarmupLevel::WARMUP_NOTHING) {
        return;
    }
    // ignore the returned shared_future
    warmup_tablet2(tablet_id);
}

std::shared_future<Status> TabletWarmupManager::warmup_tablet2(uint64_t tablet_id) {
    if (_stopped) {
        auto promise = std::promise<Status>();
        promise.set_value(Status::Aborted("warmup manager stopped!"));
        return promise.get_future().share();
    }
    std::scoped_lock lock(_mutex_pending);
    _tablet_pending.emplace_back(std::make_shared<WarmupContext>(static_cast<int64_t>(tablet_id)));
    return _tablet_pending.back()->_future;
}

void TabletWarmupManager::loop_schedule() {
    LOG(INFO) << "start tablet warmup manager schedule thread.";
    while (!_stopped.load()) {
#ifndef BE_TEST
        if (!_fe_leader_exist) {
            TNetworkAddress master_addr = get_master_address();
            if (!(master_addr.hostname.size() > 0 && master_addr.port > 0)) {
                LOG_EVERY_N(INFO, 10) << "can not get fe leader info.";
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                continue;
            } else {
                _fe_leader_exist = true;
            }
        }
#endif

        // report back to starmgr if any
        std::set<uint64_t> tmp_set_report;
        {
            std::scoped_lock lock(_mutex_batch_report);
            tmp_set_report.swap(_tablet_id_report);
        }
        if (!tmp_set_report.empty()) {
            std::vector<uint64_t> ids(tmp_set_report.begin(), tmp_set_report.end());
            auto st = _thread_pool->submit_func(
                    std::bind(&TabletWarmupManager::batch_report_tablet_replica_status, this, std::move(ids)));
            if (!st.ok()) {
                LOG(INFO) << "batch report tablet replica status submit task failed: " << st;
            }
            tmp_set_report.clear();
        }

        // get visible version if any
        std::unordered_map<int64_t, std::shared_ptr<WarmupContext>> tmp_pending_version;
        {
            std::scoped_lock lock(_mutex_pending_version);
            tmp_pending_version.swap(_tablet_pending_version);
        }
        if (!tmp_pending_version.empty()) {
            std::vector<int64_t> ids;
            for (auto& e : tmp_pending_version) {
                ids.push_back(e.first);
            }
            auto st = _thread_pool->submit_func(std::bind(&TabletWarmupManager::batch_get_partitions_meta_from_frontend,
                                                          this, std::move(tmp_pending_version)));
            if (!st.ok()) {
                for (auto& id : ids) {
                    abort_warmup(id, st);
                }
            }
        }

        bool has_pending_ids = false;
        {
            std::scoped_lock lock(_mutex_pending);
            has_pending_ids = !_tablet_pending.empty();
        }
        if (has_pending_ids) {
            // give up copying the _tablet_pending into a temp list and passes into batch_prepare_warmup() as parameter
            auto st = _thread_pool->submit_func([this] { this->batch_prepare_warmup(); });
            if (!st.ok()) {
                LOG(INFO) << "Failed to prepare warmup for tablets, error: " << st;
            }
        }
        // TODO: configurable sleep interval
        std::this_thread::sleep_for(std::chrono::milliseconds(_schedule_sleep_ms));
    }
    LOG(INFO) << "exit tablet warmup manager schedule thread.";
}

void TabletWarmupManager::batch_prepare_warmup() {
    decltype(_tablet_pending) tmp_ctxs;
    {
        std::scoped_lock lock(_mutex_pending);
        tmp_ctxs.swap(_tablet_pending);
    }
    for (auto& ctx : tmp_ctxs) {
        if (ctx->_tablet_id <= 0) {
            // the ctx is not in the _tablet_in_progress, so just abort it directly with the context interface
            ctx->abort(Status::InvalidArgument("Invalid tablet_id"));
            continue;
        }
        auto s = staros_need_warmup_tablet(ctx->_tablet_id);
        if (!s.ok()) {
            ctx->done();
            continue;
        }

        int64_t tablet_id = ctx->_tablet_id;
        bool duplicate = false;
        {
            std::scoped_lock lock(_mutex_in_progress);
            if (_tablet_in_progress.find(tablet_id) == _tablet_in_progress.end()) {
                ctx->_stats.before_version_ts = UnixMillis();
                _tablet_in_progress.emplace(tablet_id, ctx);
                g_lake_warmup_tablet_processing_count << 1;
            } else {
                duplicate = true;
            }
        }
        if (duplicate) {
            ctx->abort(Status::Aborted("duplicate tablet id found in warmup"));
            continue;
        }
        // NOTE: ctx is not available after this because of moved into _tablet_in_progress

        auto st = _thread_pool->submit_func([this, ctx]() { this->get_tablet_visible_version(ctx); });
        if (!st.ok()) {
            abort_warmup(tablet_id, std::move(st));
        }
    }
}

void TabletWarmupManager::get_tablet_visible_version(const std::shared_ptr<WarmupContext>& ctx) {
    int64_t tablet_id = ctx->_tablet_id;
    auto worker = get_staros_worker();
    if (worker == nullptr) {
        abort_warmup(tablet_id, Status::ServiceUnavailable("StarOS worker is not initialized"));
        return;
    }
    auto info_or = worker->get_shard_info(tablet_id);
    if (!info_or.ok()) {
        abort_warmup(tablet_id, to_status(info_or.status()));
        return;
    }
    int64_t partition_id = get_partition_id_from_shard_info(info_or.value());
    if (partition_id == INVALID_PARTITION_ID) {
        add_tablet_id_pending_visible_version(ctx);
        return;
    }

    // get a valid partition_id from the shardInfo
    auto partition_version_opt = _partition_version_cache.get(partition_id);
    if (partition_version_opt) {
        ctx->record_version(*partition_version_opt);
        auto st = _thread_pool->submit_func([this, ctx]() { this->do_warmup_tablet(ctx); });
        if (!st.ok()) {
            abort_warmup(tablet_id, std::move(st));
        }
    } else {
        add_tablet_id_pending_visible_version(ctx);
    }
}

void TabletWarmupManager::add_tablet_id_pending_visible_version(const std::shared_ptr<WarmupContext>& ctx) {
    std::scoped_lock lock(_mutex_pending_version);
    _tablet_pending_version.emplace(ctx->_tablet_id, ctx);
}

void TabletWarmupManager::batch_get_partitions_meta_from_frontend(
        const std::unordered_map<int64_t, std::shared_ptr<WarmupContext>>& tablet_pending_version) {
    std::vector<int64_t> tablet_ids;
    for (auto& e : tablet_pending_version) {
        tablet_ids.push_back(e.first);
    }
    TPartitionMetaRequest request;
    request.__set_tablet_ids(tablet_ids);
    TPartitionMetaResponse response;
    TNetworkAddress master_addr = get_master_address();
    auto st = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &response](ClientConnection<FrontendServiceClient>& client) {
                client->getPartitionMeta(response, request);
            });
    TEST_SYNC_POINT_CALLBACK("TabletWarmupManager::batch_get_partitions_meta.frontendrpc", &st);
    if (!st.ok()) {
        for (auto id : tablet_ids) {
            abort_warmup(id, st);
        }
        return;
    }
    for (size_t i = 0; i < response.partition_metas.size(); ++i) {
        auto& meta = response.partition_metas.at(i);
        _partition_version_cache.put(meta.partition_id, meta.visible_version);
    }
    const auto& id_meta_index = response.tablet_id_partition_meta_index;

    for (auto& e : tablet_pending_version) {
        int64_t id = e.first;
        auto iter = id_meta_index.find(id);
        if (iter == id_meta_index.end()) {
            // if meta is not found in FE, consider warmup succeed
            done_warmup(id, staros::WarmupLevel::WARMUP_NOTHING, true /* report */);
            continue;
        }
        auto version = response.partition_metas.at(iter->second).visible_version;
        auto ctx = e.second;
        ctx->record_version(version);
        auto st = _thread_pool->submit_func([this, ctx]() { this->do_warmup_tablet(ctx); });
        if (!st.ok()) {
            abort_warmup(id, std::move(st));
            continue;
        }
    }
}

void TabletWarmupManager::abort_warmup(int64_t tablet_id, Status status) {
    std::scoped_lock lock(_mutex_in_progress);
    auto iter = _tablet_in_progress.find(tablet_id);
    if (iter != _tablet_in_progress.end()) {
        iter->second->abort(std::move(status));
        _tablet_in_progress.erase(iter);
        g_lake_warmup_tablet_processing_count << -1;
    }
    // unlike `done_warmup`, fail log is printed in `WarmupContext::~WarmupContext`
}

void TabletWarmupManager::done_warmup(int64_t tablet_id, staros::WarmupLevel level, bool report) {
    std::shared_ptr<WarmupContext> ctx;
    {
        std::scoped_lock lock(_mutex_in_progress);
        auto iter = _tablet_in_progress.find(tablet_id);
        if (iter == _tablet_in_progress.end()) {
            LOG(INFO) << "Fail to find tablet " << tablet_id << "after warmup.";
            return;
        } else {
            ctx = iter->second;
            _tablet_in_progress.erase(iter);
            g_lake_warmup_tablet_processing_count << -1;
        }
    }
    // call the done() under no lock
    ctx->done();
    if (report) {
        std::scoped_lock lock(_mutex_batch_report);
        _tablet_id_report.insert(static_cast<uint64_t>(tablet_id));
        g_lake_warmup_tablet_success_count << 1;
    }
    VLOG(3) << "Successfully warmup tablet: " << tablet_id << ", level: " << level
            << ", profile: " << ctx->_stats.to_json_str();
}

void TabletWarmupManager::do_warmup_tablet(const std::shared_ptr<WarmupContext>& ctx) {
    ctx->record_start();
    int64_t tablet_id = ctx->_tablet_id;
    int64_t version = ctx->_version;
    staros::WarmupLevel warmup_level = staros_worker_warmup_level();
    if (warmup_level == staros::WarmupLevel::WARMUP_NOT_SET || warmup_level == staros::WarmupLevel::WARMUP_NOTHING) {
        done_warmup(tablet_id, staros::WarmupLevel::WARMUP_NOTHING, false /* report */);
        return;
    }

    if (version <= 1) {
        done_warmup(tablet_id, warmup_level, true /* report */);
        return;
    }

    auto start_ts = butil::gettimeofday_us();
    DeferOp defer([start_ts]() { g_lake_warmup_tablet_latency << (butil::gettimeofday_us() - start_ts); });

    // WARMUP-Level1: TABLET META
    auto tablet = _tablet_mgr->get_tablet(tablet_id, version);
    if (!tablet.ok()) {
        abort_warmup(tablet_id, std::move(tablet.status()));
        return;
    }
    if (warmup_level == staros::WarmupLevel::WARMUP_META) {
        done_warmup(tablet_id, warmup_level, true /* report */);
        return;
    }
    // check stop point
    if (_stopped.load()) {
        abort_warmup(tablet_id, Status::Aborted("warmup manager stopped!"));
        return;
    }

    auto schema = ChunkHelper::convert_schema(tablet->get_schema());
    auto reader_or = (*tablet).new_reader(schema);
    if (!reader_or.ok()) {
        abort_warmup(tablet_id, std::move(reader_or.status()));
        return;
    }
    auto reader = std::move(reader_or.value());
    auto st = reader->prepare();
    if (!st.ok()) {
        abort_warmup(tablet_id, std::move(st));
        return;
    }
    TabletReaderParams params;
    // the principle is only read file from remote and cache them to local disk cache
    // do not fill any memory cache
    params.use_page_cache = false;
    params.lake_io_opts.fill_metadata_cache = false;
    params.lake_io_opts.fill_data_cache = true;
    params.lake_io_opts.cache_file_only = config::lake_cache_select_in_physical_way;

    // WARMUP-LEVEL2: Segments & Footers
    if (warmup_level == staros::WarmupLevel::WARMUP_INDEX) {
        st.update(reader->load_all_segments(params));
        if (!st.ok()) {
            abort_warmup(tablet_id, std::move(st));
            return;
        }
        reader->close();

        // after reader is closed, statistics will be collected
        ctx->record_io_stats(reader->stats());
        g_lake_warmup_read_remote_bytes << reader->stats().compressed_bytes_read_remote;

        done_warmup(tablet_id, warmup_level, true /* report */);
        return;
    }

    st.update(reader->open(params));
    if (!st.ok()) {
        abort_warmup(tablet_id, std::move(st));
        return;
    }
    if (_stopped.load()) {
        abort_warmup(tablet_id, Status::Aborted("warmup manager stopped!"));
        reader->close();
        return;
    }

    // WARMUP-LEVEL3: Segments Data
    // iterate the reader until EOF
    auto read_chunk_ptr = ChunkHelper::new_chunk(schema, 4096);
    do {
        auto st = reader->get_next(read_chunk_ptr.get());
        read_chunk_ptr->reset();
        if (st.is_end_of_file()) {
            break;
        }
        if (!st.ok()) {
            abort_warmup(tablet_id, std::move(st));
            break;
        }
        if (_stopped.load()) {
            abort_warmup(tablet_id, Status::Aborted("warmup manager stopped!"));
            break;
        }
    } while (true);

    reader->close();

    // after reader is closed, statistics will be collected
    ctx->record_io_stats(reader->stats());
    g_lake_warmup_read_remote_bytes << reader->stats().compressed_bytes_read_remote;

    done_warmup(tablet_id, warmup_level, true /* report */);
}

void TabletWarmupManager::batch_report_tablet_replica_status(const std::vector<uint64_t>& tablet_ids) {
    auto update_st = batch_update_tablet_replica_info(tablet_ids);
    TEST_SYNC_POINT_CALLBACK("TabletWarmupManager::batch_report_status.starlet_report",
                             const_cast<std::vector<uint64_t>*>(&tablet_ids));
    if (!update_st.ok()) {
        LOG(INFO) << "batch report tablet replica status failed, err:" << update_st;
    }
}

int64_t TabletWarmupManager::get_partition_id_from_shard_info(staros::starlet::ShardInfo& info) {
    // LakeTablet.PROPERTY_KEY_PARTITION_ID
    static std::string partition_key("partitionId");
    auto iter = info.properties.find(partition_key);
    if (iter == info.properties.end()) {
        return INVALID_PARTITION_ID;
    }
    auto& str_value = iter->second;
    StringParser::ParseResult result;
    auto val = StringParser::string_to_unsigned_int<int64_t>(str_value.data(), str_value.size(), &result);
    if (result == StringParser::PARSE_SUCCESS) {
        return val;
    } else {
        return INVALID_PARTITION_ID;
    }
}

} // namespace starrocks::lake
#endif

// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "service/staros_worker.h"

#ifdef USE_STAROS

#include <starlet.h>
#include <worker.h>

#include <mutex>
#include <unordered_map>

#include "common/config.h"
#include "common/logging.h"
#include "fmt/format.h"

namespace starrocks {

std::ostream& operator<<(std::ostream& os, const staros::starlet::ShardInfo& shard) {
    return os << "Shard{.id=" << shard.id << " .uri=" << shard.obj_store_info.s3_obj_store.uri << "}";
}

absl::Status StarOSWorker::add_shard(const ShardInfo& shard) {
    LOG(INFO) << "Adding " << shard;
    std::lock_guard l(_mtx);
    _shards[shard.id] = shard;
    return absl::OkStatus();
}

absl::Status StarOSWorker::remove_shard(const ShardId id) {
    LOG(INFO) << "Removing " << id;
    std::lock_guard l(_mtx);
    _shards.erase(id);
    return absl::OkStatus();
}

StatusOr<staros::starlet::ShardInfo> StarOSWorker::get_shard_info(ShardId id) {
    std::lock_guard l(_mtx);
    auto it = _shards.find(id);
    if (it == _shards.end()) {
        return Status::NotFound(fmt::format("failed to get shardinfo {}", id));
    }
    return it->second;
}

std::vector<staros::starlet::ShardInfo> StarOSWorker::shards() {
    std::lock_guard l(_mtx);
    std::vector<staros::starlet::ShardInfo> vec;
    for (const auto& shard : _shards) {
        vec.emplace_back(shard.second);
    }
    return vec;
}

absl::StatusOr<staros::starlet::WorkerInfo> StarOSWorker::worker_info() {
    staros::starlet::WorkerInfo worker_info;
    std::lock_guard l(_mtx);
    worker_info.worker_id = _worker_id;
    worker_info.service_id = _service_id;
    worker_info.properties["port"] = std::to_string(config::starlet_port);
    for (auto&& [k, _] : _shards) {
        worker_info.shards.insert(k);
    }
    return worker_info;
}

absl::Status StarOSWorker::update_worker_info(const staros::starlet::WorkerInfo& new_worker_info) {
    std::lock_guard l(_mtx);
    _service_id = new_worker_info.service_id;
    _worker_id = new_worker_info.worker_id;
    return absl::OkStatus();
}

std::shared_ptr<StarOSWorker> g_worker;
staros::starlet::Starlet* g_starlet;

void init_staros_worker() {
    staros::starlet::StarletConfig starlet_config;
    starlet_config.rpc_port = config::starlet_port;
    if (g_starlet != nullptr) return;
    g_worker = std::make_shared<StarOSWorker>();
    g_starlet = new staros::starlet::Starlet(g_worker);
    g_starlet->init(starlet_config);
    g_starlet->set_star_mgr_addr(config::starmgr_addr);
    g_starlet->start();
}

void shutdown_staros_worker() {
    g_starlet->stop();
    delete g_starlet;
    g_starlet = nullptr;
    g_worker = nullptr;
}

} // namespace starrocks
#endif // USE_STAROS

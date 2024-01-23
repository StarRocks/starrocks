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
#include "service/staros_worker.h"

#include <fslib/fslib_all_initializer.h>
#include <starlet.h>
#include <worker.h>

#include "absl/strings/str_format.h"
#include "common/config.h"
#include "common/gflags_utils.h"
#include "common/logging.h"
#include "common/shutdown_hook.h"
#include "file_store.pb.h"
#include "fmt/format.h"
#include "fslib/star_cache_configuration.h"
#include "fslib/star_cache_handler.h"
#include "gflags/gflags.h"
#include "gutil/strings/fastmem.h"
#include "util/await.h"
#include "util/debug_util.h"
#include "util/lru_cache.h"
#include "util/sha.h"

// cachemgr thread pool size
DECLARE_int32(cachemgr_threadpool_size);
// cache backend check interval (in seconds), for async write sync check and ttl clean, e.t.c.
DECLARE_int32(cachemgr_check_interval);
// cache backend cache evictor interval (in seconds)
DECLARE_int32(cachemgr_evict_interval);
// cache will start evict cache files if free space belows this value(percentage)
DECLARE_double(cachemgr_evict_low_water);
// cache will stop evict cache files if free space is above this value(percentage)
DECLARE_double(cachemgr_evict_high_water);
// cache will evict file cache at this percent if star cache is turned on
DECLARE_double(cachemgr_evict_percent);
// cache will evict file cache at this speed if star cache is turned on
DECLARE_int32(cachemgr_evict_throughput_mb);
// type:Integer. CacheManager cache directory allocation policy. (0:default, 1:random, 2:round-robin)
DECLARE_int32(cachemgr_dir_allocate_policy);
// buffer size in starlet fs buffer stream, size <= 0 means not use buffer stream.
DECLARE_int32(fs_stream_buffer_size_bytes);
// domain allow list to force starlet using s3 virtual address style
DECLARE_string(fslib_s3_virtual_address_domainlist);
// s3client factory cache capacity
DECLARE_int32(fslib_s3client_max_items);
// s3client max connections
DECLARE_int32(fslib_s3client_max_connections);
// s3client max instances per cache item, allow using multiple client instances per cache
DECLARE_int32(fslib_s3client_max_instance_per_item);
// threadpool size for buffer prefetch task
DECLARE_int32(fs_buffer_prefetch_threadpool_size);
// switch to turn on/off buffer prefetch when read
DECLARE_bool(fs_enable_buffer_prefetch);

namespace starrocks {

std::shared_ptr<StarOSWorker> g_worker;
std::unique_ptr<staros::starlet::Starlet> g_starlet;

namespace fslib = staros::starlet::fslib;

StarOSWorker::StarOSWorker() : _mtx(), _shards(), _fs_cache(new_lru_cache(1024)) {}

StarOSWorker::~StarOSWorker() = default;

absl::Status StarOSWorker::add_shard(const ShardInfo& shard) {
    std::unique_lock l(_mtx);
    auto it = _shards.find(shard.id);
    if (it != _shards.end() && it->second.shard_info.path_info.has_fs_info() && shard.path_info.has_fs_info() &&
        it->second.shard_info.path_info.fs_info().version() < shard.path_info.fs_info().version()) {
        auto st = invalidate_fs(it->second.shard_info);
        if (!st.ok()) {
            return st;
        }
    }
    _shards.insert_or_assign(shard.id, ShardInfoDetails(shard));
    return absl::OkStatus();
}

absl::Status StarOSWorker::invalidate_fs(const ShardInfo& info) {
    auto scheme = build_scheme_from_shard_info(info);
    if (!scheme.ok()) {
        return scheme.status();
    }
    auto conf = build_conf_from_shard_info(info);
    if (!conf.ok()) {
        return conf.status();
    }
    std::string key_str = get_cache_key(*scheme, *conf);
    CacheKey key(key_str);
    _fs_cache->erase(key);
    return absl::OkStatus();
}

absl::Status StarOSWorker::remove_shard(const ShardId id) {
    std::unique_lock l(_mtx);
    _shards.erase(id);
    return absl::OkStatus();
}

absl::StatusOr<staros::starlet::ShardInfo> StarOSWorker::get_shard_info(ShardId id) const {
    std::shared_lock l(_mtx);
    auto it = _shards.find(id);
    if (it == _shards.end()) {
        return absl::NotFoundError(fmt::format("failed to get shardinfo {}", id));
    }
    return it->second.shard_info;
}

absl::StatusOr<staros::starlet::ShardInfo> StarOSWorker::retrieve_shard_info(ShardId id) {
    auto st = get_shard_info(id);
    if (absl::IsNotFound(st.status())) {
        return _fetch_shard_info_from_remote(id);
    }
    return st;
}

std::vector<staros::starlet::ShardInfo> StarOSWorker::shards() const {
    std::vector<staros::starlet::ShardInfo> vec;
    vec.reserve(_shards.size());

    std::shared_lock l(_mtx);
    for (const auto& shard : _shards) {
        vec.emplace_back(shard.second.shard_info);
    }
    return vec;
}

absl::StatusOr<staros::starlet::WorkerInfo> StarOSWorker::worker_info() const {
    staros::starlet::WorkerInfo worker_info;

    std::shared_lock l(_mtx);
    worker_info.worker_id = worker_id();
    worker_info.service_id = service_id();
    worker_info.properties["port"] = std::to_string(config::starlet_port);
    worker_info.properties["be_port"] = std::to_string(config::be_port);
    worker_info.properties["be_http_port"] = std::to_string(config::be_http_port);
    worker_info.properties["be_brpc_port"] = std::to_string(config::brpc_port);
    worker_info.properties["be_heartbeat_port"] = std::to_string(config::heartbeat_service_port);
    worker_info.properties["be_version"] = get_short_version();
    for (auto& iter : _shards) {
        worker_info.shards.insert(iter.first);
    }
    return worker_info;
}

absl::Status StarOSWorker::update_worker_info(const staros::starlet::WorkerInfo& new_worker_info) {
    return absl::OkStatus();
}

absl::StatusOr<std::shared_ptr<fslib::FileSystem>> StarOSWorker::get_shard_filesystem(ShardId id,
                                                                                      const Configuration& conf) {
    { // shared_lock, check if the filesystem already created
        std::shared_lock l(_mtx);
        auto it = _shards.find(id);
        if (it == _shards.end()) {
            // unlock the lock and try best to build the filesystem with remote rpc call
            l.unlock();
            return build_filesystem_on_demand(id, conf);
        }
        if (it->second.fs) {
            return it->second.fs;
        }
    }

    {
        std::unique_lock l(_mtx);
        auto shard_iter = _shards.find(id);
        // could be possibly shards removed or fs get created during unlock-lock
        if (shard_iter == _shards.end()) {
            // unlock the lock and try best to build the filesystem with remote rpc call
            l.unlock();
            return build_filesystem_on_demand(id, conf);
        }
        if (shard_iter->second.fs) {
            return shard_iter->second.fs;
        }
        auto fs_or = build_filesystem_from_shard_info(shard_iter->second.shard_info, conf);
        if (!fs_or.ok()) {
            return fs_or.status();
        }
        shard_iter->second.fs = std::move(fs_or).value();
        return shard_iter->second.fs;
    }
}

absl::StatusOr<staros::starlet::ShardInfo> StarOSWorker::_fetch_shard_info_from_remote(ShardId id) {
    static const int64_t kGetShardInfoTimeout = 5 * 1000 * 1000; // 5s (heartbeat interval)
    static const int64_t kCheckInterval = 10 * 1000;             // 10ms
    Awaitility wait;
    auto cond = []() { return g_starlet->is_ready(); };
    auto ret = wait.timeout(kGetShardInfoTimeout).interval(kCheckInterval).until(cond);
    if (!ret) {
        return absl::UnavailableError("starlet is still not ready!");
    }

    // get_shard_info call will probably trigger an add_shard() call to worker itself. Be sure there is no dead lock.
    return g_starlet->get_shard_info(id);
}

absl::StatusOr<std::shared_ptr<fslib::FileSystem>> StarOSWorker::build_filesystem_on_demand(ShardId id,
                                                                                            const Configuration& conf) {
    auto info_or = _fetch_shard_info_from_remote(id);
    if (!info_or.ok()) {
        return info_or.status();
    }
    return build_filesystem_from_shard_info(info_or.value(), conf);
}

absl::StatusOr<std::shared_ptr<fslib::FileSystem>> StarOSWorker::build_filesystem_from_shard_info(
        const ShardInfo& info, const Configuration& conf) {
    auto localconf = build_conf_from_shard_info(info);
    if (!localconf.ok()) {
        return localconf.status();
    }
    auto scheme = build_scheme_from_shard_info(info);
    if (!scheme.ok()) {
        return scheme.status();
    }

    if (need_enable_cache(info)) {
        // set environ variable to cachefs directory
        setenv(fslib::kFslibCacheDir.c_str(), config::starlet_cache_dir.c_str(), 0 /*overwrite*/);
    }
    return new_shared_filesystem(*scheme, *localconf);
}

bool StarOSWorker::need_enable_cache(const ShardInfo& info) {
    auto cache_info = info.cache_info;
    return cache_info.enable_cache() && !config::starlet_cache_dir.empty();
}

absl::StatusOr<std::string> StarOSWorker::build_scheme_from_shard_info(const ShardInfo& info) {
    if (need_enable_cache(info)) {
        return "cachefs://";
    }

    std::string scheme = "file://";
    switch (info.path_info.fs_info().fs_type()) {
    case staros::FileStoreType::S3:
        scheme = "s3://";
        break;
    case staros::FileStoreType::HDFS:
        scheme = "hdfs://";
        break;
    case staros::FileStoreType::AZBLOB:
        scheme = "azblob://";
        break;
    default:
        return absl::InvalidArgumentError("Unknown shard storage scheme!");
    }
    return scheme;
}

absl::StatusOr<fslib::Configuration> StarOSWorker::build_conf_from_shard_info(const ShardInfo& info) {
    // use the remote fsroot as the default cache identifier
    return info.fslib_conf_from_this(need_enable_cache(info), "");
}

absl::StatusOr<std::shared_ptr<StarOSWorker::FileSystem>> StarOSWorker::new_shared_filesystem(
        std::string_view scheme, const Configuration& conf) {
    std::string key_str = get_cache_key(scheme, conf);
    CacheKey key(key_str);

    // Lookup LRU cache
    std::shared_ptr<fslib::FileSystem> fs;
    auto handle = _fs_cache->lookup(key);
    if (handle != nullptr) {
        auto value = static_cast<CacheValue*>(_fs_cache->value(handle));
        fs = value->lock();
        _fs_cache->release(handle);
        if (fs != nullptr) {
            VLOG(9) << "Share filesystem";
            return std::move(fs);
        }
    }
    VLOG(9) << "Create a new filesystem";

    // Create a new instance of FileSystem
    auto fs_or = fslib::FileSystemFactory::new_filesystem(scheme, conf);
    if (!fs_or.ok()) {
        return fs_or.status();
    }
    // turn unique_ptr to shared_ptr
    fs = std::move(fs_or).value();

    // Put the FileSysatem into LRU cache
    auto value = new CacheValue(fs);
    handle = _fs_cache->insert(key, value, 1, cache_value_deleter);
    if (handle == nullptr) {
        delete value;
    } else {
        _fs_cache->release(handle);
    }

    return std::move(fs);
}

std::string StarOSWorker::get_cache_key(std::string_view scheme, const Configuration& conf) {
    // Take the SHA-256 hash value as the cache key
    SHA256Digest sha256;
    sha256.update(scheme.data(), scheme.size());
    for (const auto& [k, v] : conf) {
        sha256.update(k.data(), k.size());
        sha256.update(v.data(), v.size());
    }
    sha256.digest();
    return sha256.hex();
}

Status to_status(const absl::Status& absl_status) {
    switch (absl_status.code()) {
    case absl::StatusCode::kOk:
        return Status::OK();
    case absl::StatusCode::kAlreadyExists:
        return Status::AlreadyExist(fmt::format("starlet err {}", absl_status.message()));
    case absl::StatusCode::kOutOfRange:
        return Status::InvalidArgument(fmt::format("starlet err {}", absl_status.message()));
    case absl::StatusCode::kInvalidArgument:
        return Status::InvalidArgument(fmt::format("starlet err {}", absl_status.message()));
    case absl::StatusCode::kNotFound:
        return Status::NotFound(fmt::format("starlet err {}", absl_status.message()));
    case absl::StatusCode::kResourceExhausted:
        return Status::ResourceBusy(fmt::format("starlet err {}", absl_status.message()));
    default:
        return Status::InternalError(fmt::format("starlet err {}", absl_status.message()));
    }
}

void init_staros_worker() {
    if (g_starlet.get() != nullptr) {
        return;
    }
    // skip staros reinit aws sdk
    staros::starlet::fslib::skip_aws_init_api = true;

    staros::starlet::common::GFlagsUtils::UpdateFlagValue("cachemgr_threadpool_size",
                                                          std::to_string(config::starlet_cache_thread_num));
    staros::starlet::common::GFlagsUtils::UpdateFlagValue("cachemgr_evict_low_water",
                                                          std::to_string(config::starlet_cache_evict_low_water));
    staros::starlet::common::GFlagsUtils::UpdateFlagValue("cachemgr_evict_high_water",
                                                          std::to_string(config::starlet_cache_evict_high_water));
    staros::starlet::common::GFlagsUtils::UpdateFlagValue("fs_stream_buffer_size_bytes",
                                                          std::to_string(config::starlet_fs_stream_buffer_size_bytes));
    staros::starlet::common::GFlagsUtils::UpdateFlagValue("fs_enable_buffer_prefetch",
                                                          std::to_string(config::starlet_fs_read_prefetch_enable));
    staros::starlet::common::GFlagsUtils::UpdateFlagValue(
            "fs_buffer_prefetch_threadpool_size", std::to_string(config::starlet_fs_read_prefetch_threadpool_size));
    staros::starlet::common::GFlagsUtils::UpdateFlagValue("cachemgr_evict_interval",
                                                          std::to_string(config::starlet_cache_evict_interval));

    FLAGS_cachemgr_check_interval = config::starlet_cache_check_interval;
    FLAGS_cachemgr_dir_allocate_policy = config::starlet_cache_dir_allocate_policy;
    FLAGS_fslib_s3_virtual_address_domainlist = config::starlet_s3_virtual_address_domainlist;
    // use the same configuration as the external query
    FLAGS_fslib_s3client_max_connections = config::object_storage_max_connection;
    FLAGS_fslib_s3client_max_items = config::starlet_s3_client_max_cache_capacity;
    FLAGS_fslib_s3client_max_instance_per_item = config::starlet_s3_client_num_instances_per_cache;

    fslib::FLAGS_use_star_cache = config::starlet_use_star_cache;
    fslib::FLAGS_star_cache_mem_size_percent = config::starlet_star_cache_mem_size_percent;
    fslib::FLAGS_star_cache_disk_size_percent = config::starlet_star_cache_disk_size_percent;
    fslib::FLAGS_star_cache_disk_size_bytes = config::starlet_star_cache_disk_size_bytes;
    fslib::FLAGS_star_cache_block_size_bytes = config::starlet_star_cache_block_size_bytes;

    staros::starlet::StarletConfig starlet_config;
    starlet_config.rpc_port = config::starlet_port;
    g_worker = std::make_shared<StarOSWorker>();
    g_starlet = std::make_unique<staros::starlet::Starlet>(g_worker);
    g_starlet->init(starlet_config);
    g_starlet->start();
}

void shutdown_staros_worker() {
    g_starlet->stop();
    g_starlet.reset();
    g_worker = nullptr;

    LOG(INFO) << "Executing starlet shutdown hooks ...";
    staros::starlet::common::ShutdownHook::shutdown();
}

// must keep each config the same
void update_staros_starcache() {
    if (fslib::FLAGS_use_star_cache != config::starlet_use_star_cache) {
        fslib::FLAGS_use_star_cache = config::starlet_use_star_cache;
        (void)fslib::star_cache_init(fslib::FLAGS_use_star_cache);
    }
}

} // namespace starrocks
#endif // USE_STAROS

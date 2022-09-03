// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#ifdef USE_STAROS

#include "service/staros_worker.h"

#include <starlet.h>
#include <worker.h>

#include "absl/strings/str_format.h"
#include "common/config.h"
#include "common/logging.h"
#include "fmt/format.h"

namespace starrocks {

namespace fslib = staros::starlet::fslib;

std::ostream& operator<<(std::ostream& os, const staros::starlet::ShardInfo& shard) {
    return os << "Shard{.id=" << shard.id << " .uri=" << shard.obj_store_info.s3_obj_store.uri << "}";
}

absl::Status StarOSWorker::add_shard(const ShardInfo& shard) {
    std::unique_lock l(_mtx);
    LOG(INFO) << "Adding " << shard;
    _shards.try_emplace(shard.id, ShardInfoDetails(shard));
    return absl::OkStatus();
}

absl::Status StarOSWorker::remove_shard(const ShardId id) {
    std::unique_lock l(_mtx);
    LOG(INFO) << "Removing " << id;
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
    worker_info.worker_id = _worker_id;
    worker_info.service_id = _service_id;
    worker_info.properties["port"] = std::to_string(config::starlet_port);
    for (auto& iter : _shards) {
        worker_info.shards.insert(iter.first);
    }
    return worker_info;
}

absl::Status StarOSWorker::update_worker_info(const staros::starlet::WorkerInfo& new_worker_info) {
    std::unique_lock l(_mtx);
    _service_id = new_worker_info.service_id;
    _worker_id = new_worker_info.worker_id;
    return absl::OkStatus();
}

absl::StatusOr<std::shared_ptr<fslib::FileSystem>> StarOSWorker::get_shard_filesystem(ShardId id,
                                                                                      const Configuration& conf) {
    { // shared_lock, check if the filesystem already created
        std::shared_lock l(_mtx);
        auto it = _shards.find(id);
        if (it == _shards.end()) {
            return absl::NotFoundError(fmt::format("failed to get shardinfo {}", id));
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
            return absl::NotFoundError(fmt::format("failed to get shardinfo {}", id));
        }
        if (shard_iter->second.fs) {
            return shard_iter->second.fs;
        }
        fslib::Configuration localconf;
        ShardInfo& info = shard_iter->second.shard_info;

        // FIXME: currently the cache root dir is set from be.conf, could be changed in future
        std::string cache_dir = config::starlet_cache_dir;
        auto cache_setting = info.get_cache_setting();
        bool cache_enabled = cache_setting.enable_cache && !cache_dir.empty();

        std::string scheme = "file://";
        switch (info.obj_store_info.scheme) {
        case staros::starlet::ObjectStoreType::S3:
            scheme = "s3://";
            {
                auto& s3_info = info.obj_store_info.s3_obj_store;
                localconf[fslib::kSysRoot] = s3_info.uri;
                if (!s3_info.region.empty()) {
                    localconf[fslib::kS3Region] = s3_info.region;
                }
                if (!s3_info.endpoint.empty()) {
                    localconf[fslib::kS3OverrideEndpoint] = s3_info.endpoint;
                }
                if (!s3_info.access_key.empty()) {
                    localconf[fslib::kS3AccessKeyId] = s3_info.access_key;
                }
                if (!s3_info.access_key_secret.empty()) {
                    localconf[fslib::kS3AccessKeySecret] = s3_info.access_key_secret;
                }
            }
            break;
        default:
            return absl::InvalidArgumentError("Unknown shard storage scheme!");
        }

        if (cache_enabled) {
            const static std::string conf_prefix("cachefs.");

            scheme = "cachefs://";
            // rebuild configuration for cachefs
            Configuration tmp;
            tmp.swap(localconf);
            for (auto& iter : tmp) {
                localconf[conf_prefix + iter.first] = iter.second;
            }
            localconf[fslib::kSysRoot] = "/";
            // original fs sys.root as cachefs persistent uri
            localconf[fslib::kCacheFsPersistUri] = tmp[fslib::kSysRoot];
            // use shard id as cache identifier
            localconf[fslib::kCacheFsIdentifier] = absl::StrFormat("%d", info.id);
            localconf[fslib::kCacheFsTtlSecs] = absl::StrFormat("%ld", cache_setting.cache_entry_ttl_sec);
            if (cache_setting.allow_async_write_back) {
                localconf[fslib::kCacheFsAsyncWriteBack] = "true";
            }

            // set environ variable to cachefs directory
            setenv(fslib::kFslibCacheDir.c_str(), cache_dir.c_str(), 0 /*overwrite*/);
        }

        auto fs = fslib::FileSystemFactory::new_filesystem(scheme, localconf);
        if (!fs.ok()) {
            return fs.status();
        }
        // turn unique_ptr to shared_ptr
        shard_iter->second.fs = std::move(fs).value();
        return shard_iter->second.fs;
    }
}

Status to_status(absl::Status absl_status) {
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
    default:
        return Status::InternalError(fmt::format("starlet err {}", absl_status.message()));
    }
}

std::shared_ptr<StarOSWorker> g_worker;
std::unique_ptr<staros::starlet::Starlet> g_starlet;

void init_staros_worker() {
    if (g_starlet.get() != nullptr) {
        return;
    }
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
}

} // namespace starrocks
#endif // USE_STAROS

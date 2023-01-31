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

#include <starlet.h>
#include <worker.h>

#include "absl/strings/str_format.h"
#include "common/config.h"
#include "common/logging.h"
#include "file_store.pb.h"
#include "fmt/format.h"
#include "util/debug_util.h"

namespace starrocks {

namespace fslib = staros::starlet::fslib;

absl::Status StarOSWorker::add_shard(const ShardInfo& shard) {
    std::unique_lock l(_mtx);
    _shards.try_emplace(shard.id, ShardInfoDetails(shard));
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
            return absl::InternalError(fmt::format("failed to get shardinfo {}", id));
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
            return absl::InternalError(fmt::format("failed to get shardinfo {}", id));
        }
        if (shard_iter->second.fs) {
            return shard_iter->second.fs;
        }
        fslib::Configuration localconf;
        ShardInfo& info = shard_iter->second.shard_info;

        // FIXME: currently the cache root dir is set from be.conf, could be changed in future
        std::string cache_dir = config::starlet_cache_dir;
        auto cache_info = info.cache_info;
        bool cache_enabled = cache_info.enable_cache() && !cache_dir.empty();

        std::string scheme = "file://";
        switch (info.path_info.fs_info().fs_type()) {
        case staros::FileStoreType::S3:
            scheme = "s3://";
            {
                auto& s3_info = info.path_info.fs_info().s3_fs_info();
                if (!info.path_info.full_path().empty()) {
                    localconf[fslib::kSysRoot] = info.path_info.full_path();
                }
                if (!s3_info.bucket().empty()) {
                    localconf[fslib::kS3Bucket] = s3_info.bucket();
                }
                if (!s3_info.region().empty()) {
                    localconf[fslib::kS3Region] = s3_info.region();
                }
                if (!s3_info.endpoint().empty()) {
                    localconf[fslib::kS3OverrideEndpoint] = s3_info.endpoint();
                }
                if (s3_info.has_credential()) {
                    auto credential = s3_info.credential();
                    if (credential.has_default_credential()) {
                        localconf[fslib::kS3CredentialType] = "default";
                    } else if (credential.has_simple_credential()) {
                        localconf[fslib::kS3CredentialType] = "simple";
                        auto simple_credential = credential.simple_credential();
                        localconf[fslib::kS3CredentialSimpleAccessKeyId] = simple_credential.access_key();
                        localconf[fslib::kS3CredentialSimpleAccessKeySecret] = simple_credential.access_key_secret();
                    } else if (credential.has_profile_credential()) {
                        localconf[fslib::kS3CredentialType] = "instance_profile";
                    } else if (credential.has_assume_role_credential()) {
                        localconf[fslib::kS3CredentialType] = "assume_role";
                        auto role_credential = credential.assume_role_credential();
                        localconf[fslib::kS3CredentialAssumeRoleArn] = role_credential.iam_role_arn();
                        localconf[fslib::kS3CredentialAssumeRoleExternalId] = role_credential.external_id();
                    } else {
                        localconf[fslib::kS3CredentialType] = "default";
                    }
                }
            }
            break;
        case staros::FileStoreType::HDFS:
            scheme = "hdfs://";
            localconf[fslib::kSysRoot] = info.path_info.full_path();
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
            // use persistent uri as identifier to maximize sharing of cache data
            localconf[fslib::kCacheFsIdentifier] = tmp[fslib::kSysRoot];
            localconf[fslib::kCacheFsTtlSecs] = absl::StrFormat("%ld", cache_info.ttl_seconds());
            if (cache_info.async_write_back()) {
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

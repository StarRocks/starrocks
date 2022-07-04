// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#ifdef USE_STAROS

#include <starlet.h>

#include "common/statusor.h"

class StarOSWorker;
namespace starrocks {

class StarOSWorker : public staros::starlet::Worker {
public:
    using ServiceId = staros::starlet::ServiceId;
    using WorkerId = staros::starlet::WorkerId;
    using ShardId = staros::starlet::ShardId;
    using ShardInfo = staros::starlet::ShardInfo;
    using WorkerInfo = staros::starlet::WorkerInfo;

    StarOSWorker() : _service_id(0), _worker_id(0) {}

    ~StarOSWorker() override = default;

    absl::Status add_shard(const ShardInfo& shard) override;

    absl::Status remove_shard(const ShardId shard) override;

    absl::StatusOr<WorkerInfo> worker_info() override;

    absl::Status update_worker_info(const WorkerInfo& info) override;

    StatusOr<ShardInfo> get_shard_info(ShardId id);

    std::vector<ShardInfo> shards();

private:
    std::mutex _mtx;
    ServiceId _service_id;
    WorkerId _worker_id;
    std::unordered_map<ShardId, ShardInfo> _shards;
};

extern std::shared_ptr<StarOSWorker> g_worker;
extern staros::starlet::Starlet* g_starlet;
void init_staros_worker();
void shutdown_staros_worker();

} // namespace starrocks
#endif // USE_STAROS

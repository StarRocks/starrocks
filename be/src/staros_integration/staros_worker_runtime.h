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

#ifdef USE_STAROS

#include <memory>
#include <vector>

#include "common/status.h"
#include "worker.pb.h"

namespace starcache {
class StarCache;
} // namespace starcache

namespace staros::starlet {
class Starlet;
} // namespace staros::starlet

namespace starrocks {

class StarOSWorker;
class TableMetricsManager;

std::shared_ptr<StarOSWorker> get_staros_worker();
staros::starlet::Starlet* get_starlet();

void init_staros_worker(const std::shared_ptr<starcache::StarCache>& star_cache,
                        TableMetricsManager* table_metrics_mgr = nullptr);
void shutdown_staros_worker();
void set_starlet_in_shutdown();
Status batch_update_tablet_replica_info(const std::vector<uint64_t>& tablet_ids);
Status staros_need_warmup_tablet(int64_t tablet_id);
staros::WarmupLevel staros_worker_warmup_level();

#ifdef BE_TEST
void set_staros_worker_for_test(std::shared_ptr<StarOSWorker> worker);
std::unique_ptr<staros::starlet::Starlet> swap_starlet_for_test(std::unique_ptr<staros::starlet::Starlet> starlet);
#endif

} // namespace starrocks

#endif // USE_STAROS

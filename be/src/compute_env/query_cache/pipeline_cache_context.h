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

#include <memory>
#include <tuple>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "compute_env/query_cache/lane_arbiter.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/scan/scan_morsel.h"
#include "runtime/runtime_fwd.h"

namespace starrocks::query_cache {

class ScanCacheContext {
public:
    virtual ~ScanCacheContext() = default;

    virtual AcquireResult try_acquire_lane(LaneOwnerType lane_owner) = 0;
    virtual bool probe_cache(int64_t tablet_id, int64_t version) = 0;
    virtual Status reset_lane(RuntimeState* state, LaneOwnerType lane_owner) = 0;
    virtual std::tuple<int64_t, std::vector<BaseRowsetSharedPtr>> delta_version_and_rowsets(int64_t tablet_id) = 0;
};

using ScanCacheContextPtr = std::shared_ptr<ScanCacheContext>;

class CacheMultilaneOperator {
public:
    virtual ~CacheMultilaneOperator() = default;

    virtual Status set_finished(RuntimeState* state) = 0;
    virtual void set_lane_arbiter(const LaneArbiterPtr& lane_arbiter) = 0;
    virtual Status reset_lane(RuntimeState* state, LaneOwnerType lane_owner, const std::vector<ChunkPtr>& chunks) = 0;
    virtual pipeline::OperatorPtr get_internal_op(size_t i) = 0;
};

using CacheMultilaneOperatorRawPtr = CacheMultilaneOperator*;
using CacheMultilaneOperators = std::vector<CacheMultilaneOperatorRawPtr>;

class DriverCacheContext : public ScanCacheContext {
public:
    ~DriverCacheContext() override = default;

    virtual LaneArbiterPtr lane_arbiter() = 0;
    virtual void set_multilane_operators(CacheMultilaneOperators&& multilane_operators) = 0;
    virtual void set_scan_operator(pipeline::OperatorRawPtr scan_operator) = 0;
};

using DriverCacheContextPtr = std::shared_ptr<DriverCacheContext>;

} // namespace starrocks::query_cache

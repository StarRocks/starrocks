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

#include <cstdint>
#include <memory>

#include "compute_env/query_cache/pipeline_cache_context.h"
#include "compute_env/workgroup/work_group_fwd.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/primitives/driver_state.h"

namespace starrocks::pipeline {

class SplitMorselTicketChecker;
using SplitMorselTicketCheckerPtr = std::shared_ptr<SplitMorselTicketChecker>;

class DriverScanOperator {
public:
    virtual ~DriverScanOperator() = default;

    virtual workgroup::ScanSchedEntityType sched_entity_type() const = 0;
    virtual void set_scan_executor(workgroup::ScanExecutor* scan_executor) = 0;
    virtual void set_workgroup(workgroup::WorkGroupPtr wg) = 0;
    virtual void set_query_ctx(const QueryContextPtr& query_ctx) = 0;
    virtual void set_cache_context(const query_cache::ScanCacheContextPtr& cache_context) = 0;
    virtual void set_ticket_checker(const SplitMorselTicketCheckerPtr& ticket_checker) = 0;

    virtual int64_t get_last_scan_rows_num() = 0;
    virtual int64_t get_last_scan_bytes() = 0;
    virtual int64_t get_scan_table_id() const = 0;

    virtual void begin_driver_process() = 0;
    virtual void end_driver_process(DriverState driver_state) = 0;
    virtual void end_pull_chunk(int64_t time_ns) = 0;
};

} // namespace starrocks::pipeline
